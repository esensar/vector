use std::{
    collections::HashMap,
    pin::Pin,
    task::{ready, Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use futures::{Stream, StreamExt};
use metrics::gauge;
use pin_project::pin_project;
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use vector_lib::{id::ComponentKey, shutdown::ShutdownSignal};

use crate::stats;

#[pin_project]
pub(crate) struct Utilization<S> {
    timer_tx: UnboundedSender<UtilizationTimerMessage>,
    component_key: ComponentKey,
    inner: S,
}

impl<S> Utilization<S> {
    /// Consumes this wrapper and returns the inner stream.
    ///
    /// This can't be constant because destructors can't be run in a const context, and we're
    /// discarding `IntervalStream`/`Timer` when we call this.
    #[allow(clippy::missing_const_for_fn)]
    pub(crate) fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> Stream for Utilization<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The goal of this function is to measure the time between when the
        // caller requests the next Event from the stream and before one is
        // ready, with the side-effect of reporting every so often about how
        // long the wait gap is.
        //
        // This will just measure the time, while UtilizationEmitter collects
        // all the timers and emits utilization value periodically
        let this = self.project();
        if this
            .timer_tx
            .send(UtilizationTimerMessage::StartWait(
                this.component_key,
                Instant::now(),
            ))
            .is_err()
        {
            // Say something?
        }
        let result = ready!(this.inner.poll_next_unpin(cx));
        if this
            .timer_tx
            .send(UtilizationTimerMessage::StopWait(
                this.component_key,
                Instant::now(),
            ))
            .is_err()
        {
            // Say something?
        }
        return Poll::Ready(result);
    }
}

pub(super) struct Timer {
    overall_start: Instant,
    span_start: Instant,
    waiting: bool,
    total_wait: Duration,
    ewma: stats::Ewma,
}

/// A simple, specialized timer for tracking spans of waiting vs not-waiting
/// time and reporting a smoothed estimate of utilization.
///
/// This implementation uses the idea of spans and reporting periods. Spans are
/// a period of time spent entirely in one state, aligning with state
/// transitions but potentially more granular.  Reporting periods are expected
/// to be of uniform length and used to aggregate span data into time-weighted
/// averages.
impl Timer {
    pub(crate) fn new() -> Self {
        Self {
            overall_start: Instant::now(),
            span_start: Instant::now(),
            waiting: false,
            total_wait: Duration::new(0, 0),
            ewma: stats::Ewma::new(0.9),
        }
    }

    /// Begin a new span representing time spent waiting
    pub(crate) fn start_wait_at(&mut self, at: Instant) {
        if !self.waiting {
            self.end_span(at);
            self.waiting = true;
        }
    }

    /// Begin a new span representing time spent waiting
    pub(crate) fn start_wait(&mut self) {
        self.start_wait_at(Instant::now())
    }

    /// Complete the current waiting span and begin a non-waiting span
    pub(crate) fn stop_wait_at(&mut self, at: Instant) -> Instant {
        if self.waiting {
            let now = self.end_span(at);
            self.waiting = false;
            now
        } else {
            at
        }
    }

    /// Complete the current waiting span and begin a non-waiting span
    pub(crate) fn stop_wait(&mut self) -> Instant {
        self.stop_wait_at(Instant::now())
    }

    /// Meant to be called on a regular interval, this method calculates wait
    /// ratio since the last time it was called and reports the resulting
    /// utilization average.
    pub(crate) fn report(&mut self) {
        // End the current span so it can be accounted for, but do not change
        // whether or not we're in the waiting state. This way the next span
        // inherits the correct status.
        let now = self.end_span(Instant::now());

        let total_duration = now.duration_since(self.overall_start);
        let wait_ratio = self.total_wait.as_secs_f64() / total_duration.as_secs_f64();
        let utilization = 1.0 - wait_ratio;

        self.ewma.update(utilization);
        let avg = self.ewma.average().unwrap_or(f64::NAN);
        debug!(utilization = %avg);
        gauge!("utilization").set(avg);

        // Reset overall statistics for the next reporting period.
        self.overall_start = self.span_start;
        self.total_wait = Duration::new(0, 0);
    }

    fn end_span(&mut self, at: Instant) -> Instant {
        if self.waiting {
            self.total_wait += at - self.span_start;
        }
        self.span_start = at;
        self.span_start
    }
}

enum UtilizationTimerMessage {
    StartWait(ComponentKey, Instant),
    StopWait(ComponentKey, Instant),
}

struct UtilizationComponent {
    timer: Timer,
    receiver: UnboundedReceiver<UtilizationTimerMessage>,
}

pub(crate) struct UtilizationEmitter {
    timers: HashMap<ComponentKey, Timer>,
    timer_rx: UnboundedReceiver<UtilizationTimerMessage>,
    timer_tx: UnboundedSender<UtilizationTimerMessage>,
    intervals: IntervalStream,
}

impl UtilizationEmitter {
    pub(crate) fn new() -> Self {
        let (timer_tx, timer_rx) = unbounded_channel();
        Self {
            timers: HashMap::default(),
            intervals: IntervalStream::new(interval(Duration::from_secs(5))),
            timer_tx,
            timer_rx,
        }
    }

    /// Wrap a stream to emit stats about utilization. This is designed for use with
    /// the input channels of transform and sinks components, and measures the
    /// amount of time that the stream is waiting for input from upstream. We make
    /// the simplifying assumption that this wait time is when the component is idle
    /// and the rest of the time it is doing useful work. This is more true for
    /// sinks than transforms, which can be blocked by downstream components, but
    /// with knowledge of the config the data is still useful.
    pub(crate) fn wrap<S>(&mut self, key: ComponentKey, inner: S) -> Utilization<S> {
        self.timers.insert(key, Timer::new());
        Utilization {
            timer_tx: self.timer_tx.clone(),
            component_key: key,
            inner,
        }
    }

    pub(crate) async fn run_utilization(&mut self, mut shutdown: ShutdownSignal) {
        loop {
            tokio::select! {
                message = self.timer_rx.recv() => {
                    match message {
                        Some(UtilizationTimerMessage::StartWait(key, start_time)) => {
                            self.timers[&key].start_wait_at(start_time);
                        }
                        Some(UtilizationTimerMessage::StopWait(key, stop_time)) => {
                            self.timers[&key].stop_wait_at(stop_time);
                        }
                        None => break,
                    }
                },

                Some(_) = self.intervals.next() => {
                    for (key, timer) in self.timers {
                        timer.report();
                    }
                },

                _ = &mut shutdown => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

    #[tokio::test]
    async fn name() {
        let (tx, rx) = mpsc::channel::<i32>(1000);
        let mut rx: Utilization<ReceiverStream<i32>> = wrap(rx.into());

        println!("TEST LOG");

        tokio::spawn(async move {
            println!("Start tx spawn");
            tokio::time::sleep(Duration::from_secs(20)).await;
            println!("Start tx loop");
            for i in 0..10 {
                println!("Tx wait: {}", i);
                tx.send(i).await.unwrap();
            }
            println!("End tx loop");
            tokio::time::sleep(Duration::from_secs(20)).await;
            println!("Last tx send");
            tx.send(1).await.unwrap();
            println!("Last tx send done");
        });

        tokio::spawn(async move {
            println!("Start rx spawn");
            rx.next().await;
            println!("Start rx loop");
            for i in 0..200 {
                println!("Rx wait: {}", i);
                rx.next().await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            println!("End rx loop");
        })
        .await
        .unwrap();
    }
}
