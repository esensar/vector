use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::BytesMut;
use futures::{
    channel::mpsc::{unbounded, UnboundedSender},
    pin_mut,
    stream::BoxStream,
    StreamExt, TryStreamExt,
};
use futures_util::future;
use http::{header::AUTHORIZATION, StatusCode};
use stream_cancel::Tripwire;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{
    handshake::server::{ErrorResponse, Request, Response},
    Message,
};
use tokio_util::codec::Encoder as _;
use tracing::Instrument;
use vector_lib::{
    event::{Event, EventStatus},
    finalization::Finalizable,
    internal_event::{
        ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle, Output, Protocol,
    },
    sink::StreamSink,
    tls::{MaybeTlsIncomingStream, MaybeTlsListener, MaybeTlsSettings},
    EstimatedJsonEncodedSizeOf,
};

use crate::{
    codecs::{Encoder, Transformer},
    internal_events::{
        WsListenerConnectionEstablished, WsListenerConnectionFailedError,
        WsListenerConnectionShutdown, WsListenerSendError,
    },
    sources::util::http::HttpSourceAuth,
};

use super::WebSocketListenerSinkConfig;

pub struct WebSocketListenerSink {
    peers: Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>,
    tls: MaybeTlsSettings,
    transformer: Transformer,
    encoder: Encoder<()>,
    address: SocketAddr,
    auth: HttpSourceAuth,
}

impl WebSocketListenerSink {
    pub fn new(config: WebSocketListenerSinkConfig) -> crate::Result<Self> {
        let tls = MaybeTlsSettings::from_config(config.tls.as_ref(), true)?;
        let transformer = config.encoding.transformer();
        let serializer = config.encoding.build()?;
        let encoder = Encoder::<()>::new(serializer);
        let auth = HttpSourceAuth::try_from(config.auth.as_ref())?;
        Ok(Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            tls,
            address: config.address,
            transformer,
            encoder,
            auth,
        })
    }

    const fn should_encode_as_binary(&self) -> bool {
        use vector_lib::codecs::encoding::Serializer::{
            Avro, Cef, Csv, Gelf, Json, Logfmt, Native, NativeJson, Protobuf, RawMessage, Text,
        };

        match self.encoder.serializer() {
            RawMessage(_) | Avro(_) | Native(_) | Protobuf(_) => true,
            Cef(_) | Csv(_) | Logfmt(_) | Gelf(_) | Json(_) | Text(_) | NativeJson(_) => false,
        }
    }

    async fn handle_connections(
        auth: HttpSourceAuth,
        peers: Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>,
        mut tripwire: Tripwire,
        mut listener: MaybeTlsListener,
    ) {
        loop {
            tokio::select! {
                Ok(stream) = listener.accept() => {
                    tokio::spawn(
                        Self::handle_connection(auth.clone(), Arc::clone(&peers), tripwire.clone(), stream).in_current_span(),
                    );
                }

                _ = &mut tripwire => {
                    return
                }
            }
        }
    }

    async fn handle_connection(
        auth: HttpSourceAuth,
        peers: Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>,
        tripwire: Tripwire,
        stream: MaybeTlsIncomingStream<TcpStream>,
    ) -> Result<(), ()> {
        let addr = stream.peer_addr();
        debug!("Incoming TCP connection from: {}", addr);

        let header_callback = |req: &Request, response: Response| match auth.is_valid(
            &req.headers()
                .get(AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .map(ToString::to_string),
        ) {
            Ok(_) => Ok(response),
            Err(message) => {
                let mut response = ErrorResponse::default();
                *response.status_mut() = StatusCode::UNAUTHORIZED;
                *response.body_mut() = Some(message.message().to_string());
                debug!("Websocket handshake auth validation failed: {}", message);
                Err(response)
            }
        };

        let ws_stream = tokio_tungstenite::accept_hdr_async(stream, header_callback)
            .await
            .map(|s| s.take_until(tripwire))
            .map_err(|err| {
                debug!("Error during websocket handshake: {}", err);
                emit!(WsListenerConnectionFailedError {
                    error: Box::new(err)
                })
            })?;

        // Insert the write part of this peer to the peer map.
        let (tx, rx) = unbounded();

        {
            let mut peers = peers.lock().unwrap();
            debug!("WebSocket connection established: {}", addr);

            peers.insert(addr, tx);
            emit!(WsListenerConnectionEstablished {
                client_count: peers.len()
            });
        }

        let (outgoing, incoming) = ws_stream.split();

        let broadcast_incoming = incoming.try_for_each(|msg| {
            debug!(
                "Received a message from {}: {}",
                addr,
                msg.to_text().unwrap()
            );

            future::ok(())
        });

        let receive_from_others = rx.map(Ok).forward(outgoing);

        pin_mut!(broadcast_incoming, receive_from_others);
        future::select(broadcast_incoming, receive_from_others).await;

        {
            let mut peers = peers.lock().unwrap();
            debug!("{} disconnected", &addr);
            peers.remove(&addr);
            emit!(WsListenerConnectionShutdown {
                client_count: peers.len()
            });
        }

        Ok(())
    }
}

#[async_trait]
impl StreamSink<Event> for WebSocketListenerSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        println!("Start run");
        let input = input.fuse().peekable();
        pin_mut!(input);

        let bytes_sent = register!(BytesSent::from(Protocol("websocket".into())));
        let events_sent = register!(EventsSent::from(Output(None)));
        let encode_as_binary = self.should_encode_as_binary();

        let listener = self.tls.bind(&self.address).await.map_err(|_| ())?;
        println!("Prepared listener");

        let (trigger, tripwire) = Tripwire::new();
        let _connection_task = tokio::spawn(
            Self::handle_connections(
                self.auth,
                Arc::clone(&self.peers),
                tripwire.clone(),
                listener,
            )
            .in_current_span(),
        );
        println!("Started connection task");

        while input.as_mut().peek().await.is_some() {
            let mut event = input.next().await.unwrap();
            println!("Got an event: {:?}", event);
            let finalizers = event.take_finalizers();

            self.transformer.transform(&mut event);

            let event_byte_size = event.estimated_json_encoded_size_of();

            let mut bytes = BytesMut::new();
            match self.encoder.encode(event, &mut bytes) {
                Ok(()) => {
                    println!("Encoded the event: {:?}", bytes);
                    finalizers.update_status(EventStatus::Delivered);

                    let message = if encode_as_binary {
                        Message::binary(bytes)
                    } else {
                        Message::text(String::from_utf8_lossy(&bytes))
                    };
                    let message_len = message.len();

                    let peers = self.peers.lock().unwrap();
                    let broadcast_recipients = peers.iter().map(|(_, ws_sink)| ws_sink);
                    for recp in broadcast_recipients {
                        println!("Sending message");
                        if let Err(error) = recp.unbounded_send(message.clone()) {
                            println!("Send failed");
                            emit!(WsListenerSendError { error });
                        } else {
                            println!("Send success");
                            events_sent.emit(CountByteSize(1, event_byte_size));
                            bytes_sent.emit(ByteSize(message_len));
                        }
                    }
                    println!("Done sending, waiting for more events I guess");
                }
                Err(_) => {
                    println!("Some error");
                    // Error is handled by `Encoder`.
                    finalizers.update_status(EventStatus::Errored);
                }
            };
        }

        println!("Stopping connection");
        trigger.cancel();
        //connection_task.await.map_err(|_| ())?;
        println!("Sink done");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use tokio::time;
    use vector_lib::sink::VectorSink;

    use super::*;

    use crate::{
        event::{Event, LogEvent},
        test_util::components::{run_and_assert_sink_compliance, SINK_TAGS},
    };

    #[tokio::test]
    async fn test_single_client_2() {
        let event = Event::Log(LogEvent::from("foo"));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let input_events = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        let sink = WebSocketListenerSink::new(WebSocketListenerSinkConfig::default()).unwrap();

        //let clond = event.clone();
        let compliance_assertion = tokio::spawn(run_and_assert_sink_compliance(
            VectorSink::from_event_streamsink(sink),
            input_events,
            &SINK_TAGS,
        ));

        time::sleep(time::Duration::from_millis(100)).await;

        println!("Trying to connect");
        let (ws_stream, _) = tokio_tungstenite::connect_async("ws://localhost:8080")
            .await
            .expect("Client failed to connect.");
        println!("Connected");
        let (_, rx) = ws_stream.split();
        let cevent = event.clone();
        let handle = tokio::spawn(async move {
            let cloned_event = cevent.clone();
            rx.for_each(|msg| async {
                println!("Received a message...");
                let msg_text = msg.unwrap().into_text().unwrap();
                let expected = serde_json::to_string(cloned_event.as_log()).unwrap();
                assert_eq!(expected, msg_text);
                println!("Assertion is done!");
            });
        });
        tx.send(event).expect("Failed to send.");
        println!("Received?");

        //sender.await.expect("Wut happened");
        handle.await.expect("DONE?");
        drop(tx);
        compliance_assertion.await.expect("cmon");
        //send_thing.await.expect("cmon done");
    }

    //#[tokio::test]
    //async fn test_single_client() {
    //    let event = Event::Log(LogEvent::from("foo"));
    //
    //    let (tx, mut rx) = watch::channel(false);
    //    let sink = WebSocketListenerSink::new(WebSocketListenerSinkConfig::default()).unwrap();
    //
    //    let clond = event.clone();
    //    let compliance_assertion = tokio::spawn(run_and_assert_sink_compliance(
    //        VectorSink::from_event_streamsink(sink),
    //        stream::once(async move {
    //            let _ = rx.changed().await;
    //            println!("Producing event");
    //            clond.clone()
    //        }),
    //        &SINK_TAGS,
    //    ));
    //
    //    time::sleep(time::Duration::from_millis(100)).await;
    //
    //    println!("Trying to connect");
    //    let (ws_stream, _) = tokio_tungstenite::connect_async("ws://localhost:8080")
    //        .await
    //        .expect("Client failed to connect.");
    //    println!("Connected");
    //    let (_, rx) = ws_stream.split();
    //    let cevent = event.clone();
    //    let handle = tokio::spawn(async move {
    //        let cloned_event = cevent.clone();
    //        rx.for_each(|msg| async {
    //            let msg_text = msg.unwrap().into_text().unwrap();
    //            let expected = serde_json::to_string(cloned_event.as_log()).unwrap();
    //            assert_eq!(expected, msg_text);
    //            println!("Assertion is done!");
    //        })
    //        .await;
    //    });
    //    tx.send(true).unwrap();
    //    println!("Received?");
    //
    //    //sender.await.expect("Wut happened");
    //    handle.await.expect("DONE?");
    //    compliance_assertion.await.expect("cmon");
    //    //send_thing.await.expect("cmon done");
    //}

    //#[tokio::test]
    //async fn sink_spec_compliance() {
    //    let event = Event::Log(LogEvent::from("foo"));
    //
    //    let sink = WebSocketListenerSink::new(WebSocketListenerSinkConfig::default()).unwrap();
    //
    //    run_and_assert_sink_compliance(
    //        VectorSink::from_event_streamsink(sink),
    //        stream::once(ready(event)),
    //        &SINK_TAGS,
    //    )
    //    .await;
    //}
}
