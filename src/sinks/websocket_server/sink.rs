use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{
    channel::mpsc::{unbounded, UnboundedSender},
    future, pin_mut,
    stream::BoxStream,
    StreamExt, TryStreamExt,
};
use http::{header::AUTHORIZATION, StatusCode};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{
    handshake::server::{ErrorResponse, Request, Response},
    Message,
};
use tokio_util::codec::Encoder as _;
use tracing::Instrument;
use uuid::Uuid;
use vector_lib::{
    event::{Event, EventStatus, MaybeAsLogMut},
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
        ConnectionOpen, OpenGauge, WsListenerConnectionEstablished,
        WsListenerConnectionFailedError, WsListenerConnectionShutdown, WsListenerSendError,
    },
    sources::util::http::HttpSourceAuth,
};

use super::{config::MessageBuffering, WebSocketListenerSinkConfig};

pub struct WebSocketListenerSink {
    tls: MaybeTlsSettings,
    transformer: Transformer,
    encoder: Encoder<()>,
    address: SocketAddr,
    auth: HttpSourceAuth,
    message_buffering: Option<MessageBuffering>,
}

impl WebSocketListenerSink {
    pub fn new(config: WebSocketListenerSinkConfig) -> crate::Result<Self> {
        let tls = MaybeTlsSettings::from_config(config.tls.as_ref(), true)?;
        let transformer = config.encoding.transformer();
        let serializer = config.encoding.build()?;
        let encoder = Encoder::<()>::new(serializer);
        let auth = HttpSourceAuth::try_from(config.auth.as_ref())?;
        Ok(Self {
            tls,
            address: config.address,
            transformer,
            encoder,
            auth,
            message_buffering: config.message_buffering,
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
        client_checkpoints: Arc<Mutex<HashMap<IpAddr, Uuid>>>,
        buffer: Arc<Mutex<VecDeque<(Uuid, Message)>>>,
        mut listener: MaybeTlsListener,
        handle_incoming_data: bool,
    ) {
        let open_gauge = OpenGauge::new();

        while let Ok(stream) = listener.accept().await {
            tokio::spawn(
                Self::handle_connection(
                    auth.clone(),
                    Arc::clone(&peers),
                    Arc::clone(&client_checkpoints),
                    Arc::clone(&buffer),
                    stream,
                    open_gauge.clone(),
                    handle_incoming_data,
                )
                .in_current_span(),
            );
        }
    }

    async fn handle_connection(
        auth: HttpSourceAuth,
        peers: Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>,
        client_checkpoints: Arc<Mutex<HashMap<IpAddr, Uuid>>>,
        buffer: Arc<Mutex<VecDeque<(Uuid, Message)>>>,
        stream: MaybeTlsIncomingStream<TcpStream>,
        open_gauge: OpenGauge,
        handle_incoming_data: bool,
    ) -> Result<(), ()> {
        let addr = stream.peer_addr();
        debug!("Incoming TCP connection from: {}", addr);

        let mut last_received = None;
        let mut should_replay = false;

        if let Some(checkpoint) = client_checkpoints.lock().unwrap().get(&addr.ip()) {
            last_received = Some(*checkpoint);
            should_replay = true;
        }
        let header_callback = |req: &Request, response: Response| {
            let query_params = req.uri().query().unwrap_or("");
            if query_params.contains("last_received") {
                should_replay = true;
                if let Ok(url) =
                    url::Url::parse(format!("http://ignored.com/?{}", query_params).as_str())
                {
                    if let Some((_, last_received_param_value)) =
                        url.query_pairs().find(|(k, _)| k == "last_received")
                    {
                        if let Ok(last_received_val) = Uuid::parse_str(&last_received_param_value) {
                            last_received = Some(last_received_val);
                        }
                    }
                }
            }
            match auth.is_valid(
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
            }
        };

        let ws_stream = tokio_tungstenite::accept_hdr_async(stream, header_callback)
            .await
            .map_err(|err| {
                debug!("Error during websocket handshake: {}", err);
                emit!(WsListenerConnectionFailedError {
                    error: Box::new(err)
                })
            })?;

        let _open_token = open_gauge.open(|count| emit!(ConnectionOpen { count }));

        // Insert the write part of this peer to the peer map.
        let (tx, rx) = unbounded();

        {
            let mut peers = peers.lock().unwrap();
            if should_replay {
                let buffered_messages = buffer.lock().unwrap();

                for (_, message) in buffered_messages
                    .iter()
                    .filter(|(id, _)| Some(*id) > last_received)
                {
                    if let Err(error) = tx.unbounded_send(message.clone()) {
                        emit!(WsListenerSendError { error });
                    }
                }
            }

            debug!("WebSocket connection established: {}", addr);

            peers.insert(addr, tx);
            emit!(WsListenerConnectionEstablished {
                client_count: peers.len()
            });
        }

        let (outgoing, incoming) = ws_stream.split();

        let incoming_data_handler = incoming.try_for_each(|msg| {
            if handle_incoming_data {
                let ip = addr.ip();
                debug!("Received a message from {}: {}", ip, msg.to_text().unwrap());

                if let Ok(checkpoint) = msg
                    .to_text()
                    .map_err(|_| ())
                    .and_then(|msg| Uuid::parse_str(msg.trim()).map_err(|_| ()))
                {
                    debug!("Inserting checkpoint for {}: {}", ip, checkpoint);
                    client_checkpoints.lock().unwrap().insert(ip, checkpoint);
                }
            }

            future::ok(())
        });
        let forward_data_to_client = rx.map(Ok).forward(outgoing);

        pin_mut!(forward_data_to_client, incoming_data_handler);
        future::select(forward_data_to_client, incoming_data_handler).await;

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
        let input = input.fuse().peekable();
        pin_mut!(input);

        let bytes_sent = register!(BytesSent::from(Protocol("websocket".into())));
        let events_sent = register!(EventsSent::from(Output(None)));
        let encode_as_binary = self.should_encode_as_binary();

        let listener = self.tls.bind(&self.address).await.map_err(|_| ())?;

        let peers = Arc::new(Mutex::new(HashMap::default()));
        let message_buffer = Arc::new(Mutex::new(VecDeque::with_capacity(
            if let Some(MessageBuffering { ref max_events, .. }) = self.message_buffering {
                max_events.get()
            } else {
                0
            },
        )));
        let client_checkpoints = Arc::new(Mutex::new(HashMap::default()));

        let handle_incoming_data = self
            .message_buffering
            .as_ref()
            .map(|b| b.track_client_acks)
            .unwrap_or(false);
        tokio::spawn(
            Self::handle_connections(
                self.auth,
                Arc::clone(&peers),
                Arc::clone(&client_checkpoints),
                Arc::clone(&message_buffer),
                listener,
                handle_incoming_data,
            )
            .in_current_span(),
        );

        while input.as_mut().peek().await.is_some() {
            let mut event = input.next().await.unwrap();
            let message_id = Uuid::now_v7();
            let finalizers = event.take_finalizers();

            self.transformer.transform(&mut event);

            if let Some(MessageBuffering {
                message_id_path: Some(ref message_id_path),
                ..
            }) = self.message_buffering
            {
                if let Some(log) = event.maybe_as_log_mut() {
                    let mut buffer = [0; 36];
                    let uuid = message_id.hyphenated().encode_lower(&mut buffer);
                    log.value_mut()
                        .insert(message_id_path, Bytes::copy_from_slice(uuid.as_bytes()));
                }
            }

            let event_byte_size = event.estimated_json_encoded_size_of();

            let mut bytes = BytesMut::new();
            match self.encoder.encode(event, &mut bytes) {
                Ok(()) => {
                    finalizers.update_status(EventStatus::Delivered);

                    let message = if encode_as_binary {
                        Message::binary(bytes)
                    } else {
                        Message::text(String::from_utf8_lossy(&bytes))
                    };
                    let message_len = message.len();

                    if let Some(ref buffering_config) = self.message_buffering {
                        let mut buffer = message_buffer.lock().unwrap();
                        if buffer.len() + 1 >= buffering_config.max_events.get() {
                            buffer.pop_front();
                        }
                        buffer.push_back((message_id, message.clone()));
                    }

                    let peers = peers.lock().unwrap();
                    let broadcast_recipients = peers.iter().map(|(_, ws_sink)| ws_sink);
                    for recp in broadcast_recipients {
                        if let Err(error) = recp.unbounded_send(message.clone()) {
                            emit!(WsListenerSendError { error });
                        } else {
                            events_sent.emit(CountByteSize(1, event_byte_size));
                            bytes_sent.emit(ByteSize(message_len));
                        }
                    }
                }
                Err(_) => {
                    // Error is handled by `Encoder`.
                    finalizers.update_status(EventStatus::Errored);
                }
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::{channel::mpsc::UnboundedReceiver, SinkExt, Stream, StreamExt};
    use futures_util::stream;
    use std::{future::ready, num::NonZeroUsize};

    use tokio::{task::JoinHandle, time};
    use vector_lib::sink::VectorSink;

    use super::*;

    use crate::{
        event::{Event, LogEvent},
        sinks::websocket_server::config::MessageBuffering,
        test_util::{
            components::{run_and_assert_sink_compliance, SINK_TAGS},
            next_addr,
        },
    };

    #[tokio::test]
    async fn test_single_client() {
        let event = Event::Log(LogEvent::from("foo"));

        let (mut sender, input_events) = build_test_event_channel();
        let address = next_addr();
        let port = address.port();

        let websocket_sink = start_websocket_server_sink(
            WebSocketListenerSinkConfig {
                address,
                ..Default::default()
            },
            input_events,
        )
        .await;

        let client_handle = attach_websocket_client(port, vec![event.clone()]).await;
        sender.send(event).await.expect("Failed to send.");

        client_handle.await.unwrap();
        drop(sender);
        websocket_sink.await.unwrap();
    }

    #[tokio::test]
    async fn test_single_client_late_connect() {
        let event1 = Event::Log(LogEvent::from("foo1"));
        let event2 = Event::Log(LogEvent::from("foo2"));

        let (mut sender, input_events) = build_test_event_channel();
        let address = next_addr();
        let port = address.port();

        let websocket_sink = start_websocket_server_sink(
            WebSocketListenerSinkConfig {
                address,
                ..Default::default()
            },
            input_events,
        )
        .await;

        // Sending event 1 before client joined, the client should not received it
        sender.send(event1).await.expect("Failed to send.");

        // Now connect the client
        let client_handle = attach_websocket_client(port, vec![event2.clone()]).await;

        // Sending event 2, this one should be received by the client
        sender.send(event2).await.expect("Failed to send.");

        client_handle.await.unwrap();
        drop(sender);
        websocket_sink.await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_clients() {
        let event = Event::Log(LogEvent::from("foo"));

        let (mut sender, input_events) = build_test_event_channel();
        let address = next_addr();
        let port = address.port();

        let websocket_sink = start_websocket_server_sink(
            WebSocketListenerSinkConfig {
                address,
                ..Default::default()
            },
            input_events,
        )
        .await;

        let client_handle_1 = attach_websocket_client(port, vec![event.clone()]).await;
        let client_handle_2 = attach_websocket_client(port, vec![event.clone()]).await;
        sender.send(event).await.expect("Failed to send.");

        client_handle_1.await.unwrap();
        client_handle_2.await.unwrap();
        drop(sender);
        websocket_sink.await.unwrap();
    }

    #[tokio::test]
    async fn sink_spec_compliance() {
        let event = Event::Log(LogEvent::from("foo"));

        let sink = WebSocketListenerSink::new(WebSocketListenerSinkConfig {
            address: next_addr(),
            ..Default::default()
        })
        .unwrap();

        run_and_assert_sink_compliance(
            VectorSink::from_event_streamsink(sink),
            stream::once(ready(event)),
            &SINK_TAGS,
        )
        .await;
    }

    #[tokio::test]
    async fn test_client_late_connect_with_buffering() {
        let event1 = Event::Log(LogEvent::from("foo1"));
        let event2 = Event::Log(LogEvent::from("foo2"));

        let (mut sender, input_events) = build_test_event_channel();
        let address = next_addr();
        let port = address.port();

        let websocket_sink = start_websocket_server_sink(
            WebSocketListenerSinkConfig {
                address,
                message_buffering: Some(MessageBuffering {
                    max_events: NonZeroUsize::new(1).unwrap(),
                    message_id_path: None,
                    track_client_acks: false,
                }),
                ..Default::default()
            },
            input_events,
        )
        .await;

        // Sending event 1 before client joined, the client without buffering should not receive it
        sender.send(event1.clone()).await.expect("Failed to send.");

        // Now connect the clients
        let client_handle = attach_websocket_client(port, vec![event2.clone()]).await;
        let client_with_buffer_handle = attach_websocket_client_with_query(
            port,
            "last_received=0",
            vec![event1.clone(), event2.clone()],
        )
        .await;

        // Sending event 2, this one should be received by both clients
        sender.send(event2).await.expect("Failed to send.");

        client_handle.await.unwrap();
        client_with_buffer_handle.await.unwrap();
        drop(sender);
        websocket_sink.await.unwrap();
    }

    #[tokio::test]
    async fn test_client_late_connect_with_buffering_over_max_events_limit() {
        let event1 = Event::Log(LogEvent::from("foo1"));
        let event2 = Event::Log(LogEvent::from("foo2"));

        let (mut sender, input_events) = build_test_event_channel();
        let address = next_addr();
        let port = address.port();

        let websocket_sink = start_websocket_server_sink(
            WebSocketListenerSinkConfig {
                address,
                message_buffering: Some(MessageBuffering {
                    max_events: NonZeroUsize::new(1).unwrap(),
                    message_id_path: None,
                    track_client_acks: false,
                }),
                ..Default::default()
            },
            input_events,
        )
        .await;

        let client_handle =
            attach_websocket_client(port, vec![event1.clone(), event2.clone()]).await;

        // Sending 2 events before client joined, the client without buffering should receive just one
        sender.send(event1.clone()).await.expect("Failed to send.");
        sender.send(event2.clone()).await.expect("Failed to send.");

        let client_with_buffer_handle =
            attach_websocket_client_with_query(port, "last_received=0", vec![event2.clone()]).await;

        client_handle.await.unwrap();
        client_with_buffer_handle.await.unwrap();
        drop(sender);
        websocket_sink.await.unwrap();
    }

    async fn start_websocket_server_sink<S>(
        config: WebSocketListenerSinkConfig,
        events: S,
    ) -> JoinHandle<()>
    where
        S: Stream<Item = Event> + Send + 'static,
    {
        let sink = WebSocketListenerSink::new(config).unwrap();

        let compliance_assertion = tokio::spawn(run_and_assert_sink_compliance(
            VectorSink::from_event_streamsink(sink),
            events,
            &SINK_TAGS,
        ));

        time::sleep(time::Duration::from_millis(100)).await;

        compliance_assertion
    }

    async fn attach_websocket_client_with_query(
        port: u16,
        query: &str,
        expected_events: Vec<Event>,
    ) -> JoinHandle<()> {
        attach_websocket_client_url(format!("ws://localhost:{port}/?{query}"), expected_events)
            .await
    }

    async fn attach_websocket_client(port: u16, expected_events: Vec<Event>) -> JoinHandle<()> {
        attach_websocket_client_url(format!("ws://localhost:{port}"), expected_events).await
    }

    async fn attach_websocket_client_url(
        url: String,
        expected_events: Vec<Event>,
    ) -> JoinHandle<()> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Client failed to connect.");
        let (_, rx) = ws_stream.split();
        tokio::spawn(async move {
            let events = expected_events.clone();
            rx.take(events.len())
                .zip(stream::iter(events))
                .for_each(|(msg, expected)| async {
                    let msg_text = msg.unwrap().into_text().unwrap();
                    let expected = serde_json::to_string(expected.into_log().value()).unwrap();
                    assert_eq!(expected, msg_text);
                })
                .await;
        })
    }

    fn build_test_event_channel() -> (UnboundedSender<Event>, UnboundedReceiver<Event>) {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        (tx, rx)
    }
}
