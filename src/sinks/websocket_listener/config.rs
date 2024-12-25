use std::net::SocketAddr;

use snafu::ResultExt;
use vector_lib::codecs::JsonSerializerConfig;
use vector_lib::configurable::configurable_component;

use crate::{
    codecs::EncodingConfig,
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    http::Auth,
    sinks::{
        websocket::sink::{ConnectSnafu, WebSocketConnector, WebSocketError, WebSocketSink},
        Healthcheck, VectorSink,
    },
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

/// Configuration for the `websocket_listener` sink.
#[configurable_component(sink(
    "websocket_listener",
    "Deliver observability event data to websocket clients."
))]
#[derive(Clone, Debug)]
pub struct WebSocketSinkConfig {
    /// The socket address to listen for connections on.
    ///
    /// It _must_ include a port.
    #[configurable(metadata(docs::examples = "0.0.0.0:80"))]
    #[configurable(metadata(docs::examples = "localhost:80"))]
    pub address: SocketAddr,

    #[configurable(derived)]
    pub tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    pub encoding: EncodingConfig,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    pub auth: Option<Auth>,
}

impl GenerateConfig for WebSocketSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "0.0.0.0:8080".into(),
            tls: None,
            encoding: JsonSerializerConfig::default().into(),
            acknowledgements: Default::default(),
            auth: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "websocket")]
impl SinkConfig for WebSocketSinkConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let connector = self.build_connector()?;
        let ws_sink = WebSocketSink::new(self, connector.clone())?;

        Ok((
            VectorSink::from_event_streamsink(ws_sink),
            Box::pin(async move { connector.healthcheck().await }),
        ))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().input_type())
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl WebSocketSinkConfig {
    fn build_connector(&self) -> Result<WebSocketConnector, WebSocketError> {
        let tls = MaybeTlsSettings::from_config(&self.tls, false).context(ConnectSnafu)?;
        WebSocketConnector::new(self.uri.clone(), tls, self.auth.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<WebSocketSinkConfig>();
    }
}
