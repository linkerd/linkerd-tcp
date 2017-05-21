use super::{ConfigError, Path};
use super::connection::secure;
use rustls::ClientConfig as RustlsClientConfig;
use std::{net, time};
use std::sync::Arc;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;

mod config;
mod connecting;

pub use self::config::{ConnectorFactoryConfig, ConnectorConfig, TlsConnectorFactoryConfig};
pub use self::connecting::Connecting;

/// Builds a connector
pub struct ConnectorFactory(ConnectorFactoryInner);

enum ConnectorFactoryInner {
    Global(Connector),
    Static(StaticConnectorFactory),
}

impl ConnectorFactory {
    pub fn new_global(conn: Connector) -> ConnectorFactory {
        ConnectorFactory(ConnectorFactoryInner::Global(conn))
    }

    pub fn new_static(configs: Vec<(Path, ConnectorConfig)>) -> ConnectorFactory {
        let f = StaticConnectorFactory(configs);
        ConnectorFactory(ConnectorFactoryInner::Static(f))
    }

    pub fn mk_connector(&self, dst_name: &Path) -> Result<Connector, ConfigError> {
        match self.0 {
            ConnectorFactoryInner::Global(ref conn) => Ok(conn.clone()),
            ConnectorFactoryInner::Static(ref factory) => factory.mk_connector(dst_name),
        }
    }
}

struct StaticConnectorFactory(Vec<(Path, ConnectorConfig)>);
impl StaticConnectorFactory {
    fn mk_connector(&self, dst_name: &Path) -> Result<Connector, ConfigError> {
        let mut config = ConnectorConfig::default();
        for &(ref pfx, ref c) in &self.0 {
            if pfx.starts_with(dst_name) {
                config.update(c);
            }
        }
        config.mk_connector()
    }
}

#[derive(Clone)]
pub struct Tls {
    name: String,
    config: Arc<RustlsClientConfig>,
}

impl Tls {
    fn handshake(&self, tcp: TcpStream) -> secure::ClientHandshake {
        secure::client_handshake(tcp, &self.config, &self.name)
    }
}

fn new(connect_timeout: Option<time::Duration>,
       idle_timeout: Option<time::Duration>,
       tls: Option<Tls>)
       -> Connector {
    Connector {
        connect_timeout: connect_timeout,
        idle_timeout: idle_timeout,
        tls: tls,
    }
}

#[derive(Clone)]
pub struct Connector {
    connect_timeout: Option<time::Duration>,
    idle_timeout: Option<time::Duration>,
    tls: Option<Tls>,
}
impl Connector {
    pub fn connect(&self, addr: &net::SocketAddr, reactor: &Handle) -> Connecting {
        let c = TcpStream::connect(addr, reactor);
        connecting::new(c, self.tls.clone())
    }
}
