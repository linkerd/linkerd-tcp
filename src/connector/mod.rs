use super::{ConfigError, Path};
use super::connection::{Connection, ConnectionCtx};
use super::lb::EndpointCtx;
use super::socket::{Socket, SecureClientHandshake};
use futures::{Async, Poll, Future};
use rustls::ClientConfig as RustlsClientConfig;
use std::{io, time};
use std::sync::Arc;
use tokio_core::net::{TcpStream, TcpStreamNew};
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

    pub fn new_static(h: &Handle, configs: Vec<(Path, ConnectorConfig)>) -> ConnectorFactory {
        let f = StaticConnectorFactory {
            reactor: h.clone(),
            configs: configs,
        };
        ConnectorFactory(ConnectorFactoryInner::Static(f))
    }

    pub fn mk_connector(&self, dst_name: &Path) -> Result<Connector, ConfigError> {
        match self.0 {
            ConnectorFactoryInner::Global(ref conn) => Ok(conn.clone()),
            ConnectorFactoryInner::Static(ref factory) => factory.mk_connector(dst_name),
        }
    }
}

struct StaticConnectorFactory {
    reactor: Handle,
    configs: Vec<(Path, ConnectorConfig)>,
}
impl StaticConnectorFactory {
    fn mk_connector(&self, dst_name: &Path) -> Result<Connector, ConfigError> {
        let mut config = ConnectorConfig::default();
        for &(ref pfx, ref c) in &self.configs {
            if pfx.starts_with(dst_name) {
                config.update(c);
            }
        }
        config.mk_connector(&self.reactor)
    }
}

#[derive(Clone)]
pub struct Tls {
    name: String,
    config: Arc<RustlsClientConfig>,
}
impl Tls {
    fn handshake(&self, tcp: TcpStream) -> SecureClientHandshake {
        Socket::secure_client_handshake(tcp, &self.config, &self.name)
    }
}

fn new(reactor: &Handle,
       connect_timeout: Option<time::Duration>,
       idle_timeout: Option<time::Duration>,
       tls: Option<Tls>)
       -> Connector {
    Connector {
        reactor: reactor.clone(),
        connect_timeout: connect_timeout,
        idle_timeout: idle_timeout,
        tls: tls,
    }
}

#[derive(Clone)]
pub struct Connector {
    reactor: Handle,
    connect_timeout: Option<time::Duration>,
    idle_timeout: Option<time::Duration>,
    tls: Option<Tls>,
}
impl Connector {
    pub fn connect(&mut self, ctx: EndpointCtx) -> Connecting {
        let c = TcpStream::connect(&ctx.peer_addr(), &self.reactor);
        connecting::new(c, self.tls.clone(), ctx)
    }
}
