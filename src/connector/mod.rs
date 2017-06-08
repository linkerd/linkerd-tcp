use super::{ConfigError, Path};
use super::connection::secure;
use super::connection::socket::{self, Socket};
use futures::{Future, Poll};
use rustls::ClientConfig as RustlsClientConfig;
use std::{io, net, time};
use std::sync::Arc;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

mod config;

pub use self::config::{ConnectorFactoryConfig, ConnectorConfig, TlsConnectorFactoryConfig};

/// Builds a connector for each name.
pub struct ConnectorFactory(ConnectorFactoryInner);

enum ConnectorFactoryInner {
    /// Uses a single connector for all names.
    StaticGlobal(Connector),
    /// Builds a new connector for each name by applying all configurations with a
    /// matching prefix. This is considered "static" because the set of configurations may
    /// not be updated dynamically.
    StaticPrefixed(StaticPrefixConnectorFactory),
}

impl ConnectorFactory {
    pub fn new_global(conn: Connector) -> ConnectorFactory {
        ConnectorFactory(ConnectorFactoryInner::StaticGlobal(conn))
    }

    pub fn new_prefixed(prefixed_configs: Vec<(Path, ConnectorConfig)>) -> ConnectorFactory {
        let f = StaticPrefixConnectorFactory(prefixed_configs);
        ConnectorFactory(ConnectorFactoryInner::StaticPrefixed(f))
    }

    pub fn mk_connector(&self, dst_name: &Path) -> Result<Connector, ConfigError> {
        match self.0 {
            ConnectorFactoryInner::StaticGlobal(ref c) => Ok(c.clone()),
            ConnectorFactoryInner::StaticPrefixed(ref f) => f.mk_connector(dst_name),
        }
    }
}

struct StaticPrefixConnectorFactory(Vec<(Path, ConnectorConfig)>);
impl StaticPrefixConnectorFactory {
    /// Builds a new connector by applying all configurations with a matching prefix.
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

fn new(connect_timeout: Option<time::Duration>, tls: Option<Tls>) -> Connector {
    Connector {
        connect_timeout,
        tls,
    }
}

#[derive(Clone)]
pub struct Connector {
    connect_timeout: Option<time::Duration>,
    tls: Option<Tls>,
}

impl Connector {
    fn timeout<F>(&self, fut: F, timer: &Timer) -> Box<Future<Item = F::Item, Error = io::Error>>
        where F: Future<Error = io::Error> + 'static
    {
        match self.connect_timeout {
            None => Box::new(fut),
            Some(t) => Box::new(timer.timeout(fut, t).map_err(|e| e.into())),
        }
    }

    pub fn connect(&self, addr: &net::SocketAddr, reactor: &Handle, timer: &Timer) -> Connecting {
        let tcp = TcpStream::connect(addr, reactor);
        let socket: Box<Future<Item = Socket, Error = io::Error>> = match self.tls {
            None => {
                let f = tcp.map(socket::plain);
                Box::new(self.timeout(f, timer))
            }
            Some(ref tls) => {
                let tls = tls.clone();
                let f = tcp.and_then(move |tcp| tls.handshake(tcp))
                    .map(socket::secure_client);
                Box::new(self.timeout(f, timer))
            }
        };
        Connecting(socket)
    }
}

pub struct Connecting(Box<Future<Item = Socket, Error = io::Error>>);
impl Future for Connecting {
    type Item = Socket;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}
