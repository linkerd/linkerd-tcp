use super::{ConfigError, Path, Socket};
use super::connection::{Connection, ConnectionCtx};
use super::socket::SecureClientHandshake;
use futures::{Async, Poll, Future};
use rustls::ClientConfig as RustlsClientConfig;
use std::{io, net, time};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_core::reactor::Handle;

pub type ConnectorFactoryConnection = Connection<EndpointCtx>;

#[derive(Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum ConnectorFactoryConfig {
    #[serde(rename = "io.l5d.global")]
    Global(ConnectorConfig),

    #[serde(rename = "io.l5d.static")]
    Static { configs: Vec<ConnectorConfig> },
}

impl Default for ConnectorFactoryConfig {
    fn default() -> ConnectorFactoryConfig {
        ConnectorFactoryConfig::Global(ConnectorConfig::default())
    }
}

impl ConnectorFactoryConfig {
    pub fn mk_connector_factory(&self, handle: &Handle) -> Result<ConnectorFactory, ConfigError> {
        match *self {
            ConnectorFactoryConfig::Global(ref cfg) => {
                if cfg.prefix.is_some() {
                    return Err("`prefix` not supported in io.l5d.global".into());
                }
                let conn = cfg.mk_connector(handle)?;
                Ok(ConnectorFactory(ConnectorFactoryInner::Global(conn)))
            }
            ConnectorFactoryConfig::Static { ref configs } => {
                let mut pfx_configs = Vec::with_capacity(configs.len());
                for cfg in configs {
                    match cfg.prefix {
                        None => {
                            return Err("`prefix` required in io.l5d.static".into());
                        }
                        Some(ref pfx) => {
                            pfx_configs.push((pfx.clone().into(), cfg.clone()));
                        } 
                    }
                }
                let factory = StaticConnectorFactory {
                    reactor: handle.clone(),
                    configs: pfx_configs,
                };
                Ok(ConnectorFactory(ConnectorFactoryInner::Static(factory)))
            }
        }
    }
}

#[derive(Clone, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConnectorConfig {
    prefix: Option<String>,
    tls: Option<TlsConnectorFactoryConfig>,
    connect_timeout_ms: Option<u64>,
    idle_timeout_ms: Option<u64>,
    // TODO fail_fast: Option<Boolean>
    // TODO requeue_budget: Option<RequeueBudget>
}
impl ConnectorConfig {
    fn mk_connector(&self, handle: &Handle) -> Result<Connector, ConfigError> {
        let tls = match self.tls {
            None => None,
            Some(ref tls) => Some(tls.mk_tls()?),
        };
        Ok(Connector {
               reactor: handle.clone(),
               connect_timeout: self.connect_timeout_ms.map(time::Duration::from_millis),
               idle_timeout: self.idle_timeout_ms.map(time::Duration::from_millis),
               tls: tls,
           })
    }

    fn update(&mut self, other: &ConnectorConfig) {
        if let Some(ref otls) = other.tls {
            self.tls = Some(otls.clone());
        }
        if let Some(ct) = other.connect_timeout_ms {
            self.connect_timeout_ms = Some(ct);
        }
        if let Some(ct) = other.idle_timeout_ms {
            self.idle_timeout_ms = Some(ct);
        }
    }
}

#[derive(Clone, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsConnectorFactoryConfig {
    pub dns_name: String,
    pub trust_certs: Option<Vec<String>>,
}
impl TlsConnectorFactoryConfig {
    pub fn mk_tls(&self) -> Result<Tls, ConfigError> {
        unimplemented!()
    }
}

/// Builds a connector
pub struct ConnectorFactory(ConnectorFactoryInner);
enum ConnectorFactoryInner {
    Global(Connector),
    Static(StaticConnectorFactory),
}
impl ConnectorFactory {
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

#[derive(Clone)]
pub struct Connector {
    reactor: Handle,
    connect_timeout: Option<time::Duration>,
    idle_timeout: Option<time::Duration>,
    tls: Option<Tls>,
}
impl Connector {
    pub fn connect(&mut self, ctx: EndpointCtx) -> ConnectingSocket {
        let c = TcpStream::connect(&ctx.peer_addr(), &self.reactor);
        ConnectingSocket(Some(ConnectingSocketState::Connecting(c, self.tls.clone(), ctx)))
    }
}

pub struct ConnectingSocket(Option<ConnectingSocketState>);
enum ConnectingSocketState {
    Connecting(TcpStreamNew, Option<Tls>, EndpointCtx),
    Handshaking(Box<SecureClientHandshake>, EndpointCtx),
    Ready(Socket, EndpointCtx),
}

impl Future for ConnectingSocket {
    type Item = Connection<EndpointCtx>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state = self.0
                .take()
                .expect("connect must not be polled after completion");

            match state {
                ConnectingSocketState::Connecting(mut fut, tls, ctx) => {
                    match fut.poll()? {
                        Async::NotReady => {
                            self.0 = Some(ConnectingSocketState::Connecting(fut, tls, ctx));
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(tcp) => {
                            match tls {
                                Some(tls) => {
                                    let conn = Box::new(tls.handshake(tcp));
                                    self.0 = Some(ConnectingSocketState::Handshaking(conn, ctx));
                                }
                                None => {
                                    let sock = Socket::plain(tcp);
                                    self.0 = Some(ConnectingSocketState::Ready(sock, ctx));
                                }
                            }
                        }
                    }
                }

                ConnectingSocketState::Handshaking(mut tls, ctx) => {
                    match tls.poll()? {
                        Async::NotReady => {
                            self.0 = Some(ConnectingSocketState::Handshaking(tls, ctx));
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(sock) => {
                            self.0 = Some(ConnectingSocketState::Ready(sock, ctx));
                        }
                    }
                }

                ConnectingSocketState::Ready(sock, ctx) => {
                    let conn = Connection {
                        context: ConnectionCtx::new(sock.local_addr(),
                                                    sock.peer_addr(),
                                                    ctx.dst_name().clone(),
                                                    ctx),
                        socket: sock,
                    };
                    return Ok(Async::Ready(conn));
                }
            }
        }
    }
}

/// The state of a load balaner endpoint.
#[derive(Clone, Debug)]
pub struct EndpointCtx(Rc<RefCell<InnerEndpointCtx>>);

#[derive(Debug)]
struct InnerEndpointCtx {
    peer_addr: net::SocketAddr,
    dst_name: Path,
    connect_attempts: usize,
    connect_failures: usize,
    connect_successes: usize,
    disconnects: usize,
    bytes_to_dst: usize,
    bytes_to_src: usize,
}

impl EndpointCtx {
    pub fn new(addr: net::SocketAddr, dst: Path) -> EndpointCtx {
        let inner = InnerEndpointCtx {
            peer_addr: addr,
            dst_name: dst,
            connect_attempts: 0,
            connect_failures: 0,
            connect_successes: 0,
            disconnects: 0,
            bytes_to_dst: 0,
            bytes_to_src: 0,
        };
        EndpointCtx(Rc::new(RefCell::new(inner)))
    }

    pub fn dst_name(&self) -> Path {
        self.0.borrow().dst_name.clone()
    }

    pub fn peer_addr(&self) -> net::SocketAddr {
        let s = self.0.borrow();
        (*s).peer_addr
    }

    pub fn active(&self) -> usize {
        let InnerEndpointCtx {
            connect_successes,
            disconnects,
            ..
        } = *self.0.borrow();

        connect_successes - disconnects
    }

    pub fn connect_init(&self) {
        let mut s = self.0.borrow_mut();
        (*s).connect_attempts += 1;
    }

    pub fn connect_ok(&self) {
        let mut s = self.0.borrow_mut();
        (*s).connect_successes += 1;
    }

    pub fn connect_fail(&self) {
        let mut s = self.0.borrow_mut();
        (*s).connect_failures += 1;
    }

    pub fn disconnect(&self) {
        let mut s = self.0.borrow_mut();
        (*s).disconnects += 1;
    }

    pub fn dst_write(&self, sz: usize) {
        let mut s = self.0.borrow_mut();
        (*s).bytes_to_dst += sz;
    }

    pub fn src_write(&self, sz: usize) {
        let mut s = self.0.borrow_mut();
        (*s).bytes_to_dst += sz;
    }
}
