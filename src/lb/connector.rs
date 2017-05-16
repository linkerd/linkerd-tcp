use super::config::ConnectorConfig;
use super::endpoint::EndpointCtx;
use super::super::{ConfigError, Path};
use super::super::connection::{Connection, ConnectionCtx};
use super::super::socket::{Socket, SecureClientHandshake};
use futures::{Async, Poll, Future};
use rustls::ClientConfig as RustlsClientConfig;
use std::{io, time};
use std::sync::Arc;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_core::reactor::Handle;

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

pub fn new(reactor: &Handle,
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
