//! A simple layer-4 load balancing library on tokio.
//!
//! Inspired by https://github.com/tailhook/tk-pool.
//!
//! TODO: if removed endpoints can't be considered for load balancing, they should be
//! removed from `endpoints.
//!
//! TODO: Srcs will have to be made a trait to accomodate additional serverside
//! context: specifically, ALPN.

use futures::{Future, Stream};
use rustls;
use std::io;
use std::net::{self, SocketAddr};
use std::sync::Arc;
use tacho;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Handle;

mod balancer;
mod duplex;
mod endpoint;
mod proxy_stream;
mod shared;
mod socket;


pub use self::balancer::Balancer;
use self::duplex::Duplex;
pub use self::endpoint::Endpoint;
use self::proxy_stream::ProxyStream;
pub use self::shared::Shared;
use self::socket::Socket;

pub struct Src(Socket);
pub struct Dst(Socket);

pub trait WithAddr {
    fn addr(&self) -> SocketAddr;
}

impl WithAddr for Src {
    fn addr(&self) -> SocketAddr {
        self.0.addr()
    }
}

/// Binds on `addr` and produces `U`-typed src connections.
pub trait Acceptor {
    fn accept(&self, addr: &SocketAddr) -> Box<Stream<Item = Src, Error = io::Error>>;
}

/// Establishes a `D`-typed connection to `addr`.
// TODO does the address type need to be abstracted to support additional (TLS) metadata?
pub trait Connector {
    fn connect(&self, addr: &SocketAddr) -> Box<Future<Item = Dst, Error = io::Error>>;
}

pub struct PlainAcceptor {
    handle: Handle,
    connects: tacho::Counter,
}
impl PlainAcceptor {
    pub fn new(h: Handle, m: tacho::Scope) -> PlainAcceptor {
        PlainAcceptor {
            handle: h,
            connects: m.counter("connects".into()),
        }
    }
}
impl Acceptor for PlainAcceptor {
    fn accept(&self, addr: &SocketAddr) -> Box<Stream<Item = Src, Error = io::Error>> {
        let mut connects = self.connects.clone();
        TcpListener::bind(addr, &self.handle)
            .unwrap()
            .incoming()
            .map(move |(s, a)| {
                connects.incr(1);
                Src(Socket::plain(a, s))
            })
            .boxed()
    }
}

/// A `Connector` that builds `TcpStream`-typed connections on the provided `Handle`.
pub struct PlainConnector(Handle);
impl PlainConnector {
    pub fn new(h: Handle) -> PlainConnector {
        PlainConnector(h)
    }
}
impl Connector for PlainConnector {
    fn connect(&self, addr: &net::SocketAddr) -> Box<Future<Item = Dst, Error = io::Error>> {
        let addr = *addr;
        let f = TcpStream::connect(&addr, &self.0).map(move |s| Dst(Socket::plain(addr, s)));
        Box::new(f)
    }
}

pub struct SecureAcceptor {
    handle: Handle,
    config: Arc<rustls::ServerConfig>,
    connects: tacho::Counter,
    fails: tacho::Counter,
}
impl SecureAcceptor {
    pub fn new(h: Handle, c: rustls::ServerConfig, m: tacho::Scope) -> SecureAcceptor {
        SecureAcceptor {
            handle: h,
            config: Arc::new(c),
            connects: m.counter("connects".into()),
            fails: m.counter("handshake_failures".into()),
        }
    }
}
impl Acceptor for SecureAcceptor {
    fn accept(&self, addr: &SocketAddr) -> Box<Stream<Item = Src, Error = io::Error>> {
        let tls = self.config.clone();
        let l = TcpListener::bind(addr, &self.handle).unwrap();

        let mut connects = self.connects.clone();
        let mut fails = self.fails.clone();

        // Lift handshake errors so those connections are ignored.
        let sockets = l.incoming()
            .and_then(move |(tcp, addr)| Socket::secure_server_handshake(addr, tcp, &tls));
        let srcs = sockets.then(Ok).filter_map(move |result| match result {
            Err(_) => {
                fails.incr(1);
                None
            }
            Ok(s) => {
                connects.incr(1);
                Some(Src(s))
            }
        });
        Box::new(srcs)
    }
}

pub struct SecureConnector {
    name: String,
    handle: Handle,
    tls: Arc<rustls::ClientConfig>,
}
impl SecureConnector {
    pub fn new(n: String, c: rustls::ClientConfig, h: Handle) -> SecureConnector {
        SecureConnector {
            name: n,
            handle: h,
            tls: Arc::new(c),
        }
    }
}
impl Connector for SecureConnector {
    fn connect(&self, addr: &net::SocketAddr) -> Box<Future<Item = Dst, Error = io::Error>> {
        let tls = self.tls.clone();
        let name = self.name.clone();
        let addr = *addr;
        let f = TcpStream::connect(&addr, &self.handle)
            .and_then(move |tcp| Socket::secure_client_handshake(addr, tcp, &tls, &name))
            .map(Dst);
        Box::new(f)
    }
}
