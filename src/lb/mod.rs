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
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpListener, TcpStream};

mod balancer;
mod driver;
mod duplex;
mod endpoint;
mod proxy_stream;
mod shared;
mod socket;

use self::driver::Driver;
use self::duplex::Duplex;
use self::proxy_stream::ProxyStream;
use self::socket::Socket;

pub use self::balancer::Balancer;
pub use self::endpoint::Endpoint;
pub use self::shared::Shared;

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

pub struct PlainAcceptor(Handle);

impl PlainAcceptor {
    pub fn new(h: Handle) -> PlainAcceptor {
        PlainAcceptor(h)
    }
}

impl Acceptor for PlainAcceptor {
    fn accept(&self, addr: &SocketAddr) -> Box<Stream<Item = Src, Error = io::Error>> {
        let s = TcpListener::bind(addr, &self.0)
            .unwrap()
            .incoming()
            .map(|(s, a)| Src(Socket::plain(a, s)));
        Box::new(s)
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
}

impl SecureAcceptor {
    pub fn new(h: Handle, c: rustls::ServerConfig) -> SecureAcceptor {
        SecureAcceptor {
            handle: h,
            config: Arc::new(c),
        }
    }
}

impl Acceptor for SecureAcceptor {
    fn accept(&self, addr: &SocketAddr) -> Box<Stream<Item = Src, Error = io::Error>> {
        let tls = self.config.clone();
        let l = TcpListener::bind(addr, &self.handle).unwrap();
        let sockets = l.incoming()
            .and_then(move |(tcp, addr)| Socket::secure_server_handshake(addr, tcp, &tls));
        // Lift handshake errors so those connections are ignored.
        let srcs = sockets.then(Ok).filter_map(|result| match result {
            Err(_) => None,
            Ok(s) => Some(Src(s)),
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