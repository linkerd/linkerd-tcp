use super::Path;
use super::connection::{Connection, Duplex, Summary};
use super::router::Router;
use super::socket::Socket;
use futures::{Sink, AsyncSink, Async, Future, Poll, StartSend, Stream, future};
use rustls;
use std::{io, net};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tokio_core::net::{TcpStream, TcpListener};
use tokio_core::reactor::Handle;
//use tacho::Scope;

mod config;
mod sni;
pub use self::config::ServerConfig;

/// An incoming connection.
pub type SrcConnection = Connection<ServerCtx>;

fn unbound(addr: net::SocketAddr,
           dst: Path,
           router: Router,
           buf: Rc<RefCell<Vec<u8>>>,
           tls: Option<Tls>)
           -> Unbound {
    let meta = Meta {
        listen_addr: addr,
        dst_name: dst,
        router: Rc::new(router),
        buf: buf,
        tls: tls,
        ctx: ServerCtx::default(),
    };
    Unbound(meta)
}

pub struct Unbound(Meta);
impl Unbound {
    pub fn bind(self, reactor: &Handle) -> io::Result<Bound> {
        let listen = TcpListener::bind(&self.0.listen_addr, reactor)?;
        Ok(Bound {
               reactor: reactor.clone(),
               listener: listen,
               meta: self.0,
           })
    }
}

pub struct Bound {
    reactor: Handle,
    listener: TcpListener,
    meta: Meta,
}
impl Bound {
    fn into_future(mut self) -> Box<Future<Item = (), Error = ()>> {
        let listener = self.listener;
        let serving = Serving {
            reactor: self.reactor,
            meta: self.meta,
        };
        let fut = listener.incoming().forward(serving);
        Box::new(fut.map(|_| {}).map_err(|_| {}))
    }
}

struct Meta {
    pub listen_addr: net::SocketAddr,
    pub dst_name: Path,
    pub router: Rc<Router>,
    pub buf: Rc<RefCell<Vec<u8>>>,
    pub tls: Option<Tls>,
    pub ctx: ServerCtx,
}

struct Serving {
    reactor: Handle,
    meta: Meta,
}

impl Sink for Serving {
    type SinkItem = (TcpStream, net::SocketAddr);
    type SinkError = io::Error;
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, io::Error> {
        let dst_name = self.meta.dst_name.clone();
        let ctx = self.meta.ctx.clone();
        let buf = self.meta.buf.clone();
        let router = self.meta.router.clone();

        // Finish accepting the connection from the server.
        //
        // TODO we should be able to get metadata from a TLS handshake but we can't!
        let src = {
            let (tcp, _) = item;
            let sock: Box<Future<Item = Socket, Error = io::Error>> =
                match self.meta.tls.as_ref() {
                    None => Box::new(future::ok(Socket::plain(tcp))),
                    Some(tls) => Box::new(Socket::secure_server_handshake(tcp, &tls.config)),
                };
            let dst_name = dst_name.clone();
            sock.map(move |sock| Connection::new(dst_name, sock, ctx))
        };

        // Obtain a balancer.
        let bal = router.route(&dst_name, &self.reactor);

        // Once the incoming connection is ready and we have a balancer ready, obtain an
        // outbound connection and begin streaming. We obtain an outbound connection after
        // the incoming handshake is complete so that we don't waste outbound connections
        // on failed inbound connections.
        let duplex = src.join(bal)
            .and_then(move |(src, bal)| {
                          bal.connect()
                              .and_then(move |dst| Duplex::new(src, dst, buf))
                      });

        // Do all of this work in a single, separate task so that we may process
        // additional connections while this connection is open.
        //
        // TODO: implement some sort of backpressure here?
        self.reactor.spawn(duplex.map(|_| {}).map_err(|_| {}));
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        // TODO allow servers to be closed gracefully.
        Ok(Async::NotReady)
    }
}

#[derive(Clone)]
pub struct Tls {
    config: Arc<rustls::ServerConfig>,
}

#[derive(Clone, Debug, Default)]
pub struct ServerCtx(Rc<RefCell<InnerServerCtx>>);

#[derive(Debug, Default)]
struct InnerServerCtx {
    connects: usize,
    disconnects: usize,
    failures: usize,
    bytes_to_dst: usize,
    bytes_to_src: usize,
}

impl ServerCtx {
    fn active(&self) -> usize {
        let InnerServerCtx {
            connects,
            disconnects,
            ..
        } = *self.0.borrow();

        connects - disconnects
    }
}
