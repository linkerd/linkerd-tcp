use super::Path;
use super::connection::{Connection, Duplex, Socket, ctx, secure, socket};
use super::router::Router;
use futures::{Async, Future, Poll, Stream, future};
use rustls;
use std::{io, net};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
//use tacho::Scope;
use tokio_core::net::{TcpListener, Incoming};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

mod config;
mod sni;
pub use self::config::ServerConfig;

/// An incoming connection.
pub type SrcConnection = Connection<ctx::Null>;

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
    };
    Unbound(meta)
}

struct Meta {
    pub listen_addr: net::SocketAddr,
    pub dst_name: Path,
    pub router: Rc<Router>,
    pub buf: Rc<RefCell<Vec<u8>>>,
    pub tls: Option<Tls>,
}

pub struct Unbound(Meta);
impl Unbound {
    pub fn listen_addr(&self) -> net::SocketAddr {
        self.0.listen_addr
    }

    pub fn dst_name(&self) -> &Path {
        &self.0.dst_name
    }

    pub fn bind(self, reactor: &Handle, timer: &Timer) -> io::Result<Bound> {
        debug!("routing on {} to {}", self.0.listen_addr, self.0.dst_name);
        let listen = TcpListener::bind(&self.0.listen_addr, reactor)?;
        Ok(Bound {
               reactor: reactor.clone(),
               timer: timer.clone(),
               bound_addr: listen.local_addr().unwrap(),
               incoming: listen.incoming(),
               meta: self.0,
           })
    }
}

pub struct Bound {
    reactor: Handle,
    timer: Timer,
    incoming: Incoming,
    meta: Meta,
    bound_addr: net::SocketAddr,
}
impl Future for Bound {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        // Accept all inbound connections from the listener and spawn their work into the
        // router. This should perhaps yield control back to the reactor periodically.
        loop {
            trace!("{}: polling incoming", self.bound_addr);
            match self.incoming.poll()? {
                Async::NotReady => {
                    return Ok(Async::NotReady);
                }
                Async::Ready(None) => {
                    trace!("{}: incoming stream closed", self.bound_addr);
                    return Ok(Async::Ready(()));
                }
                Async::Ready(Some((tcp, _))) => {
                    trace!("{}: incoming stream from {}",
                           self.bound_addr,
                           tcp.peer_addr().unwrap());
                    // Finish accepting the connection from the server.
                    //
                    // TODO we should be able to get metadata from a TLS handshake but we can't!
                    let src = {
                        let sock: Box<Future<Item = Socket,
                                             Error = io::Error>> = match self.meta.tls.as_ref() {
                            None => Box::new(future::ok(socket::plain(tcp))),
                            Some(tls) => {
                                let sock = secure::server_handshake(tcp, &tls.config)
                                    .map(socket::secure_server);
                                Box::new(sock)
                            }
                        };
                        let dst_name = self.meta.dst_name.clone();
                        sock.map(move |sock| {
                                     let ctx = ctx::null(sock.local_addr(), sock.peer_addr());
                                     Connection::new(dst_name, sock, ctx)
                                 })
                    };

                    // Obtain a selector.
                    let dst = self.meta
                        .router
                        .route(&self.meta.dst_name, &self.reactor, &self.timer);

                    // Once the incoming connection is ready and we have a balancer ready, obtain an
                    // outbound connection and begin streaming. We obtain an outbound connection after
                    // the incoming handshake is complete so that we don't waste outbound connections
                    // on failed inbound connections.
                    let duplex = {
                        let buf = self.meta.buf.clone();
                        src.join(dst)
                            .and_then(move |(src, dst)| {
                                          dst.select().and_then(move |dst| {
                                                                    Duplex::new(src, dst, buf)
                                                                })
                                      })
                    };

                    // Do all of this work in a single, separate task so that we may process
                    // additional connections while this connection is open.
                    //
                    // TODO: implement some sort of backpressure here?
                    self.reactor.spawn(duplex.map(|_| {}).map_err(|_| {}));
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Tls {
    config: Arc<rustls::ServerConfig>,
}
