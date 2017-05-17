use super::Path;
use super::connection::{Connection, Duplex, Summary};
use super::router::Router;
use super::socket::Socket;
use futures::{Future, Poll, future};
use rustls;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use tokio_core::net::TcpStream;
//use tacho::Scope;

mod config;
mod sni;
pub use self::config::ServerConfig;

/// An incoming connection.
pub type SrcConnection = Connection<ServerCtx>;

fn new(dst: Path, router: Router, buf: Rc<RefCell<Vec<u8>>>, tls: Option<Tls>) -> Server {
    let inner = InnerServer {
        dst_name: dst,
        router: Rc::new(router),
        buf: buf,
        tls: tls,
        ctx: ServerCtx::default(),
    };
    Server(inner)
}

pub struct Server(InnerServer);
struct InnerServer {
    pub dst_name: Path,
    pub router: Rc<Router>,
    pub buf: Rc<RefCell<Vec<u8>>>,
    pub tls: Option<Tls>,
    pub ctx: ServerCtx,
}
impl Server {
    pub fn serve(&self, tcp: TcpStream) -> Serving {
        let dst_name = self.0.dst_name.clone();
        let ctx = self.0.ctx.clone();
        let buf = self.0.buf.clone();
        let router = self.0.router.clone();
        let src = {
            let sock: Box<Future<Item = Socket, Error = io::Error>> = match self.0.tls.as_ref() {
                None => Box::new(future::ok(Socket::plain(tcp))),
                Some(ref tls) => Box::new(Socket::secure_server_handshake(tcp, &tls.config)),
            };
            let dst_name = dst_name.clone();
            sock.map(move |sock| Connection::new(dst_name, sock, ctx))
        };
        let dst = router.route(&dst_name).and_then(|bal| bal.connect());
        let duplex = src.join(dst)
            .and_then(move |(src, dst)| Duplex::new(src, dst, buf));
        Serving(Box::new(duplex))
    }
}

pub struct Serving(Box<Future<Item = Summary, Error = io::Error>>);
impl Future for Serving {
    type Item = Summary;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, io::Error> {
        self.0.poll()
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
