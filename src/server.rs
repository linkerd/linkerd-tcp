use super::balancer::Connect;
use super::connection::Connection;
use super::duplex::{Duplex, DuplexSummary};
use super::router::{Router, Route};
use futures::{Future, Poll, Async};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
//use tacho::Scope;

/// An incoming connection.
pub type SrcConnection = Connection<ServerCtx>;

pub fn new(router: Router, buf: Rc<RefCell<Vec<u8>>>) -> Server {
    Server {
        router: router,
        buf: buf,
        // srv_metrics: metrics.scope("srv", listen_addr.into()),
        // metrics: metrics,
    }
}

pub struct Server {
    router: Router,
    buf: Rc<RefCell<Vec<u8>>>,
    // srv_metrics: tacho::Scope,
    // metrics: tacho::Scope,
}
impl Server {
    pub fn serve(&mut self, src: Connection<ServerCtx>) -> Serving {
        let route = self.router.route(src.context.dst_name());
        Serving(Some(State::Routing(src, self.buf.clone(), route)))
    }
}

pub struct Serving(Option<State>);
enum State {
    Routing(Connection<ServerCtx>, Rc<RefCell<Vec<u8>>>, Route),
    Connecting(Connection<ServerCtx>, Rc<RefCell<Vec<u8>>>, Connect),
    Streaming(Duplex),
}
impl Future for Serving {
    type Item = DuplexSummary;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, io::Error> {
        loop {
            match self.0.take() {
                None => panic!("future polled after completion"),
                Some(State::Routing(src, buf, mut route)) => {
                    match route.poll()? {
                        Async::NotReady => {
                            self.0 = Some(State::Routing(src, buf, route));
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(mut balancer) => {
                            let connect = balancer.connect();
                            self.0 = Some(State::Connecting(src, buf, connect));
                        }
                    }
                }
                Some(State::Connecting(src, buf, mut connect)) => {
                    match connect.poll()? {
                        Async::NotReady => {
                            self.0 = Some(State::Connecting(src, buf, connect));
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(dst) => {
                            self.0 = Some(State::Streaming(Duplex::new(src, dst, buf)));
                        }
                    }
                }
                Some(State::Streaming(mut duplex)) => {
                    match duplex.poll()? {
                        Async::NotReady => {
                            self.0 = Some(State::Streaming(duplex));
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(summary) => {
                            return Ok(summary.into());
                        }
                    }
                }
            }
        }
    }
}

/// The state of a
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
