use super::*;
use futures::{Future, Poll, Async};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
//use tacho::Scope;

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
    pub fn serve(&mut self, src: Connection) -> Serving {
        let route = self.router.route(&src.envelope);
        Serving(Some(State::Routing(src, self.buf.clone(), route)))
    }
}

pub struct Serving(Option<State>);
enum State {
    Routing(Connection, Rc<RefCell<Vec<u8>>>, Route),
    Connecting(Connection, Rc<RefCell<Vec<u8>>>, Connect),
    Streaming(Duplex),
}
impl Future for Serving {
    type Item = Summary;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Summary, io::Error> {
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
