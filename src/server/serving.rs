use super::SrcConnection;
use super::super::connection::{Duplex, Summary};
use super::super::lb::Connect;
use super::super::router::{Router, Route};
use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

pub fn new(src: SrcConnection, buf: Rc<RefCell<Vec<u8>>>, route: Route) -> Serving {
    Serving(Some(State::Routing(src, buf, route)))
}

pub struct Serving(Option<State>);

enum State {
    Routing(SrcConnection, Rc<RefCell<Vec<u8>>>, Route),
    Connecting(SrcConnection, Rc<RefCell<Vec<u8>>>, Connect),
    Streaming(Duplex),
}

impl Future for Serving {
    type Item = Summary;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, io::Error> {
        loop {
            match self.0.take().expect("future polled after completion") {
                State::Routing(src, buf, mut route) => {
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
                State::Connecting(src, buf, mut connect) => {
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
                State::Streaming(mut duplex) => {
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
