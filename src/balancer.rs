use super::{Connection, DstAddr};
use futures::{Future, Sink, Poll, Async, StartSend, AsyncSink};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

pub fn new(addrs: Vec<DstAddr>) -> Balancer {
    Balancer { addrs: Rc::new(RefCell::new(addrs)) }
}

/// Balancers accept a stream of service discovery updates and
///
///
#[derive(Clone)]
pub struct Balancer {
    addrs: Rc<RefCell<Vec<DstAddr>>>,
}

impl Balancer {
    pub fn connect(&mut self) -> Connect {
        unimplemented!()
    }
}

impl Sink for Balancer {
    type SinkItem = Vec<DstAddr>;
    type SinkError = io::Error;

    /// Update the load balancer from service discovery.
    fn start_send(&mut self, new_addrs: Vec<DstAddr>) -> StartSend<Vec<DstAddr>, Self::SinkError> {
        // TODO this is where we will update the load balancer's state to retire lapsed
        // endpoints.  Should new connections be initiated here as well?
        let mut addrs = self.addrs.borrow_mut();
        *addrs = new_addrs;

        Ok(AsyncSink::Ready)
    }

    /// Never completes.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::NotReady)
    }
}

pub struct Connect();
impl Future for Connect {
    type Item = Connection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}
