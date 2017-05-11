use super::{Connection, DstAddr, namerd};
use futures::{Future, Sink, Poll, Async, StartSend, AsyncSink};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

pub fn new(addrs: namerd::Result<Vec<DstAddr>>) -> Balancer {
    Balancer { addrs: Rc::new(RefCell::new(addrs)) }
}

/// Balancers accept a stream of service discovery updates and
///
///
#[derive(Clone)]
pub struct Balancer {
    addrs: Rc<RefCell<namerd::Result<Vec<DstAddr>>>>,
}

impl Balancer {
    pub fn connect(&mut self) -> Connect {
        unimplemented!()
    }
}

impl Sink for Balancer {
    type SinkItem = namerd::Result<Vec<DstAddr>>;
    type SinkError = ();

    /// Update the load balancer from service discovery.
    fn start_send(&mut self,
                  update: namerd::Result<Vec<DstAddr>>)
                  -> StartSend<namerd::Result<Vec<DstAddr>>, Self::SinkError> {
        match update {
            Ok(new_addrs) => {
                // TODO this is where we will update the load balancer's state to retire lapsed
                // endpoints.  Should new connections be initiated here as well?
                let mut addrs = self.addrs.borrow_mut();
                *addrs = new_addrs;
            }
            Err(e) => warn!("balancer update error: {:?}", e),
        }
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
