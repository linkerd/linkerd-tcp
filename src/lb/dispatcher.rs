use super::manager::Waiter;
use super::super::{DstConnection, Path};
use super::super::connector::Connector;
use super::super::resolver::Resolve;
use futures::{Future, Stream, Poll, Async};
use futures::unsync::{mpsc, oneshot};
use rand::{self, Rng};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;

pub fn new(waiters: mpsc::UnboundedSender<Waiter>) -> Dispatcher {
    Dispatcher(waiters)
}

// TODO we should limit max waiteers probably.
#[derive(Clone)]
pub struct Dispatcher(mpsc::UnboundedSender<Waiter>);
impl Dispatcher {
    /// Obtain an established connection immediately or wait until one becomes available.
    pub fn dispatch(&self) -> Dispatch {
        // Add a waiter to the pool. When the Manager is able to obtain
        let (waiter, pending) = oneshot::channel();
        if let Err(_) = self.0.send(waiter) {
            // XXX what should we actually do here?
            Dispatch(Some(Err(io::ErrorKind::Other.into())))
        } else {
            Dispatch(Some(Ok(pending)))
        }
    }
}

pub struct Dispatch(Option<io::Result<oneshot::Receiver<DstConnection>>>);
impl Future for Dispatch {
    type Item = DstConnection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut recv = self.0
            .take()
            .expect("connect must not be polled after completion")?;
        match recv.poll() {
            Err(_) => Err(io::Error::new(io::ErrorKind::Interrupted, "canceled")),
            Ok(Async::Ready(conn)) => Ok(Async::Ready(conn)),
            Ok(Async::NotReady) => {
                self.0 = Some(Ok(recv));
                Ok(Async::NotReady)
            }
        }
    }
}
