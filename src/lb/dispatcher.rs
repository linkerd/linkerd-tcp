use super::super::DstConnection;
use futures::{Future, Poll, Async};
use futures::unsync::{mpsc, oneshot};
use std::io;

pub fn new(waiters: mpsc::UnboundedSender<Dispatchee>) -> Dispatcher {
    Dispatcher(waiters)
}

// TODO limit max waiters.
#[derive(Clone)]
pub struct Dispatcher(mpsc::UnboundedSender<Dispatchee>);

/// The response-side of a request from a `Dispatcher` for a `DstConnection`.
pub type Dispatchee = oneshot::Sender<DstConnection>;

impl Dispatcher {
    /// Obtains a connection to the destination.
    pub fn dispatch(&self) -> Dispatch {
        let (waiter, pending) = oneshot::channel();
        let result = match self.0.send(waiter) {
            // XXX what should we actually do here?
            Err(_) => Err(io::ErrorKind::Other.into()),
            Ok(_) => Ok(pending),
        };
        Dispatch(Some(result))
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
