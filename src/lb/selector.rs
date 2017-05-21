use super::super::DstConnection;
use futures::{Future, Poll, Async};
use futures::unsync::{mpsc, oneshot};
use std::io;

pub fn new(waiters: mpsc::UnboundedSender<DstConnectionRequest>) -> Selector {
    Selector(waiters)
}

/// Selects
// TODO limit max waiters.
#[derive(Clone)]
pub struct Selector(mpsc::UnboundedSender<DstConnectionRequest>);

/// The response-side of a request from a `Selector` for a `DstConnection`.
pub type DstConnectionRequest = oneshot::Sender<DstConnection>;

impl Selector {
    /// Obtains a connection to the destination.
    pub fn select(&self) -> Select {
        let (waiter, pending) = oneshot::channel();
        let result = match self.0.send(waiter) {
            // XXX what should we actually do here?
            Err(_) => Err(io::ErrorKind::Other.into()),
            Ok(_) => Ok(pending),
        };
        Select(Some(result))
    }
}

pub struct Select(Option<io::Result<oneshot::Receiver<DstConnection>>>);

impl Future for Select {
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
