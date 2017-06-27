use super::SenderLost;
use super::shared::{RecvHandle, Shared};
use futures::{Future, Poll, Async};
use std::cell::RefCell;
use std::rc::{Rc, Weak};

pub fn new<T>(shared: &Rc<RefCell<Shared<T>>>) -> Receiver<T> {
    Receiver { shared: Rc::downgrade(shared) }
}

/// A Stream of values from a shared channel.
///
/// Streams obtain values on-demand and in the order requested.
///
/// Receivers may be cloned. Cloned receivers act independently to compete for values on
/// the underlying channel.
pub struct Receiver<T> {
    /// Holds a weak reference to the shared state of the channel. The Stream is closed
    /// when then sender is dropped.
    shared: Weak<RefCell<Shared<T>>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver { shared: self.shared.clone() }
    }
}

impl<T> Receiver<T> {
    pub fn recv(self) -> Recv<T> {
        Recv(self.shared.upgrade().map(|s| s.borrow_mut().recv()))
    }
}

/// Obtains a value from the channel asynchronously.
pub struct Recv<T>(Option<RecvHandle<T>>);
impl<T> Future for Recv<T> {
    type Item = T;
    type Error = SenderLost;
    fn poll(&mut self) -> Poll<T, SenderLost> {
        match self.0.take() {
            None => Err(SenderLost()),
            Some(mut recv) => {
                match recv.poll()? {
                    Async::NotReady => {
                        self.0 = Some(recv);
                        Ok(Async::NotReady)
                    }
                    ready => Ok(ready),
                }
            }
        }
    }
}
