use super::shared::{PollRecv, Shared};
use futures::{Future, Stream, Poll, Async};
use futures::task::Task;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::rc::{Rc, Weak};

pub fn new<T>(shared: &Rc<RefCell<Shared<T>>>) -> Receiver<T> {
    Receiver {
        current: None,
        shared: Rc::downgrade(shared),
    }
}

/// A Stream of values from a shared channel.
///
/// Streams obtain values on-demand and in the order requested.
///
/// Receivers may be cloned. Cloned receivers act independently to compete for values on
/// the underlying channel.
pub struct Receiver<T> {
    /// Holds a strong reference to the pending task in the shared recv queue. The shared
    /// recv queue holds weak references so that dropped receivers are ignored.
    current: Option<Rc<Task>>,

    /// Holds a weak reference to the shared state of the channel. The Stream is closed
    /// when then sender is dropped.
    shared: Weak<RefCell<Shared<T>>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            current: None,
            shared: self.shared.clone(),
        }
    }
}

impl<T> Receiver<T> {
    pub fn recv(self) -> Recv<T> {
        Recv(Some(self))
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<T>, ()> {
        self.current.take();
        match self.shared.upgrade() {
            None => Ok(Async::Ready(None)),
            Some(rx) => {
                match rx.borrow_mut().poll_recv() {
                    PollRecv::Ready(item) => Ok(Async::Ready(Some(item))),
                    PollRecv::NotReady(task) => {
                        // Hold a reference to the current task so that the shared channel
                        // knows this receiver is still interested in receiving a result.
                        // If this receiver is dropped, the shared channel is able to
                        // notify another receiver instead.
                        self.current = Some(task);
                        Ok(Async::NotReady)
                    }
                }
            }
        }
    }
}

/// Obtains a single value from the channel asynchronously.
pub struct Recv<T>(Option<Receiver<T>>);
impl<T> Future for Recv<T> {
    type Item = T;
    type Error = RecvLostSender;
    fn poll(&mut self) -> Poll<T, RecvLostSender> {
        let mut recv = self.0.take().expect("polled after completion");
        match recv.poll() {
            Ok(Async::Ready(Some(t))) => Ok(Async::Ready(t)),
            Ok(Async::NotReady) => {
                self.0 = Some(recv);
                Ok(Async::NotReady)
            }
            _ => Err(RecvLostSender()),
        }
    }
}

#[derive(Debug)]
pub struct RecvLostSender();
impl fmt::Display for RecvLostSender {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "recv failed because sender is gone")
    }
}
impl Error for RecvLostSender {
    fn description(&self) -> &str {
        "recv failed because sender is gone"
    }
}
