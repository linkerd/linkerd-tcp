use futures::{Future, Stream, Poll, Async, AsyncSink, Sink, StartSend};
use futures::task::{self, Task};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::rc::{Rc, Weak};

/// Creates a channel capable of receiving values from an .
///
/// It is intended to be used by producer task that dispatches to an arbitrary number of
/// worker tasks on a single Core.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(Shared {
        sendq_capacity: capacity,
        sendq: VecDeque::new(),
        blocked_recvs: VecDeque::new(),
        blocked_send: None,
    }));
    let rx = Receiver {
        current: None,
        shared: Rc::downgrade(&shared),
    };
    (Sender(shared), rx)
}

/// Holds the shared internal state of a single-producer multi-consumer channel.
///
/// A `Sink` is exposed to accept T-typed values into an internal queue. The sink will
/// buffer up to `sendq_capacity` items before applying backpressure.
///
/// As `Receiver`s attempt to pull values from the sendq,
struct Shared<T> {
    sendq_capacity: usize,
    sendq: VecDeque<T>,
    blocked_recvs: VecDeque<Weak<Task>>,
    blocked_send: Option<Task>,
}

impl<T> Shared<T> {
    fn available_capacity(&self) -> usize {
        self.sendq_capacity - self.len()
    }
    fn is_empty(&self) -> bool {
        self.sendq.is_empty()
    }
    fn is_full(&self) -> bool {
        self.available_capacity() == 0
    }
    fn len(&self) -> usize {
        self.sendq.len()
    }

    fn notify_recvs(&mut self) -> usize {
        let mut notified = 0;
        while notified != self.len() {
            match self.blocked_recvs.pop_front() {
                None => break,
                Some(t) => {
                    if let Some(recv) = t.upgrade() {
                        recv.notify();
                        notified += 1;
                    }
                }
            }
        }
        notified
    }
}

impl<T> Sink for Shared<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, Self::SinkError> {
        if self.sendq.len() < self.sendq_capacity {
            self.sendq.push_back(item);
            Ok(AsyncSink::Ready)
        } else {
            // Ask to be notified when there's more capacity in the channel and ensure
            // that pending receivers have been informed that there are pending values.
            self.blocked_send = Some(task::current());
            self.notify_recvs();
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.sendq.is_empty() {
            Ok(Async::Ready(()))
        } else {
            // Ask to be notified when more receivers are available.
            self.blocked_send = Some(task::current());
            self.notify_recvs();
            Ok(Async::NotReady)
        }
    }
}

/// Exposes the shared channel as a Sink.
pub struct Sender<T>(Rc<RefCell<Shared<T>>>);

impl<T> Sender<T> {
    pub fn available_capacity(&self) -> usize {
        self.0.borrow().available_capacity()
    }
    pub fn is_empty(&self) -> bool {
        self.0.borrow().is_empty()
    }
    pub fn is_full(&self) -> bool {
        self.0.borrow().is_full()
    }
    pub fn len(&self) -> usize {
        self.0.borrow().len()
    }
}

impl<T> Sink for Sender<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, Self::SinkError> {
        self.0.borrow_mut().start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.0.borrow_mut().poll_complete()
    }
}

impl<'a, T> Sink for &'a Sender<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, Self::SinkError> {
        self.0.borrow_mut().start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.0.borrow_mut().poll_complete()
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
            Some(shared) => {
                let mut shared = shared.borrow_mut();

                // If  the sender is waiting for receivers, notify it.
                if let Some(task) = shared.blocked_send.take() {
                    task.notify();
                }

                // If there are no pending receivers, try to take a queued item.
                let item = if shared.blocked_recvs.is_empty() {
                    shared.sendq.pop_front()
                } else {
                    None
                };

                // If an item isn't immediately available, queue the current task to be
                // notified by the sender.
                if item.is_some() {
                    Ok(Async::Ready(item))
                } else {
                    let task = Rc::new(task::current());
                    shared.blocked_recvs.push_back(Rc::downgrade(&task));
                    self.current = Some(task);
                    Ok(Async::NotReady)
                }
            }
        }
    }
}

/// Obtains a single value from the channel.
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
