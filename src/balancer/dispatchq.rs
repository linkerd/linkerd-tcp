use futures::{Future, Poll, Async, AsyncSink, Sink, StartSend};
use futures::task::{self, Task};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::rc::{Rc, Weak};

/// Creates a dispatching channel.
///
/// This channel supports a single producer task and many consumers.
/// It is intended to be used by a producer task that
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(Shared {
        capacity,
        buffer: VecDeque::new(),
        blocked_recvs: VecDeque::new(),
        blocked_sender: None,
    }));
    let rx = Receiver(Rc::downgrade(&shared));
    let tx = Sender(shared);
    (tx, rx)
}

#[derive(Debug)]
struct Shared<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    blocked_sender: Option<Task>,
    blocked_recvs: VecDeque<Weak<Task>>,
}

impl<T> Shared<T> {
    fn available_capacity(&self) -> usize {
        self.capacity - self.buffer.len()
    }
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
    fn is_full(&self) -> bool {
        self.available_capacity() == 0
    }
    fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<T> Sink for Shared<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, Self::SinkError> {
        if self.buffer.len() == self.capacity {
            warn!("start_send: sendq at capacity");
            self.blocked_sender = Some(task::current());
            return Ok(AsyncSink::NotReady(item));
        }

        self.buffer.push_back(item);
        trace!(
            "start_send: sendq={}/{} recvq={}",
            self.buffer.len(),
            self.capacity,
            self.blocked_recvs.len(),
        );

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!(
            "poll_complete: dispatching sendq={}/{} recvq={}",
            self.buffer.len(),
            self.capacity,
            self.blocked_recvs.len()
        );

        let mut notified = 0;
        while notified != self.buffer.len() {
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

        let res = if notified == self.buffer.len() {
            Async::Ready(())
        } else {
            Async::NotReady
        };
        trace!(
            "poll_complete: dispatched sendq={}/{} recvq={} ready={}",
            self.buffer.len(),
            self.capacity,
            self.blocked_recvs.len(),
            res.is_ready()
        );

        Ok(res)
    }
}
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

pub struct Receiver<T>(Weak<RefCell<Shared<T>>>);

impl<T> Receiver<T> {
    pub fn recv(&self) -> Recv<T> {
        Recv {
            shared: self.0.clone(),
            current: None,
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver(self.0.clone())
    }
}

pub struct Recv<T> {
    current: Option<Rc<Task>>,
    shared: Weak<RefCell<Shared<T>>>,
}
impl<T> Future for Recv<T> {
    type Item = T;
    type Error = RecvError;
    fn poll(&mut self) -> Poll<T, RecvError> {
        match self.shared.upgrade() {
            None => {
                self.current = None;
                Err(RecvError())
            }
            Some(s) => {
                let mut shared = s.borrow_mut();
                if let Some(sender) = shared.blocked_sender.take() {
                    // If the sender was waiting, notify that a receiver is freeing
                    // capacity or ready to receive..
                    sender.notify();
                }
                match shared.buffer.pop_front() {
                    None => {
                        let current = Rc::new(task::current());
                        shared.blocked_recvs.push_back(Rc::downgrade(&current));
                        self.current = Some(current);
                        Ok(Async::NotReady)
                    }
                    Some(item) => {
                        self.current = None;
                        Ok(Async::Ready(item))
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RecvError();
impl fmt::Display for RecvError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "recv failed because sender is gone")
    }
}
impl Error for RecvError {
    fn description(&self) -> &str {
        "recv failed because sender is gone"
    }
}
