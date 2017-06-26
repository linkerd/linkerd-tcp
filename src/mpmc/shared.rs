use futures::{Poll, Async, AsyncSink, Sink, StartSend};
use futures::task::{self, Task};
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

pub fn new<T>(capacity: usize) -> Shared<T> {
    Shared {
        sendq_capacity: capacity,
        sendq: VecDeque::new(),
        blocked_recvs: VecDeque::new(),
        blocked_send: None,
    }
}

/// Holds the shared internal state of a single-producer multi-consumer channel.
///
/// A `Sink` is exposed to accept T-typed values into an internal queue. The sink will
/// buffer up to `sendq_capacity` items before applying backpressure.
///
/// If a `Receiver` attempts to obtain a value but the sendq is empty (or other receivers
/// are blocked), then the receiver's task is saved to be notified when the sendq has a
/// value for the receiver..
pub struct Shared<T> {
    sendq_capacity: usize,
    sendq: VecDeque<T>,
    blocked_recvs: VecDeque<Weak<Task>>,
    blocked_send: Option<Task>,
}

impl<T> Shared<T> {
    /// Gets the number of items that may be enqueued currently.
    pub fn available_capacity(&self) -> usize {
        self.sendq_capacity - self.len()
    }

    /// Indicates whether the channel is currently at capacity.
    pub fn is_full(&self) -> bool {
        self.available_capacity() == 0
    }

    /// Gets the numbner of items buffered in the channel.
    pub fn len(&self) -> usize {
        self.sendq.len()
    }

    /// Indicates whether the channel has no buffered items.
    pub fn is_empty(&self) -> bool {
        self.sendq.is_empty()
    }

    /// Notifies `self.len()` blocked receivers to receive items.
    pub fn notify_recvs(&mut self) -> usize {
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

    /// Tries to receive an item from the queue.
    ///
    /// If an item cannot be satisfied immediately, the current task is saved to be
    /// notified when an item may be consumed (i.e. by `notify_recvs`).
    pub fn poll_recv(&mut self) -> PollRecv<T> {
        // If  the sender is waiting for receivers, notify it.
        if let Some(task) = self.blocked_send.take() {
            task.notify();
        }

        // If there are no pending receivers, try to take a queued item.
        let item = if self.blocked_recvs.is_empty() {
            self.sendq.pop_front()
        } else {
            None
        };

        // If an item isn't immediately available, queue the current task to be
        // notified by the sender.
        match item {
            Some(item) => PollRecv::Ready(item),
            None => {
                let task = Rc::new(task::current());
                self.blocked_recvs.push_back(Rc::downgrade(&task));
                PollRecv::NotReady(task)
            }
        }
    }
}

pub enum PollRecv<T> {
    Ready(T),
    NotReady(Rc<Task>),
}

impl<T> Sink for Shared<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, Self::SinkError> {
        if self.sendq.len() < self.sendq_capacity {
            self.blocked_send = None;
            self.sendq.push_back(item);
            Ok(AsyncSink::Ready)
        } else {
            // Attempt to notify waiting receivers of available items.
            self.notify_recvs();

            // Ask to be notified when there's more capacity in the channel and ensure
            // that pending receivers have been informed that there are pending values.
            self.blocked_send = Some(task::current());
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.sendq.is_empty() {
            self.blocked_send = None;
            Ok(Async::Ready(()))
        } else {
            // Attempt to notify waiting receivers of available items.
            self.notify_recvs();

            // Ask to be notified when receivers have acted consumed the sendq.
            self.blocked_send = Some(task::current());
            Ok(Async::NotReady)
        }
    }
}
