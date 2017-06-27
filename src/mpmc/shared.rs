use super::SenderLost;
use futures::{Poll, Async, AsyncSink, Sink, StartSend};
use futures::task::{self, Task};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

pub fn new<T>(capacity: usize) -> Shared<T> {
    Shared {
        sendq_capacity: capacity,
        state: None,
        dispatching: VecDeque::new(),
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
    blocked_send: Option<Task>,
    sendq_capacity: usize,

    state: Option<State<T>>,
    dispatching: VecDeque<DispatchedRecv<T>>,
}

enum State<T> {
    Sending(VecDeque<T>),
    Receiving(VecDeque<BlockedRecv<T>>),
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
        let sendq = match self.state.as_ref() {
            Some(&State::Sending(ref sq)) => sq.len(),
            _ => 0,
        };
        sendq + self.dispatching.len()
    }

    /// Indicates whether the channel has no buffered items.
    pub fn is_empty(&self) -> bool {
        match self.state.as_ref() {
            Some(&State::Sending(ref sq)) => sq.is_empty(),
            _ => true,
        }
    }

    fn notify_sender(&mut self) {
        // If the sender is waiting for receivers, notify it.
        if let Some(task) = self.blocked_send.take() {
            task.notify();
        }
    }

    /// Tries to receive an item from the queue.
    ///
    /// If an item cannot be satisfied immediately, the current task is saved to be
    /// notified when an item may be consumed (i.e. by `notify_recvs`).
    pub fn recv(&mut self) -> RecvHandle<T> {
        let mut recvq = match self.state.take() {
            Some(State::Sending(mut sendq)) => {
                if let Some(item) = sendq.pop_front() {
                    self.state = Some(State::Sending(sendq));
                    self.notify_sender();
                    return RecvHandle::Ready(Some(item));
                }
                VecDeque::new()
            }
            None => VecDeque::new(),            
            Some(State::Receiving(recvq)) => recvq,
        };

        let slot = RecvSlot::new();
        let handle = slot.pending();
        recvq.push_back(BlockedRecv(task::current(), slot));
        self.state = Some(State::Receiving(recvq));
        self.notify_sender();
        handle
    }
}

impl<T> Sink for Shared<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, Self::SinkError> {
        let mut sendq = match self.state.take() {
            Some(State::Receiving(mut recvq)) => {
                if let Some(BlockedRecv(task, mut slot)) = recvq.pop_front() {
                    slot.set(item);
                    task.notify();
                    self.dispatching.push_back(DispatchedRecv(slot));
                    return Ok(AsyncSink::Ready);
                }
                VecDeque::new()
            }
            None => VecDeque::new(),            
            Some(State::Sending(sendq)) => sendq,
        };

        if sendq.len() + self.dispatching.len() < self.sendq_capacity {
            self.blocked_send = None;
            sendq.push_back(item);
            self.state = Some(State::Sending(sendq));
            Ok(AsyncSink::Ready)
        } else {
            // Ask to be notified when there's more capacity in the channel and ensure
            // that pending receivers have been informed that there are pending values.
            self.blocked_send = Some(task::current());
            self.state = Some(State::Sending(sendq));
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        for _ in 0..self.dispatching.len() {
            let DispatchedRecv(slot) = self.dispatching.pop_front().unwrap();
            if slot.is_orphaned() {
                if let Some(item) = slot.take() {
                    let sendq = match self.state.take() {
                        Some(State::Receiving(mut recvq)) => {
                            if let Some(BlockedRecv(task, mut slot)) = recvq.pop_front() {
                                slot.set(item);
                                task.notify();
                                self.dispatching.push_back(DispatchedRecv(slot));
                                continue;
                            }
                            VecDeque::new()
                        }
                        None => VecDeque::new(),            
                        Some(State::Sending(sendq)) => sendq,
                    };
                    unimplemented!();
                }
            }
            self.dispatching.push_back(DispatchedRecv(slot));
        }

        if self.dispatching.is_empty() {
            if let Some(&State::Sending(ref sendq)) = self.state.as_ref() {
                if sendq.is_empty() {
                    return Ok(Async::Ready(()));
                }
            }
        }
        Ok(Async::NotReady)
    }
}

struct BlockedRecv<T>(Task, RecvSlot<T>);

struct DispatchedRecv<T>(RecvSlot<T>);
impl<T> DispatchedRecv<T> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn is_orphaned(&self) -> bool {
        self.0.is_orphaned()
    }
}

struct RecvSlot<T>(Rc<RefCell<Option<T>>>);
impl<T> Clone for RecvSlot<T> {
    fn clone(&self) -> Self {
        RecvSlot(self.0.clone())
    }
}
impl<T> RecvSlot<T> {
    fn new() -> RecvSlot<T> {
        RecvSlot(Rc::new(RefCell::new(None)))
    }

    fn set(&mut self, t: T) {
        *self.0.borrow_mut() = Some(t);
    }

    fn is_empty(&self) -> bool {
        self.0.borrow().is_none()
    }

    fn take(&self) -> Option<T> {
        self.0.borrow_mut().take()
    }

    fn is_orphaned(&self) -> bool {
        Rc::strong_count(&self.0) == 1
    }

    fn pending(&self) -> RecvHandle<T> {
        RecvHandle::Pending(Rc::downgrade(&self.0))
    }
}

pub enum RecvHandle<T> {
    Pending(Weak<RefCell<Option<T>>>),
    Ready(Option<T>),
}
impl<T> RecvHandle<T> {
    pub fn poll(&mut self) -> Poll<T, SenderLost> {
        match self {
            &mut RecvHandle::Ready(ref mut val) => Ok(Async::Ready(val.take().unwrap())),
            &mut RecvHandle::Pending(ref mut pending) => {
                match pending.upgrade() {
                    None => Err(SenderLost()),
                    Some(slot) => {
                        match slot.borrow_mut().take() {
                            Some(t) => Ok(Async::Ready(t)),
                            None => Ok(Async::NotReady),
                        }
                    }
                }
            }
        }
    }
}
