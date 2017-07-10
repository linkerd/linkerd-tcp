use super::SenderLost;
use futures::{Poll, Async, AsyncSink, Sink, StartSend};
use futures::task::{self, Task};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

pub fn new<T>(capacity: usize) -> Shared<T> {
    Shared {
        send_capacity: capacity,
        state: None,
        sending: VecDeque::new(),
        blocked_send: None,
    }
}

/// Holds the shared internal state of a single-producer multi-consumer channel.
///
/// A `Sink` is exposed to accept T-typed values into an internal queue. The sink will
/// buffer up to `send_capacity` items before applying backpressure.
///
/// If a `Receiver` attempts to obtain a value but the sendq is empty (or other receivers
/// are blocked), then the receiver's task is saved to be notified when the sendq has a
/// value for the receiver..
pub struct Shared<T> {
    blocked_send: Option<Task>,
    send_capacity: usize,
    state: Option<State<T>>,
    sending: VecDeque<Sending<T>>,
}

enum State<T> {
    Buffering(VecDeque<T>),
    Receiving(VecDeque<BlockedRecv<T>>),
}

impl<T> Shared<T> {
    /// Gets the number of items that may be enqueued currently.
    pub fn available_capacity(&self) -> usize {
        self.send_capacity - self.len()
    }

    /// Indicates whether the channel is currently at capacity.
    pub fn is_full(&self) -> bool {
        self.available_capacity() == 0
    }

    /// Gets the numbner of items buffered in the channel.
    pub fn len(&self) -> usize {
        let sendq = match self.state.as_ref() {
            Some(&State::Buffering(ref sq)) => sq.len(),
            _ => 0,
        };
        sendq + self.sending.len()
    }

    /// Indicates whether the channel has no buffered items.
    pub fn is_empty(&self) -> bool {
        match self.state.as_ref() {
            Some(&State::Buffering(ref sq)) => sq.is_empty(),
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
            Some(State::Buffering(mut sendq)) => {
                if let Some(item) = sendq.pop_front() {
                    self.state = Some(State::Buffering(sendq));
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

    fn resend(&mut self, item: T) {
        let mut sendq = match self.state.take() {
            Some(State::Receiving(mut recvq)) => {
                if let Some(BlockedRecv(task, mut slot)) = recvq.pop_front() {
                    slot.set(item);
                    task.notify();
                    self.sending.push_back(Sending(slot));
                    if !recvq.is_empty() {
                        self.state = Some(State::Receiving(recvq));
                    }
                    return;
                }

                VecDeque::new()
            }
            None => VecDeque::new(),            
            Some(State::Buffering(sendq)) => sendq,
        };

        // Cut the line, ignoring capacity limitations (because the provided item must
        // have been taken out of the sending queue).
        sendq.push_front(item);
        self.state = Some(State::Buffering(sendq));
    }

    fn drain_sending(&mut self) {
        for _ in 0..self.sending.len() {
            let send = self.sending.pop_front().unwrap();
            if send.lost_receiver() {
                if let Some(item) = send.reclaim() {
                    self.resend(item);
                }
                drop(send);
            } else {
                self.sending.push_back(send);
            }
        }
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
                    self.sending.push_back(Sending(slot));
                    return Ok(AsyncSink::Ready);
                }
                VecDeque::new()
            }
            None => VecDeque::new(),            
            Some(State::Buffering(sendq)) => sendq,
        };

        if sendq.len() + self.sending.len() == self.send_capacity {
            self.drain_sending();
            if sendq.len() + self.sending.len() == self.send_capacity {
                // Ask to be notified when there's more capacity in the channel and ensure
                // that pending receivers have been informed that there are pending values.
                self.blocked_send = Some(task::current());
                self.state = Some(State::Buffering(sendq));
                return Ok(AsyncSink::NotReady(item));
            }
        }

        self.blocked_send = None;
        sendq.push_back(item);
        self.state = Some(State::Buffering(sendq));
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.drain_sending();
        if !self.sending.is_empty() {
            self.blocked_send = Some(task::current());
            return Ok(Async::NotReady);
        }

        match self.state.as_ref() {
            Some(&State::Buffering(ref sq)) if !sq.is_empty() => {
                self.blocked_send = Some(task::current());
                Ok(Async::NotReady)
            }
            _ => Ok(Async::Ready(())),
        }
    }
}

struct BlockedRecv<T>(Task, RecvSlot<T>);

struct Sending<T>(RecvSlot<T>);
impl<T> Sending<T> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn lost_receiver(&self) -> bool {
        self.0.ref_count() == 1
    }

    fn reclaim(&self) -> Option<T> {
        self.0.take()
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

    fn ref_count(&self) -> usize {
        Rc::strong_count(&self.0)
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
