use super::shared::Shared;
use futures::{Poll, Sink, StartSend};
use std::cell::RefCell;
use std::rc::Rc;

pub fn new<T>(shared: Rc<RefCell<Shared<T>>>) -> Sender<T> {
    Sender(shared)
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

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender(self.0.clone())
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
