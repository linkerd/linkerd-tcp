use futures::{Future, Poll, Async, AsyncSink, Sink, StartSend, unsync};
use std::cell::RefCell;
use std::cmp;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

pub fn channel<T>(capacity: usize) -> ChannelQ<T> {
    let inner = Inner {
        listeners: VecDeque::new(),
        buffer: VecDeque::with_capacity(cmp::max(capacity, 1) - 1),
    };
    ChannelQ(Rc::new(RefCell::new(inner)))
}

struct Inner<T> {
    listeners: VecDeque<unsync::oneshot::Sender<T>>,
    buffer: VecDeque<T>,
}
impl<T> Inner<T> {
    fn listen(&mut self) -> unsync::oneshot::Receiver<T> {
        let (tx, rx) = unsync::oneshot::channel();
        self.listeners.push_back(tx);
        rx
    }

    fn dispatch(&mut self, item: T) -> AsyncSink<T> {
        match self.listeners.pop_front() {
            None => AsyncSink::NotReady(item),
            Some(tx) => {
                match tx.send(item) {
                    Err(item) => self.dispatch(item),
                    Ok(()) => AsyncSink::Ready,
                }
            }
        }
    }

    fn flush_listeners(&mut self) {
        for _ in 0..self.listeners.len() {
            let mut l = self.listeners.pop_front().unwrap();
            if let Ok(Async::NotReady) = l.poll_cancel() {
                self.listeners.push_back(l);
            }
        }
    }
}
impl<T> Sink for Inner<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, ()> {
        if self.buffer.is_empty() {
            if let AsyncSink::NotReady(item) = self.dispatch(item) {
                self.buffer.push_back(item);
            }
            return Ok(AsyncSink::Ready);
        }

        if !self.listeners.is_empty() {
            while let Some(other) = self.buffer.pop_front() {
                if let AsyncSink::NotReady(other) = self.dispatch(other) {
                    self.buffer.push_back(other);
                    break;
                }
            }
        }

        if self.buffer.capacity() == self.buffer.len() {
            return Ok(AsyncSink::NotReady(item));
        }

        self.buffer.push_back(item);
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), ()> {
        self.flush_listeners();
        if self.listeners.is_empty() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

pub struct ChannelQ<T>(Rc<RefCell<Inner<T>>>);
impl<T> ChannelQ<T> {
    fn capacity(&self) -> usize {
        self.0.borrow().buffer.capacity()
    }

    fn len(&self) -> usize {
        self.0.borrow().buffer.len()
    }

    fn listeners(&self) -> usize {
        self.0.borrow().listeners.len()
    }

    pub fn recv(&self) -> Recv<T> {
        Recv {
            inner: self.0.clone(),
            pending: None,
        }
    }
}

impl<T> Sink for ChannelQ<T> {
    type SinkItem = T;
    type SinkError = ();
    fn start_send(&mut self, item: T) -> StartSend<T, ()> {
        self.0.borrow_mut().start_send(item)
    }
    fn poll_complete(&mut self) -> Poll<(), ()> {
        self.0.borrow_mut().poll_complete()
    }
}

pub struct Recv<T> {
    inner: Rc<RefCell<Inner<T>>>,
    pending: Option<unsync::oneshot::Receiver<T>>,
}
impl<T> Future for Recv<T> {
    type Item = T;
    type Error = ();
    fn poll(&mut self) -> Poll<T, ()> {
        if let Some(mut pending) = self.pending.take() {
            match pending.poll() {
                Err(_) => {}
                Ok(Async::Ready(v)) => {
                    return Ok(Async::Ready(v));
                }
                Ok(Async::NotReady) => {
                    self.pending = Some(pending);
                    return Ok(Async::NotReady);
                }
            }
        }

        let rx = self.inner.borrow_mut().listen();
        self.pending = Some(rx);
        Ok(Async::NotReady)
    }
}
