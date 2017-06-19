use futures::{Future, Stream, Poll, Async, AsyncSink, Sink, StartSend};
use futures::unsync::{oneshot, mpsc};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

/// Creates a dispatching channel.
///
/// This channel supports a single producer task and many consumers.
/// It is intended to be used by a producer task that
pub fn channel<T>(send_capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (recvq_tx, recvq_rx) = mpsc::unbounded();
    let sendq = Rc::new(RefCell::new(VecDeque::with_capacity(send_capacity)));
    let pending = Rc::new(RefCell::new(0));
    let tx = Sender {
        recvq_rx,
        sendq: sendq.clone(),
        pending: pending.clone(),
    };
    let rx = Receiver { recvq_tx, pending };
    (tx, rx)
}

pub struct Sender<T> {
    recvq_rx: mpsc::UnboundedReceiver<oneshot::Sender<T>>,
    sendq: Rc<RefCell<VecDeque<T>>>,
    pending: Rc<RefCell<usize>>,
}
impl<T> Sender<T> {
    pub fn sendq_capacity(&self) -> usize {
        self.sendq.borrow().capacity()
    }

    pub fn sendq_size(&self) -> usize {
        self.sendq.borrow().len()
    }

    fn dispatch(&mut self) -> Async<()> {
        let mut sendq = self.sendq.borrow_mut();
        while let Some(v) = sendq.pop_front() {
            trace!("dispatch: took waiter from sendq; polling recvq");
            match self.recvq_rx.poll() {
                Ok(Async::NotReady) => {
                    trace!("dispatch: recvq not ready");
                    sendq.push_front(v);
                    return Async::NotReady;
                }
                Ok(Async::Ready(None)) => {
                    trace!("dispatch: recvq done");
                    return Async::Ready(());
                }
                Err(_) => {
                    trace!("dispatch: recvq failed");
                    sendq.push_front(v);
                }
                Ok(Async::Ready(Some(recv))) => {
                    *self.pending.borrow_mut() -= 1;
                    trace!("dispatch: recvq satisfied: sending waiter");
                    if let Err(v) = recv.send(v) {
                        trace!("dispatch: sending waiter failed");
                        sendq.push_front(v);
                    }
                }
            }
        }
        Async::Ready(())
    }
}

impl<T> Sink for Sender<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, ()> {
        {
            let mut sendq = self.sendq.borrow_mut();
            trace!("start_send: dispatching sendq={}/{}",
                   sendq.len(),
                   sendq.capacity());
            if sendq.capacity() == sendq.len() {
                return Ok(AsyncSink::NotReady(item));
            }

            sendq.push_back(item);
        }
        self.dispatch();
        {
            let sendq = self.sendq.borrow();
            trace!("start_send: dispatched sendq={}/{}",
                   sendq.len(),
                   sendq.capacity());
        }
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), ()> {
        {
            let sendq = self.sendq.borrow();
            trace!("poll_complete: dispatching sendq={}/{}",
                   sendq.len(),
                   sendq.capacity());
        }
        let res = self.dispatch();
        {
            let sendq = self.sendq.borrow();
            trace!("poll_complete: dispatched sendq={}/{} ready={}",
                   sendq.len(),
                   sendq.capacity(),
                   res.is_ready());
        }
        Ok(res)
    }
}

pub struct Receiver<T> {
    recvq_tx: mpsc::UnboundedSender<oneshot::Sender<T>>,
    pending: Rc<RefCell<usize>>,
}
impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Receiver<T> {
        Receiver {
            recvq_tx: self.recvq_tx.clone(),
            pending: self.pending.clone(),
        }
    }
}
impl<T> Receiver<T> {
    pub fn pending(&self) -> usize {
        *self.pending.borrow()
    }

    pub fn recv(&self) -> Recv<T> {
        trace!("receiver: recv (pending={})", self.pending());
        Recv {
            rx: None,
            recvq_tx: self.recvq_tx.clone(),
            pending: self.pending.clone(),
        }
    }
}

pub struct Recv<T> {
    recvq_tx: mpsc::UnboundedSender<oneshot::Sender<T>>,
    rx: Option<oneshot::Receiver<T>>,
    pending: Rc<RefCell<usize>>,
}

impl<T> Future for Recv<T> {
    type Item = T;
    type Error = ();
    fn poll(&mut self) -> Poll<T, ()> {
        trace!("recv: poll (rx={}, pending={})",
               self.rx.is_some(),
               *self.pending.borrow());
        let mut rx = match self.rx.take() {
            Some(rx) => rx,
            None => {
                trace!("recv: creating oneshot");
                let (tx, rx) = oneshot::channel();
                if mpsc::UnboundedSender::send(&self.recvq_tx, tx).is_err() {
                    trace!("recv: sending oneshot failed");
                    return Err(());
                }
                *self.pending.borrow_mut() += 1;
                rx
            }
        };

        let res = rx.poll().map_err(|_| {})?;
        trace!("recv: poll (ready={}, pending={})",
               res.is_ready(),
               *self.pending.borrow());
        if !res.is_ready() {
            self.rx = Some(rx);
        }
        Ok(res)
    }
}
