use super::pool::{Pool, Waiter};
use super::super::{DstConnection, Path};
use super::super::connector::Connector;
use super::super::resolver::Resolve;
use futures::{Future, Stream, Sink, Poll, Async, StartSend, AsyncSink};
use futures::unsync::{mpsc, oneshot};
use rand::{self, Rng};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;

pub fn new(on_dispatch: mpsc::UnboundedSender<()>, pool: Rc<Pool>) -> Dispatcher {
    Dispatcher {
        on_dispatch: on_dispatch,
        pool: pool,
    }
}

#[derive(Clone)]
pub struct Dispatcher {
    on_dispatch: mpsc::UnboundedSender<()>,
    pool: Rc<Pool>,
}

impl Dispatcher {
    /// Obtain an established connection immediately or wait until one becomes available.
    pub fn dispatch(&self) -> Dispatch {
        if let Some(conn) = self.take_connection() {
            return Dispatch(Some(DispatchState::Ready(conn)));
        }
        // Add a waiter to the pool. When the Manager is able to obtain
        let (waiter, pending) = oneshot::channel();
        if let Err(sender) = self.pool.add_waiter(waiter) {
            // XXX what should we actually do here?
            Dispatch(Some(DispatchState::Failed(io::ErrorKind::Other.into())))
        } else {
            Dispatch(Some(DispatchState::Pending(pending)))
        }
    }

    // XXX This isn't proper (weighted) load balancing.
    // 1. Select 2 endpoints at random.
    // 2. Score both endpoints.
    // 3. Take winner.
    fn take_connection(&self) -> Option<DstConnection> {
        let mut active = self.pool.active.borrow_mut();
        match active.len() {
            0 => {
                trace!("no endpoints ready");
                None
            }
            1 => {
                // One endpoint, use it.
                let (_, mut ep) = active.get_index_mut(0).unwrap();
                ep.connected
                    .pop_front()
                    .map(|conn| {
                             //self.established_connections -= 1;
                             conn
                         })
            }
            sz => {
                // Pick 2 candidate indices.
                let (i0, i1) = if sz == 2 {
                    // There are only two endpoints, so no need for an RNG.
                    (0, 1)
                } else {
                    // 3 or more endpoints: choose two distinct endpoints at random.
                    let mut rng = rand::thread_rng();
                    let i0 = rng.gen_range(0, sz);
                    let mut i1 = rng.gen_range(0, sz);
                    while i0 == i1 {
                        i1 = rng.gen_range(0, sz);
                    }
                    (i0, i1)
                };
                let addr = {
                    // Determine the index of the lesser-loaded endpoint
                    let (addr0, ep0) = active.get_index(i0).unwrap();
                    let (addr1, ep1) = active.get_index(i1).unwrap();
                    if ep0.load <= ep1.load {
                        trace!("dst: {} *{} (not {} *{})",
                               addr0,
                               ep0.weight,
                               addr1,
                               ep1.weight);

                        *addr0
                    } else {
                        trace!("dst: {} *{} (not {} *{})",
                               addr1,
                               ep1.weight,
                               addr0,
                               ep0.weight);
                        *addr1
                    }
                };

                {
                    let mut ep = active.get_mut(&addr).unwrap();
                    ep.connected.pop_front()
                }
            }
        }

        // for ep in self.active.values_mut() {
        //     if let Some(conn) = ep.connected.pop_front() {
        //         self.established_connections -= 1;
        //         return Some(conn);
        //     }
        // }
        // None
    }
}

pub struct Dispatch(Option<DispatchState>);
enum DispatchState {
    Pending(oneshot::Receiver<DstConnection>),
    Ready(DstConnection),
    Failed(io::Error),
}

impl Future for Dispatch {
    type Item = DstConnection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let state = self.0
            .take()
            .expect("connect must not be polled after completion");
        match state {
            DispatchState::Failed(e) => Err(e),
            DispatchState::Ready(conn) => Ok(conn.into()),
            DispatchState::Pending(mut recv) => {
                match recv.poll() {
                    Err(_) => Err(io::Error::new(io::ErrorKind::Interrupted, "canceled")),
                    Ok(Async::Ready(conn)) => Ok(conn.into()),
                    Ok(Async::NotReady) => {
                        self.0 = Some(DispatchState::Pending(recv));
                        Ok(Async::NotReady)
                    }
                }
            }
        }
    }
}
