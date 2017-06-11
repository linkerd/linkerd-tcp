use super::{DstAddr, DstCtx};
use super::endpoint::Endpoint;
use super::manager::Endpoints;
use super::selector::DstConnectionRequest;
use super::super::Path;
use super::super::connection::{Connection, Socket};
use super::super::connector::{Connector, Connecting};
use super::super::resolver::{self, Resolve};
use futures::{Future, Stream, Sink, Poll, Async, AsyncSink, StartSend, unsync};
use ordermap::OrderMap;
use rand::{self, Rng};
use std::{cmp, io, net};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::vec_deque::Drain;
use std::rc::Rc;
use tacho::{self, Timing};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(dst_name: Path,
           reactor: Handle,
           timer: Timer,
           connector: Connector,
           endpoints: Rc<RefCell<Endpoints>>,
           max_waiters: usize,
           metrics: &tacho::Scope)
           -> Dispatcher {
    let connect_latency = metrics.timer_us("connect_latency_us");
    Dispatcher {
        dst_name,
        reactor,
        timer,
        connector,
        endpoints,
        max_waiters,
        connecting: VecDeque::with_capacity(max_waiters),
        connected: VecDeque::with_capacity(max_waiters),
        waiters: VecDeque::with_capacity(max_waiters),
        connect_latency,
    }
}

/// Accepts connection requests
pub struct Dispatcher {
    dst_name: Path,
    reactor: Handle,
    timer: Timer,
    connector: Connector,
    endpoints: Rc<RefCell<Endpoints>>,
    connect_latency: tacho::Timer,
    connecting: VecDeque<tacho::Timed<Connecting>>,
    connected: VecDeque<Connection<DstCtx>>,
    waiters: VecDeque<DstConnectionRequest>,
    max_waiters: usize,
}

impl Dispatcher {
    /// Selects an endpoint using the power of two choices.
    ///
    /// We select 2 endpoints randomly, compare their weighted loads
    fn select_endpoint(&mut self) -> Option<&Endpoint> {
        let endpoints = self.endpoints.borrow();
        let available = endpoints.available();
        match available.len() {
            0 => {
                trace!("no endpoints ready");
                None
            }
            1 => {
                // One endpoint, use it.
                available.get_index(0).map(|(_, ep)| ep)
            }
            sz => {
                let mut rng = rand::thread_rng();

                // Pick 2 candidate indices.
                let (i0, i1) = if sz == 2 {
                    // There are only two endpoints, so no need for an RNG.
                    (0, 1)
                } else {
                    // 3 or more endpoints: choose two distinct endpoints at random.
                    let i0 = rng.gen_range(0, sz);
                    let mut i1 = rng.gen_range(0, sz);
                    while i0 == i1 {
                        i1 = rng.gen_range(0, sz);
                    }
                    (i0, i1)
                };

                // Determine the index of the lesser-loaded endpoint
                let (addr0, ep0) = available.get_index(i0).unwrap();
                let (addr1, ep1) = available.get_index(i1).unwrap();
                let (load0, load1) = (ep0.weighted_load(), ep1.weighted_load());

                let use0 = if load0 == load1 {
                    rng.gen::<bool>()
                } else {
                    load0 < load1
                };

                if use0 {
                    trace!("dst: {} {} (not {} {})", addr0, load0, addr1, load1);
                    Some(ep0)
                } else {
                    trace!("dst: {} {} (not {} {})", addr1, load1, addr0, load0);
                    Some(ep1)
                }
            }
        }
    }

    fn connect(&self, ep: &Endpoint) -> io::Result<()> {
        let conn = self.connector
            .connect(&ep.peer_addr(), &self.reactor, &self.timer);
        let mut conn = self.connect_latency.time(conn);
        match conn.poll()? {
            Async::NotReady => {
                self.connecting.push_back(conn);
            }
            Async::Ready(conn) => {
                let conn = Connection::new(self.dst_name, conn, ep.ctx());
                self.connected.push_back(conn);
            }
        }
        Ok(())
    }

    fn dispatch_waiters(&mut self) {
        while let Some(conn) = self.connected.pop_front() {
            match self.waiters.pop_front() {
                None => {
                    self.connected.push_front(conn);
                    return;
                }
                Some(waiter) => {
                    waiter.send(conn);
                }
            }
        }
    }
}

/// Buffers up to `max_waiters` concurrent connection requests, along with corresponding connectin attempts.
impl Sink for Dispatcher {
    type SinkItem = DstConnectionRequest;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.waiters.len() == self.max_waiters {
            return Ok(AsyncSink::NotReady(item));
        }

        match self.select_endpoint() {
            None => Ok(AsyncSink::NotReady(item)),
            Some(ep) => {
                self.waiters.push_back(item);
                self.connect(&ep);
                Ok(AsyncSink::Ready)
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}
