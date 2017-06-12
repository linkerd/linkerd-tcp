use super::{Endpoints, EndpointMap, Waiter};
use super::endpoint::{self, Endpoint};
use super::super::connection::Connection;
use super::super::connector::Connector;
use futures::{Future, Sink, Poll, Async, AsyncSink, StartSend};
use rand::{self, Rng};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

pub fn new(reactor: Handle,
           timer: Timer,
           connector: Connector,
           endpoints: Rc<RefCell<Endpoints>>,
           min_connections: usize,
           max_waiters: usize,
           metrics: &tacho::Scope)
           -> Dispatcher {
    let connect_latency = metrics.timer_us("connect_latency_us");
    Dispatcher {
        reactor,
        timer,
        connector,
        endpoints,
        min_connections,
        max_waiters,
        connecting: VecDeque::with_capacity(max_waiters),
        connected: VecDeque::with_capacity(max_waiters),
        waiters: VecDeque::with_capacity(max_waiters),
        connect_latency,
    }
}

/// Accepts connection requests
pub struct Dispatcher {
    reactor: Handle,
    timer: Timer,
    connector: Connector,
    endpoints: Rc<RefCell<Endpoints>>,
    connect_latency: tacho::Timer,
    connecting: VecDeque<tacho::Timed<endpoint::Connecting>>,
    min_connections: usize,
    connected: VecDeque<Connection<endpoint::Ctx>>,
    waiters: VecDeque<Waiter>,
    max_waiters: usize,
}

impl Dispatcher {
    /// Selects an endpoint using the power of two choices.
    ///
    /// We select 2 endpoints randomly, compare their weighted loads
    fn select_endpoint(available: &EndpointMap) -> Option<&Endpoint> {
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

    fn dispatch_waiters(&mut self) {
        while let Some(conn) = self.connected.pop_front() {
            if let Err(conn) = self.dispatch_to_next_waiter(conn) {
                self.connected.push_front(conn);
                return;
            }
        }
    }

    fn dispatch_to_next_waiter(&mut self,
                               conn: endpoint::Connection)
                               -> Result<(), endpoint::Connection> {
        match self.waiters.pop_front() {
            None => Err(conn),
            Some(waiter) => {
                match waiter.send(conn) {
                    Ok(()) => Ok(()),
                    Err(conn) => self.dispatch_to_next_waiter(conn),
                }
            }
        }
    }

    fn connect(&mut self) -> io::Result<usize> {
        let endpoints = self.endpoints.borrow();
        let available = endpoints.available();
        let mut connected = 0;
        let needed = self.min_connections +
                     (self.waiters.len() - self.connecting.len() + self.connected.len());
        for _ in 0..needed {
            match Dispatcher::select_endpoint(available) {
                None => {
                    return Ok(connected);
                }
                Some(ep) => {
                    let sock = self.connector
                        .connect(&ep.peer_addr(), &self.reactor, &self.timer);
                    let mut conn = self.connect_latency.time(ep.connect(sock, &self.timer));
                    match conn.poll()? {
                        Async::NotReady => self.connecting.push_back(conn),
                        Async::Ready(conn) => self.connected.push_back(conn),
                    }
                    connected += 1;
                }
            }
        }
        Ok(connected)
    }
}

/// Buffers up to `max_waiters` concurrent connection requests, along with corresponding connection attempts.
impl Sink for Dispatcher {
    type SinkItem = Waiter;
    type SinkError = io::Error;

    fn start_send(&mut self, waiter: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.waiters.len() < self.max_waiters {
            let connecting = self.connect()?;
            self.dispatch_waiters();
            if connecting > 0 {
                return Ok(AsyncSink::Ready);
            }
        }

        Ok(AsyncSink::NotReady(waiter))
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.connecting.is_empty() {
            if self.waiters.is_empty() {
                return Ok(Async::Ready(()));
            }

            return Ok(Async::NotReady);
        }

        Ok(Async::NotReady)
    }
}

struct Metrics {
    // // A cache of aggregated metadata about retired endpoints.
    // retired_meta: EndpointsMeta,
    available: tacho::Gauge,
    failed: tacho::Gauge,
    retired: tacho::Gauge,
    connecting: tacho::Gauge,
    connected: tacho::Gauge,
    completing: tacho::Gauge,
    load: tacho::Gauge,
    waiters: tacho::Gauge,
    poll_us: tacho::Stat,
    attempts: tacho::Counter,
    connects: tacho::Counter,
    timeouts: tacho::Counter,
    failures: tacho::Counter,
    connect_latency: tacho::Timer,
    connection_duration: tacho::Timer,
}
