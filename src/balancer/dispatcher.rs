use super::{EndpointMap, Waiter};
use super::endpoint::{self, Endpoint};
use super::super::Path;
use super::super::connection::Connection;
use super::super::connector::Connector;
use futures::{Future, Stream, Poll, Async};
use rand::{self, Rng};
use std::collections::VecDeque;
use std::io;
use tacho;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

type ConnectingQ = VecDeque<tacho::Timed<endpoint::Connecting>>;
type ConnectedQ = VecDeque<Connection<endpoint::Ctx>>;
type WaiterQ = VecDeque<Waiter>;

#[derive(Debug, PartialEq, Eq)]
pub enum State {
    Done,
    NotReady,
    NeedsPoll,
}

pub struct Dispatcher<W> {
    dst_name: Path,

    connector: Connector,
    reactor: Handle,
    timer: Timer,

    /// Provides new connection requests as a Stream.
    rx: Option<Rx<W>>,

    /// A queue of pending connections.
    connecting: ConnectingQ,

    /// A queue of ready connections to be dispatched ot waiters.
    connected: ConnectedQ,

    /// A queue of waiters that have not yet received a connection.
    waiters: WaiterQ,

    metrics: Metrics,
}

impl<W> Dispatcher<W>
    where W: Stream<Item = Waiter>
{
    pub fn new(reactor: Handle,
               timer: Timer,
               dst_name: Path,
               recv: W,
               connector: Connector,
               metrics: &tacho::Scope)
               -> Dispatcher<W> {
        let rx = Rx {
            recv,
            is_ready: true,
        };
        Dispatcher {
            reactor,
            timer,
            dst_name,
            connector,
            rx: Some(rx),
            connecting: ConnectingQ::default(),
            connected: ConnectedQ::default(),
            waiters: WaiterQ::default(),
            metrics: Metrics::new(metrics),
        }
    }

    fn is_done(&self) -> bool {
        self.rx.is_none() && self.waiters.is_empty()
    }

    fn is_not_ready(&self) -> bool {
        self.rx.as_ref().map(|rx| !rx.is_ready).unwrap_or(false) || !self.connecting.is_empty()
    }

    fn state(&self) -> State {
        if self.is_done() {
            State::Done
        } else if self.is_not_ready() {
            State::NotReady
        } else {
            State::NeedsPoll
        }
    }

    pub fn poll(&mut self) -> State {
        // Poll all pending connections. Newly established connections are added to the
        // `connected` queue, to be dispatched.
        self.poll_connecting();

        // Now that we may have new established connnections, dispatch them to waiters.
        self.dispatch_connected_to_waiters();

        // Having dispatched, we're ready to refill the waiters queue from the channel. No
        // more than `max_waiters` items are retained at once.
        //
        // We may end up in a situation where we haven't received `Async::NotReady` from
        // `waiters_rx.poll()`. We rely on the fact that connection events will be
        // necessary to satisfy existing waiters, and another `recv_waiters()` call will
        // be triggered from those events.
        self.recv_waiters();
        self.metrics.waiters.set(self.waiters.len());

        self.state()
    }

    pub fn init(&mut self, available: &EndpointMap) -> State {
        self.init_connecting(available);
        self.dispatch_connected_to_waiters();
        self.recv_waiters();
        self.metrics.waiters.set(self.waiters.len());
        self.state()
    }

    fn init_connecting(&mut self, available: &EndpointMap) {
        let needed = {
            let needed = self.connector.min_connections() + self.waiters.len();
            let pending = self.connecting.len() + self.connected.len();
            if needed < pending {
                0
            } else {
                needed - pending
            }
        };
        debug!("initiating {} connections", needed);

        if available.is_empty() {
            trace!("no available endpoints");
            return;
        }

        let mut rng = rand::thread_rng();
        for _ in 0..needed {
            match select_endpoint(&mut rng, available) {
                None => {
                    trace!("no endpoints ready");
                    self.metrics.unavailable.incr(1);
                    return;
                }
                Some(ep) => {
                    self.metrics.attempts.incr(1);
                    let mut conn = {
                        let sock = self.connector
                            .connect(&ep.peer_addr(), &self.reactor, &self.timer);
                        let c = ep.connect(sock, &self.metrics.connection_duration);
                        self.metrics.connect_latency.time(c)
                    };
                    match conn.poll() {
                        Err(e) => {
                            debug!("connection failed: {}", e);
                            self.metrics.failure(&e);
                        }
                        Ok(Async::NotReady) => {
                            trace!("connection pending");
                            self.metrics.pending.incr(1);
                            self.connecting.push_back(conn);
                        }
                        Ok(Async::Ready(conn)) => {
                            debug!("connected");
                            self.metrics.connects.incr(1);
                            self.metrics.pending.decr(1);
                            self.metrics.open.incr(1);
                            self.connected.push_back(conn);
                        }
                    }
                }
            }
        }
    }

    /// Receives and attempts to dispatch new waiters.
    ///
    /// If there are no available connections to be dispatched, up to `max_waiters` are
    /// buffered.
    fn recv_waiters(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            while self.waiters.len() < self.connector.max_waiters() {
                match rx.poll() {
                    Err(_) => {
                        error!("{}: error from waiters channel", self.dst_name);
                    }
                    Ok(Async::Ready(Some(w))) => {
                        match self.connected.pop_front() {
                            None => self.waiters.push_back(w),
                            Some(conn) => {
                                if let Err(conn) = w.send(conn) {
                                    self.connected.push_front(conn);
                                }
                            }
                        }
                    }
                    Ok(Async::Ready(None)) => {
                        return;
                    }
                    Ok(Async::NotReady) => {
                        self.rx = Some(rx);
                        return;
                    }
                }
            }
            self.rx = Some(rx);
        }
    }

    fn poll_connecting(&mut self) {
        debug!("polling {} pending connections", self.connecting.len());
        for _ in 0..self.connecting.len() {
            let mut connecting = self.connecting.pop_front().unwrap();
            match connecting.poll() {
                Err(e) => {
                    debug!("connection failed: {}", e);
                    self.metrics.pending.decr(1);
                    self.metrics.failure(&e);
                }
                Ok(Async::NotReady) => {
                    trace!("connection pending");
                    self.connecting.push_back(connecting);
                }
                Ok(Async::Ready(connected)) => {
                    debug!("connected");
                    self.metrics.connects.incr(1);
                    self.metrics.pending.decr(1);
                    self.metrics.open.incr(1);
                    self.connected.push_back(connected);
                }
            }
        }
    }

    fn dispatch_connected_to_waiters(&mut self) {
        debug!("dispatching {} connections to {} waiters",
               self.connected.len(),
               self.waiters.len());
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
}

struct Rx<W> {
    recv: W,
    is_ready: bool,
}
impl<W> Stream for Rx<W>
    where W: Stream<Item = Waiter>
{
    type Item = W::Item;
    type Error = W::Error;
    fn poll(&mut self) -> Poll<Option<W::Item>, W::Error> {
        let poll = self.recv.poll();
        self.is_ready = poll.as_ref().map(|p| p.is_ready()).unwrap_or(true);
        poll
    }
}

/// Selects an endpoint using the power of two choices.
///
/// Two endpoints are chosen randomly and return the lesser-loaded endpoint.
/// If no endpoints are available, `None` is retruned.
fn select_endpoint<'r, 'e, R: Rng>(rng: &'r mut R,
                                   available: &'e EndpointMap)
                                   -> Option<&'e Endpoint> {
    match available.len() {
        0 => None,
        1 => {
            // One endpoint, use it.
            available.get_index(0).map(|(_, ep)| ep)
        }
        sz => {
            // Pick 2 candidate indices.
            let (i0, i1) = if sz == 2 {
                if rng.gen::<bool>() { (0, 1) } else { (1, 0) }
            } else {
                // 3 or more endpoints: choose two distinct endpoints at random.
                let i0 = rng.gen_range(0, sz);
                let mut i1 = rng.gen_range(0, sz);
                while i0 == i1 {
                    i1 = rng.gen_range(0, sz);
                }
                (i0, i1)
            };

            // Determine the the scores of each endpoint
            let (addr0, ep0) = available.get_index(i0).unwrap();
            let (load0, weight0) = (ep0.load(), ep0.weight());
            let score0 = (load0 + 1) as f64 * (1.0 - weight0);

            let (addr1, ep1) = available.get_index(i1).unwrap();
            let (load1, weight1) = (ep1.load(), ep1.weight());
            let score1 = (load1 + 1) as f64 * (1.0 - weight1);

            if score0 <= score1 {
                trace!("dst: {} {}*{} (not {} {}*{})",
                       addr0,
                       load0,
                       weight0,
                       addr1,
                       load1,
                       weight1);
                Some(ep0)
            } else {
                trace!("dst: {} {}*{} (not {} {}*{})",
                       addr1,
                       load1,
                       weight1,
                       addr0,
                       load0,
                       weight0);
                Some(ep1)
            }
        }
    }
}

struct Metrics {
    pending: tacho::Gauge,
    open: tacho::Gauge,
    waiters: tacho::Gauge,
    attempts: tacho::Counter,
    unavailable: tacho::Counter,
    connects: tacho::Counter,
    timeouts: tacho::Counter,
    refused: tacho::Counter,
    failures: tacho::Counter,
    connect_latency: tacho::Timer,
    connection_duration: tacho::Timer,
}

impl Metrics {
    fn new(base: &tacho::Scope) -> Metrics {
        let conn = base.clone().prefixed("connection");
        Metrics {
            pending: conn.gauge("pending"),
            open: conn.gauge("open"),
            waiters: base.gauge("waiters"),
            unavailable: base.counter("unavailable"),
            attempts: conn.counter("attempts"),
            connects: conn.counter("connects"),
            timeouts: conn.clone().labeled("cause", "timeout").counter("failure"),
            refused: conn.clone().labeled("cause", "refused").counter("failure"),
            failures: conn.clone().labeled("cause", "other").counter("failure"),
            connect_latency: conn.timer_us("latency_us"),
            connection_duration: conn.timer_ms("duration_ms"),
        }
    }

    fn failure(&self, err: &io::Error) {
        match err.kind() {
            io::ErrorKind::TimedOut => self.timeouts.incr(1),
            io::ErrorKind::ConnectionRefused => self.refused.incr(1),
            _ => self.failures.incr(1),
        }
    }
}

// use futures::{Sink, AsyncSink, StartSend};
// struct DispatcherSink {}
// impl Sink for DispatcherSink {
//     type SinkItem = Waiter;
//     type SinkError = ();

//     fn start_send(&mut self, waiter: Waiter) -> StartSend<Waiter, ()> {

//     }
// }
