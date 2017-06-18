use super::{channelq, EndpointMap, Waiter};
use super::endpoint::{self, Endpoint};
use super::super::Path;
use super::super::connection::Connection;
use super::super::connector::Connector;
use futures::{Future, Stream, Poll, Async, Sink, AsyncSink, sink};
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
    pending: Option<Waiter>,

    channelq: channelq::ChannelQ<Waiter>,

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
        let channelq = channelq::channel(connector.max_waiters());
        Dispatcher {
            reactor,
            timer,
            dst_name,
            connector,
            rx: Some(rx),
            pending: None,
            channelq,
            metrics: Metrics::new(metrics),
        }
    }

    // pub fn poll(&mut self) -> State {
    //     self.recv_waiters();
    //     //self.metrics.waiters.set(self.waiters.len());

    //     self.state()
    // }

    // pub fn init(&mut self, available: &EndpointMap) -> State {
    //     self.init_connecting(available);
    //     self.dispatch_connected_to_waiters();
    //     self.recv_waiters();
    //     // TODO self.metrics.waiters.set(self.waiters.len());
    //     self.state()
    // }

    fn init_connecting(&mut self, available: &EndpointMap, needed: usize) {
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
                        let waiter = self.channelq.recv();
                        let c = ep.connect(sock, &self.metrics.connection_duration);
                        self.metrics
                            .connect_latency
                            .time(c)
                            .map_err(|e| {
                                         error!("connection error: {}", e);
                                         ()
                                     })
                            .and_then(move |dst| {
                                waiter.then(|res| match res {
                                                Err(_) => Err(()),
                                                Ok(w) => {
                                                    w.send(dst);
                                                    Ok(())
                                                }
                                            })
                            })
                    };
                    self.reactor.spawn(conn);
                }
            }
        }
    }

    /// Receives and attempts to dispatch new waiters.
    ///
    /// If there are no available connections to be dispatched, up to `max_waiters` are
    /// buffered.
    pub fn start_recv(&mut self) -> Async<()> {
        if let Some(pending) = self.pending.take() {
            if let Ok(AsyncSink::NotReady(p)) = self.channelq.start_send(pending) {
                self.pending = Some(p);
                return Async::NotReady;
            }
        }

        match self.rx.take() {
            None => Async::Ready(()),
            Some(mut rx) => {
                loop {
                    match rx.poll() {
                        Err(_) => {}
                        Ok(Async::Ready(None)) => {
                            return Async::Ready(());
                        }
                        Ok(Async::Ready(Some(waiter))) => {
                            if let Ok(AsyncSink::NotReady(waiter)) =
                                self.channelq.start_send(waiter) {
                                self.pending = Some(waiter);
                                self.rx = Some(rx);
                                return Async::NotReady;
                            }
                        }
                        Ok(Async::NotReady) => {
                            self.rx = Some(rx);
                            return Async::NotReady;
                        }
                    }
                }
            }
        }
    }

    pub fn poll_complete(&mut self, ) 
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
