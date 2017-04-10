

use WeightedAddr;
use futures::{StartSend, AsyncSink, Async, Poll, Sink, Stream};
use lb::{Connector, Endpoint, Shared, Src, WithAddr};
use rand::{self, Rng};
use std::{f32, io};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;
use tacho::{self, Timing};
use tokio_core::reactor::Handle;

/// Distributes TCP connections across a pool of dsts.
///
/// May only be accessed from a single thread.  Use `Balancer::into_shared` for a
/// cloneable/shareable variant.
///
/// ## Panics
///
/// Panics if the `Stream` of address of resolutions ends. It must never complete.
///
pub struct Balancer<A, C> {
    // Streams address updates (i.e. from service discovery).
    addrs: A,

    // Initiates dst connections.
    connector: C,

    // Holds transfer data between socks (because we don't yet employ a 0-copy strategy).
    // This buffer is used for _all_ transfers in this balancer.
    buffer: Rc<RefCell<Vec<u8>>>,

    // Endpoints that are in service discovery or otherwise active, but without
    // established connections..
    unready: VecDeque<Endpoint>,

    // Endpoints that have established connections ready for dispatch.
    ready: VecDeque<Endpoint>,

    // We thank these endpoints for their service, but they have been deregistered and
    // should initiate new connections.
    retired: VecDeque<Endpoint>,

    stats: Stats,

    fail_fast_mode: bool,
}

impl<A, C> Balancer<A, C>
    where A: Stream<Item = Vec<WeightedAddr>, Error = io::Error>,
          C: Connector + 'static
{
    /// Creates a new balancer with the given address stream
    pub fn new(addrs: A,
               connector: C,
               buf: Rc<RefCell<Vec<u8>>>,
               metrics: tacho::Metrics)
               -> Balancer<A, C> {
        Balancer {
            addrs: addrs,
            connector: connector,
            buffer: buf,
            unready: VecDeque::new(),
            ready: VecDeque::new(),
            retired: VecDeque::new(),
            stats: Stats::new(metrics),
            fail_fast_mode: false,
        }
    }

    /// Moves this balancer into one that may be shared across threads.
    ///
    /// The Balancer's handle is used to drive all balancer changes on a single thread,
    /// while other threads may submit `Srcs` to be processed.
    pub fn into_shared(self, max_waiters: usize, h: Handle) -> Shared
        where A: 'static
    {
        Shared::new(self, max_waiters, h)
    }

    /// Drop retired endpoints that have no pending connections.
    fn evict_retirees(&mut self, rec: &mut tacho::Recorder) -> io::Result<()> {
        let sz = self.retired.len();
        trace!("checking {} retirees", sz);
        for _ in 0..sz {
            let mut ep = self.retired.pop_front().unwrap();
            ep.poll_connections(rec)?;
            if ep.is_active() {
                trace!("still active {}", ep.addr());
                self.retired.push_back(ep);
            } else {
                trace!("evicting {}", ep.addr());
                drop(ep);
            }
        }
        Ok(())
    }

    fn poll_ready(&mut self, rec: &mut tacho::Recorder) -> io::Result<()> {
        let sz = self.ready.len();
        trace!("checking {} ready", sz);
        for _ in 0..sz {
            let mut ep = self.ready.pop_front().unwrap();
            ep.poll_connections(rec)?;
            if ep.is_ready() {
                trace!("ready {}", ep.addr());
                self.ready.push_back(ep);
            } else {
                trace!("not ready {}", ep.addr());
                self.unready.push_back(ep);
            }
        }
        Ok(())
    }

    fn promote_unready(&mut self, rec: &mut tacho::Recorder) -> io::Result<()> {
        let sz = self.unready.len();
        trace!("checking {} unready", sz);
        for _ in 0..sz {
            let mut ep = self.unready.pop_front().unwrap();
            ep.poll_connections(rec)?;
            if ep.is_ready() {
                trace!("ready {}", ep.addr());
                self.ready.push_back(ep);
            } else {
                trace!("not ready {}", ep.addr());
                self.unready.push_back(ep);
            }
        }
        Ok(())
    }

    /// Checks if the addrs has updated.  If it has, update `endpoints` new addresses and
    /// weights.
    ///
    /// ## Panics
    ///
    /// If the addrs stream ends.
    fn discover_and_retire(&mut self) -> io::Result<()> {
        trace!("polling addr");
        if let Async::Ready(addrs) = self.addrs.poll()? {
            trace!("addr update");
            let addrs = addrs.expect("addr stream must be infinite");
            // If there are no addrs to route to, drop requests quickly.
            // TODO: validate that fail_fast_mode is being disabled once addrs exist.
            self.fail_fast_mode = addrs.is_empty();
            let new = addr_weight_map(&addrs);
            self.update_endpoints(&new);
        }
        Ok(())
    }

    /// Updates the endpoints with an address resolution update.
    fn update_endpoints(&mut self, new_addrs: &HashMap<SocketAddr, f32>) {
        let mut ep_addrs = HashSet::new();

        trace!("updating {} unready", self.unready.len());
        for _ in 0..self.unready.len() {
            let mut ep = self.unready.pop_front().unwrap();
            let addr = ep.addr();
            match new_addrs.get(&addr) {
                None => {
                    trace!("retiring {}", addr);
                    ep.retire();
                    if ep.is_active() {
                        self.retired.push_back(ep)
                    } else {
                        trace!("evicting {}", addr);
                        drop(ep);
                    }
                }
                Some(&w) => {
                    trace!("updating {} *{}", addr, w);
                    ep.set_weight(w);
                    self.unready.push_back(ep);
                    ep_addrs.insert(addr);
                }
            }
        }

        trace!("updating {} ready", self.ready.len());
        for _ in 0..self.ready.len() {
            let mut ep = self.ready.pop_front().unwrap();
            let addr = ep.addr();
            match new_addrs.get(&addr) {
                None => {
                    if ep.is_active() {
                        trace!("retiring {}", addr);
                        ep.retire();
                        if ep.is_active() {
                            self.retired.push_back(ep);
                        } else {
                            trace!("evicting {}", addr);
                            drop(ep);
                        }
                    } else {
                        trace!("evicting {}", addr);
                        drop(ep);
                    }
                }
                Some(&w) => {
                    trace!("updating {} *{}", addr, w);
                    ep.set_weight(w);
                    self.ready.push_back(ep);
                    ep_addrs.insert(addr);
                }
            }
        }

        // Check to see if we have re-added anything that has previously been marked as
        // retired.
        trace!("updating {} retired", self.retired.len());
        for _ in 0..self.retired.len() {
            let mut ep = self.retired.pop_front().unwrap();
            let addr = ep.addr();
            match new_addrs.get(&addr) {
                None => {
                    self.retired.push_back(ep);
                }
                Some(&w) => {
                    trace!("reviving {}", addr);
                    ep.unretire();
                    ep.set_weight(w);
                    self.ready.push_back(ep);
                    ep_addrs.insert(addr);
                }
            }
        }

        for (addr, weight) in new_addrs {
            if !ep_addrs.contains(addr) {
                trace!("adding {} *{}", addr, weight);
                let metrics = self.stats.metrics.clone();
                self.connect(Endpoint::new(*addr, *weight, metrics));
            }
        }
    }

    /// Dispatches an `Src` to a dst `Endpoint`, if possible.
    ///
    /// Chooses two endpoints at random and uses the lesser-loaded of the two.
    // TODO pluggable strategy
    fn dispatch(&mut self, src: Src, rec: &mut tacho::Recorder) -> StartSend<Src, io::Error> {
        trace!("dispatching {}", src.addr());
        // Choose an endpoint.
        match self.ready.len() {
            0 => {
                trace!("no endpoints ready");
                Ok(AsyncSink::NotReady(src))
            }
            1 => {
                // One endpoint, use it.
                let mut ep = self.ready.pop_front().unwrap();
                let tx = ep.transmit(src, self.buffer.clone(), rec);
                // Replace the connection preemptively.
                self.connect(ep);
                Ok(tx)
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
                // Determine the index of the lesser-loaded endpoint
                let idx = {
                    let ep0 = &self.ready[i0];
                    let ep1 = &self.ready[i1];
                    if ep0.load() <= ep1.load() {
                        trace!("dst: {} *{} (not {} *{})",
                               ep0.addr(),
                               ep0.weight(),
                               ep1.addr(),
                               ep1.weight());
                        i0
                    } else {
                        trace!("dst: {} *{} (not {} *{})",
                               ep1.addr(),
                               ep1.weight(),
                               ep1.addr(),
                               ep0.weight());
                        i1
                    }
                };

                let tx = {
                    // Once we know the index of the endpoint we want to use, obtain a mutable
                    // reference to begin proxying.
                    let mut ep = self.ready.swap_remove_front(idx).unwrap();
                    let tx = ep.transmit(src, self.buffer.clone(), rec);
                    // Replace the connection preemptively.
                    self.connect(ep);
                    tx
                };

                Ok(tx)
            }
        }
    }

    fn connect(&mut self, mut ep: Endpoint) {
        ep.init_connection(&self.connector);
        if ep.conns_established() > 0 {
            self.ready.push_back(ep);
        } else {
            self.unready.push_back(ep);
        }
    }

    fn record_balanacer_stats(&mut self, rec: &mut tacho::Recorder) {
        self.stats.measure(rec, &self.unready, &self.ready, &self.retired);
    }
}

fn addr_weight_map(new: &[WeightedAddr]) -> HashMap<SocketAddr, f32> {
    let mut s = HashMap::new();
    for wa in new {
        s.insert(wa.0, wa.1);
    }
    s
}

/// Receives `Src` sockets to be dismatched to an underlying endpoint.
///
/// `start_send` returns `Async::Ready` if there is a dst endpoint available, and
/// `Async::NotReady` otherwise.
///
/// `poll_complete` always returns `Async::NotReady`, since the load balancer may always
/// receive more srcs.
impl<A, C> Sink for Balancer<A, C>
    where A: Stream<Item = Vec<WeightedAddr>, Error = io::Error>,
          C: Connector + 'static
{
    type SinkItem = Src;
    type SinkError = io::Error;

    /// Updates the list of endpoints before attempting to dispatch `src` to a
    /// dst endpoint.
    fn start_send(&mut self, src: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let src_addr = src.addr();
        let mut rec = self.stats.recorder();
        let poll_t = tacho::Timing::start();
        trace!("start_send {}: unready={} ready={} retired={}",
               src_addr,
               self.unready.len(),
               self.ready.len(),
               self.retired.len());
        let ret = match self.dispatch(src, &mut rec) {
            Err(e) => Err(e),
            Ok(AsyncSink::Ready) => Ok(AsyncSink::Ready),
            Ok(AsyncSink::NotReady(src)) => {
                self.evict_retirees(&mut rec)?;
                self.promote_unready(&mut rec)?;
                self.discover_and_retire()?;
                trace!("retrying {} unready={} ready={} retired={} failfast={}",
                       src_addr,
                       self.unready.len(),
                       self.ready.len(),
                       self.retired.len(),
                       self.fail_fast_mode);
                if self.fail_fast_mode {
                    trace!("in fail fast mode, dropping traffic");
                    Err(io::ErrorKind::Other.into())
                } else {
                    self.dispatch(src, &mut rec)
                }
            }
        };

        self.record_balanacer_stats(&mut rec);
        trace!("start_sent {}: {} unready={} ready={} retired={}",
               src_addr,
               match &ret {
                   &Ok(AsyncSink::Ready) => "sent",
                   &Ok(AsyncSink::NotReady(_)) => "not sent",
                   &Err(_) => "failed",
               },
               self.unready.len(),
               self.ready.len(),
               self.retired.len());

        rec.add(&self.stats.poll_time_us, poll_t.elapsed_us());
        ret
    }

    /// Updates the list of endpoints as needed.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let mut rec = self.stats.recorder();
        let poll_t = tacho::Timing::start();
        trace!("poll_complete unready={} ready={} retired={}",
               self.unready.len(),
               self.ready.len(),
               self.retired.len());
        self.evict_retirees(&mut rec)?;
        self.poll_ready(&mut rec)?;
        self.promote_unready(&mut rec)?;
        self.discover_and_retire()?;
        self.record_balanacer_stats(&mut rec);
        trace!("poll_completed unready={} ready={} retired={}",
               self.unready.len(),
               self.ready.len(),
               self.retired.len());
        rec.add(&self.stats.poll_time_us, poll_t.elapsed_us());
        Ok(Async::NotReady)
    }
}

struct Stats {
    metrics: tacho::Metrics,
    conns_established: tacho::GaugeKey,
    conns_active: tacho::GaugeKey,
    conns_pending: tacho::GaugeKey,
    endpoints_ready: tacho::GaugeKey,
    endpoints_unready: tacho::GaugeKey,
    endpoints_retired: tacho::GaugeKey,
    poll_time_us: tacho::StatKey,
}

impl Stats {
    fn new(m: tacho::Metrics) -> Stats {
        Stats {
            conns_established: m.scope().gauge("conns_established".into()),
            conns_active: m.scope().gauge("conns_active".into()),
            conns_pending: m.scope().gauge("conns_pending".into()),
            endpoints_ready: m.scope().gauge("endpoints_ready".into()),
            endpoints_unready: m.scope().gauge("endpoints_unready".into()),
            endpoints_retired: m.scope().gauge("endpoints_retired".into()),
            poll_time_us: m.scope().stat("poll_time_us".into()),
            metrics: m,
        }
    }

    fn recorder(&self) -> tacho::Recorder {
        self.metrics.recorder()
    }

    fn measure(&self,
               rec: &mut tacho::Recorder,
               unready: &VecDeque<Endpoint>,
               ready: &VecDeque<Endpoint>,
               retired: &VecDeque<Endpoint>) {
        let mut established = 0u64;
        let mut active = 0u64;
        let mut pending = 0u64;
        for e in unready {
            established += e.conns_established() as u64;
            active += e.conns_active() as u64;
            pending += e.conns_pending() as u64;
        }
        for e in ready {
            established += e.conns_established() as u64;
            active += e.conns_active() as u64;
            pending += e.conns_pending() as u64;
        }
        for e in retired {
            active += e.conns_active() as u64;
        }

        rec.set(&self.conns_established, established);
        rec.set(&self.conns_active, active);
        rec.set(&self.conns_pending, pending);
        rec.set(&self.endpoints_ready, ready.len() as u64);
        rec.set(&self.endpoints_unready, unready.len() as u64);
        rec.set(&self.endpoints_retired, retired.len() as u64);
    }
}
