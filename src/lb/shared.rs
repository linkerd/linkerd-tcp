use futures::{StartSend, Poll, Sink, Stream};
use futures::sync::mpsc;
use std::io;
use tokio_core::reactor::Handle;

use {Driver, WeightedAddr};
use lb::{Balancer, Connector, Src, WithAddr};

/// Allows a balancer to be shared acorss threads.
pub struct Shared(mpsc::Sender<Src>);

impl Shared {
    /// Spawn the `balancer` in the given `handle`.
    pub fn new<A, C>(balancer: Balancer<A, C>, max_waiters: usize, handle: Handle) -> Shared
        where A: Stream<Item = Vec<WeightedAddr>, Error = io::Error> + 'static,
              C: Connector + 'static
    {
        let (tx, rx) = mpsc::channel(max_waiters);
        let driver = Driver::new(rx.fuse(), balancer.sink_map_err(|_| {}));
        handle.spawn(driver);
        Shared(tx)
    }
}

impl Clone for Shared {
    fn clone(&self) -> Self {
        Shared(self.0.clone())
    }
}

impl Sink for Shared {
    type SinkItem = Src;
    type SinkError = io::Error;

    fn start_send(&mut self, src: Src) -> StartSend<Src, Self::SinkError> {
        debug!("start_send {}", src.addr());
        self.0.start_send(src).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        debug!("poll_complete");
        // This doesn't actually do anything, since balancers never complete.
        self.0.poll_complete().map_err(|_| unreachable!())
    }
}
