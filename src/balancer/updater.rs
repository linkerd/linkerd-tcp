use super::{Endpoints, WeightedAddr};
use super::super::Path;
use futures::{Sink, Poll, Async, AsyncSink, StartSend};
use std::cell::RefCell;
use std::rc::Rc;
use tacho;

pub fn new(dst_name: Path,
           endpoints: Rc<RefCell<Endpoints>>,
           available: tacho::Gauge,
           retired: tacho::Gauge)
           -> Updater {
    Updater {
        dst_name,
        endpoints,
        available,
        retired,
    }
}

pub struct Updater {
    dst_name: Path,
    endpoints: Rc<RefCell<Endpoints>>,
    available: tacho::Gauge,
    retired: tacho::Gauge,
}

impl Sink for Updater {
    type SinkItem = Vec<WeightedAddr>;
    type SinkError = ();

    fn start_send(&mut self, addrs: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let mut endpoints = self.endpoints.borrow_mut();
        endpoints.update_resolved(&self.dst_name, &addrs);
        self.available.set(endpoints.available().len());
        self.retired.set(endpoints.available().len());
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}
