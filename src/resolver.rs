use super::{Path, namerd};
use futures::{Future, Sink, Stream, Poll};
use futures::sync::mpsc;
use std::{io, net};
use std::cell::RefCell;
use std::rc::Rc;
use tokio_core::reactor::Remote;

/// A weighted concrete destination address.
#[derive(Clone, Debug)]
pub struct DstAddr {
    pub addr: net::SocketAddr,
    pub weight: f32,
}
impl DstAddr {
    pub fn new(addr: net::SocketAddr, weight: f32) -> DstAddr {
        DstAddr {
            addr: addr,
            weight: weight,
        }
    }
}

// TODO In the future, we likely want to change this to use the split bind & addr APIs so
// that.
#[derive(Clone)]
pub struct Resolver {
    reactor: Remote,
    namerd: namerd::Namerd,
}
impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        let addrs = {
            let namerd = self.namerd.clone();
            let path = path.clone();
            let (tx, rx) = mpsc::unbounded();
            self.reactor
                .spawn(move |handle| {
                           namerd
                               .resolve(handle, path.as_str())
                               .map_err(|_| {})
                               .forward(tx.sink_map_err(|_| {}))
                               .map(|_| {})
                               .map_err(|_| {})
                       });
            rx
        };
        Resolve {
            path: path,
            state: Rc::new(RefCell::new(State::Pending(addrs))),
        }
    }
}

// A stream of name resolutions.
#[derive(Clone)]
pub struct Resolve {
    path: Path,
    state: Rc<RefCell<State>>,
}
enum State {
    Pending(mpsc::UnboundedReceiver<namerd::Result<Vec<DstAddr>>>),
    Streaming(Vec<DstAddr>, mpsc::UnboundedReceiver<namerd::Result<Vec<DstAddr>>>),
    Done,
}
impl Stream for Resolve {
    type Item = Vec<DstAddr>;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}
