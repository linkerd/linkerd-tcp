use super::{DstAddr, Path};
use futures::{Future, Sink, Stream, Poll};
use futures::sync::mpsc;
use tokio_core::reactor::Remote;

mod namerd;
use self::namerd::Namerd;

#[derive(Debug)]
pub enum Error {
    Hyper(::hyper::Error),
    UnexpectedStatus(::hyper::StatusCode),
    Serde(::serde_json::Error),
    NotBound,
}

pub type Result<T> = ::std::result::Result<T, Error>;

// TODO In the future, we likely want to change this to use the split bind & addr APIs so
// balancers can be shared across logical names. In the meantime, it's sufficient to have
// a balancer per logical name.
#[derive(Clone)]
pub struct Resolver {
    reactor: Remote,
    namerd: Namerd,
}

impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        let addrs = {
            let namerd = self.namerd.clone();
            let path = path.clone();
            let (tx, rx) = mpsc::unbounded();
            let tx = tx.sink_map_err(|_| {});
            self.reactor
                .spawn(move |handle| {
                           let addrs = namerd.resolve(handle, path.as_str());
                           addrs.map_err(|_| {}).forward(tx).map(|_| {})
                       });
            rx
        };
        Resolve(addrs)
    }
}

// A stream of name resolutions.
pub struct Resolve(mpsc::UnboundedReceiver<Result<Vec<DstAddr>>>);
impl Stream for Resolve {
    type Item = Result<Vec<DstAddr>>;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}
