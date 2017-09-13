
use super::{Result, Error, WeightedAddr};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;
use futures::Stream;

pub trait Namer {
    fn with_handle(self: Box<Self>, handle: &Handle, timer: &Timer) -> Box<WithHandle>;
}

pub trait WithHandle {
    fn resolve(&self, target: &str) -> Box<Stream<Item = Result<Vec<WeightedAddr>>, Error = Error>>;
}
