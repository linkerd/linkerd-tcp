use super::{Result, Error, WeightedAddr};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;
use futures::Stream;
use std::collections::HashMap;
use futures::stream;
use futures::Poll;

pub mod namerd;

use self::namerd::{Addrs, WithClient, Namerd};

pub enum StreamEither<A, B> {
    A(A),
    B(B),
}

impl<A, B> Stream for StreamEither<A, B>
    where A: Stream,
          B: Stream<Item = A::Item, Error = A::Error>
{
    type Item = A::Item;
    type Error = A::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match *self {
            StreamEither::A(ref mut a) => a.poll(),
            StreamEither::B(ref mut b) => b.poll(),
        }
    }
}

pub enum Namer {
    Constant(HashMap<String, Vec<WeightedAddr>>),
    Namerd(Namerd)
}

impl Namer {
    pub fn with_handle(self, handle: &Handle, timer: &Timer) -> WithHandle {
        match self {
            Namer::Constant(addrs) => WithHandle::Constant(addrs),
            Namer::Namerd(namerd) => WithHandle::WithClient(namerd.with_handle(handle, timer)),
        }
    }
}

pub enum WithHandle {
    Constant(HashMap<String, Vec<WeightedAddr>>),
    WithClient(WithClient),
}

impl WithHandle {
    pub fn resolve(&self, target: &str) -> StreamEither<stream::Once<Result<Vec<WeightedAddr>>, Error>, Addrs> {
        match self {
            &WithHandle::Constant(ref addrs) => match addrs.get(target) {
                Some(addrs) => StreamEither::A(stream::once(Ok(Ok(addrs.clone())))),
                None => StreamEither::A(stream::once(Ok(Err(Error::NotBound)))),
            },
            &WithHandle::WithClient(ref with_client) =>
              StreamEither::B(with_client.resolve(target))
        }
    }
}