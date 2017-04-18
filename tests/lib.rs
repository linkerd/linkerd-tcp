#[cfg(tests)]
mod tests;
extern crate log;

extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate tokio_io;
extern crate linkerd_tcp;

mod mocks;
pub use mocks::MockNamerd;
