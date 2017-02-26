#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate tokio_core;

pub mod transfer;
pub use transfer::BufferedTransfer;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
