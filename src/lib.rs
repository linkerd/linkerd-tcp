#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate hyper;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate tokio_core;
extern crate url;

// use futures::Future;
use std::net::SocketAddr;
// use tokio_core::net::TcpStream;

mod transfer;
pub use transfer::BufferedTransfer;

pub mod namerd;

pub trait WithAddr {
    fn addr(&self) -> SocketAddr;
}

pub trait Endpointer {
    type Endpoint: WithAddr;
    fn endpoint(&self) -> Option<Self::Endpoint>;
}

// WIP

// pub trait Proxier {
//     fn proxy<F>(&up_addr: SocketAddr, up_stream: TcpStream) -> F
//         where F: Future,
//               F::Item = () {
//     }
// }

// pub trait ProxyServer {
//     fn serve<F>(addr: &SocketAddr) -> F
//         where F: Future<Item = ()>,
//               F: 'static;
// }
