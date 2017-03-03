use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::net::SocketAddr;
use tokio_core::net::TcpStream;

use lb::ProxyStream;

/// Joins upstream and downstream transfers into a single Future.
pub struct Duplex {
    pub down_addr: SocketAddr,
    pub up_addr: SocketAddr,
    down: Option<ProxyStream>,
    up: Option<ProxyStream>,
    down_size: u64,
    up_size: u64,
}

pub fn new(down_addr: SocketAddr,
           down_stream: TcpStream,
           up_addr: SocketAddr,
           up_stream: TcpStream,
           buf: Rc<RefCell<Vec<u8>>>)
           -> Duplex {
    let up = Rc::new(up_stream);
    let down = Rc::new(down_stream);
    Duplex {
        down_addr: down_addr,
        up_addr: up_addr,
        down: Some(ProxyStream::new(up.clone(), down.clone(), buf.clone())),
        up: Some(ProxyStream::new(down, up, buf)),
        down_size: 0,
        up_size: 0,
    }
}

impl Future for Duplex {
    type Item = (u64, u64);
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, io::Error> {
        if let Some(mut down) = self.down.take() {
            debug!("polling downward from {} to {}",
                   self.up_addr,
                   self.down_addr);
            if let Async::Ready(sz) = down.poll()? {
                debug!("downward complete from {} to {}",
                       self.up_addr,
                       self.down_addr);
                self.down_size += sz;
            } else {
                self.down = Some(down)
            }
        }
        if let Some(mut up) = self.up.take() {
            debug!("polling upward from {} to {}", self.down_addr, self.up_addr);
            if let Async::Ready(sz) = up.poll()? {
                debug!("upward complete from {} to {}",
                       self.down_addr,
                       self.up_addr);
                self.up_size += sz;
            } else {
                self.up = Some(up)
            }
        }
        if self.down.is_none() && self.up.is_none() {
            Ok(Async::Ready((self.down_size, self.up_size)))
        } else {
            Ok(Async::NotReady)
        }
    }
}
