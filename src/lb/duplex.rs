use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::net::SocketAddr;

use lb::{ProxyStream, Socket, WithAddr};

/// Joins src and dst transfers into a single Future.
pub struct Duplex {
    pub src_addr: SocketAddr,
    pub dst_addr: SocketAddr,
    to_dst: Option<ProxyStream>,
    to_src: Option<ProxyStream>,
    to_dst_size: u64,
    to_src_size: u64,
}

impl Duplex {
    pub fn new(src: Socket, dst: Socket, buf: Rc<RefCell<Vec<u8>>>) -> Duplex {
        let src_addr = src.addr();
        let dst_addr = dst.addr();
        let src = Rc::new(RefCell::new(src));
        let dst = Rc::new(RefCell::new(dst));
        Duplex {
            src_addr: src_addr,
            dst_addr: dst_addr,
            to_dst: Some(ProxyStream::new(src.clone(), dst.clone(), buf.clone())),
            to_src: Some(ProxyStream::new(dst, src, buf)),
            to_dst_size: 0,
            to_src_size: 0,
        }
    }
}

impl Future for Duplex {
    type Item = (u64, u64);
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, io::Error> {
        if let Some(mut to_dst) = self.to_dst.take() {
            trace!("polling dstward from {} to {}",
                   self.src_addr,
                   self.dst_addr);
            match to_dst.poll()? {
                Async::Ready(sz) => {
                    trace!("dstward complete from {} to {}",
                           self.src_addr,
                           self.dst_addr);
                    self.to_dst_size += sz;
                }
                Async::NotReady => {
                    self.to_dst = Some(to_dst);
                }
            }
        }

        if let Some(mut to_src) = self.to_src.take() {
            trace!("polling srcward from {} to {}",
                   self.dst_addr,
                   self.src_addr);
            match to_src.poll()? {
                Async::Ready(sz) => {
                    trace!("srcward complete from {} to {}",
                           self.dst_addr,
                           self.src_addr);
                    self.to_src_size += sz;
                }
                Async::NotReady => {
                    self.to_src = Some(to_src);
                }
            }
        }

        if self.to_dst.is_none() && self.to_src.is_none() {
            Ok(Async::Ready((self.to_dst_size, self.to_src_size)))
        } else {
            Ok(Async::NotReady)
        }
    }
}
