use super::Connection;
use super::Ctx;
use super::half_duplex::{self, HalfDuplex};
use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io;
use std::net;
use std::rc::Rc;

pub struct Summary {
    pub to_dst_bytes: usize,
    pub to_src_bytes: usize,
}

pub fn new<S, D>(src: Connection<S>, dst: Connection<D>, buf: Rc<RefCell<Vec<u8>>>) -> Duplex<S, D>
where
    S: Ctx,
    D: Ctx,
{
    let src_addr = src.peer_addr();
    let dst_addr = dst.peer_addr();
    let src = Rc::new(RefCell::new(src));
    let dst = Rc::new(RefCell::new(dst));
    Duplex {
        dst_addr,
        to_dst: Some(half_duplex::new(src.clone(), dst.clone(), buf.clone())),
        to_dst_bytes: 0,

        src_addr,
        to_src: Some(half_duplex::new(dst.clone(), src.clone(), buf)),
        to_src_bytes: 0,
    }
}

/// Joins src and dst transfers into a single Future.
pub struct Duplex<S, D> {
    dst_addr: net::SocketAddr,
    src_addr: net::SocketAddr,
    to_dst: Option<HalfDuplex<S, D>>,
    to_src: Option<HalfDuplex<D, S>>,
    to_dst_bytes: usize,
    to_src_bytes: usize,
}

impl<S: Ctx, D: Ctx> Future for Duplex<S, D> {
    type Item = Summary;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Summary, io::Error> {
        if let Some(mut to_dst) = self.to_dst.take() {
            trace!("polling dstward from {} to {}",
                   self.src_addr,
                   self.dst_addr);
            match to_dst.poll()? {
                Async::Ready(sz) => {
                    trace!("dstward complete from {} to {}",
                           self.src_addr,
                           self.dst_addr);
                    self.to_dst_bytes = sz;
                }
                Async::NotReady => {
                    trace!("dstward not ready");
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
                    self.to_src_bytes = sz;
                }
                Async::NotReady => {
                    trace!("srcward not ready");
                    self.to_src = Some(to_src);
                }
            }
        }

        if self.to_dst.is_none() && self.to_src.is_none() {
            trace!("complete");
            // self.tx_bytes_stat.add(self.tx_bytes);
            // self.rx_bytes_stat.add(self.rx_bytes)
            let summary = Summary {
                to_dst_bytes: self.to_dst_bytes,
                to_src_bytes: self.to_src_bytes,
            };
            Ok(Async::Ready(summary))
        } else {
            trace!("not ready");
            Ok(Async::NotReady)
        }
    }
}
