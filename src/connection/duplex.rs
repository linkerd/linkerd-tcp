use super::half_duplex::{self, HalfDuplex};
use super::super::balancer::{DstCtx, DstConnection};
use super::super::connection::{ConnectionCtx, ctx};
use super::super::server::SrcConnection;
use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io;
use std::net;
use std::rc::Rc;
//use tacho;

pub struct DuplexCtx {
    pub src: ConnectionCtx<ctx::Null>,
    pub dst: ConnectionCtx<DstCtx>,
}

pub struct Summary {
    pub to_dst_bytes: u64,
    pub to_src_bytes: u64,
}

/// Joins src and dst transfers into a single Future.
pub struct Duplex {
    ctx: Option<DuplexCtx>,
    to_dst: Option<HalfDuplex>,
    to_src: Option<HalfDuplex>,

    to_dst_bytes: u64,
    //tx_bytes_stat: tacho::Stat,
    to_src_bytes: u64,
    //rx_bytes_stat: tacho::Stat,
}

impl Duplex {
    pub fn new(src: SrcConnection, dst: DstConnection, buf: Rc<RefCell<Vec<u8>>>) -> Duplex {
        let src_socket = Rc::new(RefCell::new(src.socket));
        let dst_socket = Rc::new(RefCell::new(dst.socket));
        Duplex {
            ctx: Some(DuplexCtx {
                          src: src.ctx,
                          dst: dst.ctx,
                      }),

            to_dst: Some(half_duplex::new(src_socket.clone(), dst_socket.clone(), buf.clone())),
            to_dst_bytes: 0,

            to_src: Some(half_duplex::new(dst_socket, src_socket, buf)),
            to_src_bytes: 0,
        }
    }

    fn src_addr(&self) -> net::SocketAddr {
        match self.ctx {
            None => panic!("missing context"),
            Some(ref ctx) => ctx.src.peer_addr(),
        }
    }

    fn dst_addr(&self) -> net::SocketAddr {
        match self.ctx {
            None => panic!("missing context"),
            Some(ref ctx) => ctx.dst.peer_addr(),
        }
    }
}

impl Future for Duplex {
    type Item = Summary;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Summary, io::Error> {
        if let Some(mut to_dst) = self.to_dst.take() {
            trace!("polling dstward from {} to {}",
                   self.src_addr(),
                   self.dst_addr());
            match to_dst.poll()? {
                Async::Ready(sz) => {
                    trace!("dstward complete from {} to {}",
                           self.src_addr(),
                           self.dst_addr());
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
                   self.dst_addr(),
                   self.src_addr());
            match to_src.poll()? {
                Async::Ready(sz) => {
                    trace!("srcward complete from {} to {}",
                           self.dst_addr(),
                           self.src_addr());
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
            let ctx = self.ctx.take().expect("missing context");
            drop(ctx);
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
