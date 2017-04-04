use futures::{Async, Future, Poll};

use lb::{ProxyStream, Socket, WithAddr};
use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use tacho;

/// Joins src and dst transfers into a single Future.
pub struct Duplex {
    pub src_addr: SocketAddr,
    pub dst_addr: SocketAddr,
    tx: Option<ProxyStream>,
    rx: Option<ProxyStream>,

    tx_bytes: u64,
    tx_bytes_stat: tacho::StatKey,
    rx_bytes: u64,
    rx_bytes_stat: tacho::StatKey,
    metrics: tacho::Metrics, // only used for obtaining a recorder.
}

impl Duplex {
    pub fn new(src: Socket,
               dst: Socket,
               buf: Rc<RefCell<Vec<u8>>>,
               tx_metrics: tacho::Metrics,
               rx_metrics: tacho::Metrics)
               -> Duplex {
        let src_addr = src.addr();
        let dst_addr = dst.addr();
        let src = Rc::new(RefCell::new(src));
        let dst = Rc::new(RefCell::new(dst));
        let tx_bytes_stat = tx_metrics.scope().stat("bytes".into());
        let rx_byte_stat = rx_metrics.scope().stat("bytes".into());
        let metrics = rx_metrics.clone(); // doesn't matter which one.
        let tx = ProxyStream::new(src.clone(), dst.clone(), buf.clone(), tx_metrics);
        let rx = ProxyStream::new(dst, src, buf, rx_metrics);
        Duplex {
            src_addr: src_addr,
            dst_addr: dst_addr,
            tx: Some(tx),
            rx: Some(rx),

            tx_bytes: 0,
            tx_bytes_stat: tx_bytes_stat,

            rx_bytes: 0,
            rx_bytes_stat: rx_byte_stat,

            metrics: metrics,
        }
    }
}

impl Future for Duplex {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<(), io::Error> {
        if let Some(mut tx) = self.tx.take() {
            trace!("polling dstward from {} to {}",
                   self.src_addr,
                   self.dst_addr);
            match tx.poll()? {
                Async::Ready(sz) => {
                    trace!("dstward complete from {} to {}",
                           self.src_addr,
                           self.dst_addr);
                    self.tx_bytes += sz;
                }
                Async::NotReady => {
                    trace!("dstward not ready");
                    self.tx = Some(tx);
                }
            }
        }

        if let Some(mut rx) = self.rx.take() {
            trace!("polling srcward from {} to {}",
                   self.dst_addr,
                   self.src_addr);
            match rx.poll()? {
                Async::Ready(sz) => {
                    trace!("srcward complete from {} to {}",
                           self.dst_addr,
                           self.src_addr);
                    self.rx_bytes += sz;
                }
                Async::NotReady => {
                    trace!("srcward not ready");
                    self.rx = Some(rx);
                }
            }
        }

        if self.tx.is_none() && self.rx.is_none() {
            trace!("complete");
            let mut rec = self.metrics.recorder();
            rec.add(&self.tx_bytes_stat, self.tx_bytes);
            rec.add(&self.rx_bytes_stat, self.rx_bytes);
            Ok(Async::Ready(()))
        } else {
            trace!("not ready");
            Ok(Async::NotReady)
        }
    }
}
