use super::{Path, ProxyStream, Socket};
use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io;
use std::net;
use std::rc::Rc;
use std::time;
//use tacho;

/// A src or dst connection.
pub struct Connection {
    pub envelope: Envelope,
    socket: Socket,
}

impl Connection {
    pub fn new(e: Envelope, s: Socket) -> Connection {
        Connection {
            envelope: e,
            socket: s,
        }
    }
}

/// Describes a connection's metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The address of the local interface.
    pub local_addr: net::SocketAddr,

    /// The address of the remote address..
    pub remote_addr: net::SocketAddr,

    /// An optional identifier for the remote endpoint (e.g. from TLS -- SNI or client ID or...).
    pub remote_id: Option<Path>,

    /// The destination service name.
    pub dst_name: Path,

    /// The time by which the stream must be established.
    pub connect_deadline: Option<time::Instant>,

    /// The time by which the stream must be completed.
    pub stream_deadline: Option<time::Instant>,

    /// The amount of time that a stream may remain idle before being closed.
    pub idle_timeout: Option<time::Duration>,
}

/// Joins src and dst transfers into a single Future.
pub struct Duplex {
    pub src: Rc<Envelope>,
    pub dst: Rc<Envelope>,
    tx: Option<ProxyStream>,
    rx: Option<ProxyStream>,

    tx_bytes: u64,
    //tx_bytes_stat: tacho::Stat,
    rx_bytes: u64,
    //rx_bytes_stat: tacho::Stat,
}

impl Duplex {
    pub fn new(src: Connection, dst: Connection, buf: Rc<RefCell<Vec<u8>>>) -> Duplex {
        let src_env = Rc::new(src.envelope);
        let dst_env = Rc::new(dst.envelope);

        let src = Rc::new(RefCell::new(src.socket));
        let dst = Rc::new(RefCell::new(dst.socket));
        // let tx_bytes_stat = tx_metrics.stat("bytes".into());
        // let rx_bytes_stat = rx_metrics.stat("bytes".into());

        Duplex {
            src: src_env,
            dst: dst_env,

            tx: Some(ProxyStream::new(src.clone(), dst.clone(), buf.clone())),
            rx: Some(ProxyStream::new(dst, src, buf)),

            tx_bytes: 0,
            //tx_bytes_stat: tx_bytes_stat,
            rx_bytes: 0,
            //rx_bytes_stat: rx_bytes_stat,
        }
    }

    fn src_addr(&self) -> &net::SocketAddr {
        &self.src.remote_addr
    }
    fn dst_addr(&self) -> &net::SocketAddr {
        &self.dst.remote_addr
    }
}

impl Future for Duplex {
    type Item = Summary;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Summary, io::Error> {
        if let Some(mut tx) = self.tx.take() {
            trace!("polling dstward from {} to {}",
                   self.src_addr(),
                   self.dst_addr());
            match tx.poll()? {
                Async::Ready(sz) => {
                    trace!("dstward complete from {} to {}",
                           self.src_addr(),
                           self.dst_addr());
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
                   self.dst_addr(),
                   self.src_addr());
            match rx.poll()? {
                Async::Ready(sz) => {
                    trace!("srcward complete from {} to {}",
                           self.dst_addr(),
                           self.src_addr());
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
            // self.tx_bytes_stat.add(self.tx_bytes);
            // self.rx_bytes_stat.add(self.rx_bytes);
            let summary = Summary {
                src: self.src.clone(),
                dst: self.dst.clone(),
                bytes: self.tx_bytes,
            };
            Ok(Async::Ready(summary))
        } else {
            trace!("not ready");
            Ok(Async::NotReady)
        }
    }
}

pub struct Summary {
    pub src: Rc<Envelope>,
    pub dst: Rc<Envelope>,
    pub bytes: u64,
}
