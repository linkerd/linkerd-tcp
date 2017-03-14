//! Inspired by tokio-socks5 example.

use futures::{Async, Future, Poll};
use std::net;
use std::cell::RefCell;
use std::rc::Rc;
use std::io::{self, Read, Write};
use tokio_core::net::TcpStream;

/// A future representing reading all data from one side of a proxy connection and writing
/// it to another.
///
/// In the typical case, nothing allocations are required.  If the write side exhibits
/// backpressure, however, a buffer is allocated to
pub struct ProxyStream {
    reader: Rc<TcpStream>,
    writer: Rc<TcpStream>,

    // The shared global buffer that all connections on our server are using.
    buf: Rc<RefCell<Vec<u8>>>,

    // Holds data that can't be fully written.
    pending: Option<Vec<u8>>,

    // The number of bytes we've written so far.
    bytes_transferred: u64,
}

impl ProxyStream {
    pub fn new(reader: Rc<TcpStream>,
               writer: Rc<TcpStream>,
               buffer: Rc<RefCell<Vec<u8>>>)
               -> ProxyStream {
        ProxyStream {
            reader: reader,
            writer: writer,
            buf: buffer,
            pending: None,
            bytes_transferred: 0,
        }
    }
}

// Here we implement the `Future` trait for `Transfer` directly. This does not
// use any combinators, and shows how you might implement it in custom
// situations if needed.
impl Future for ProxyStream {
    // Our future resolves to the number of bytes transferred, or an I/O error
    // that happens during the connection, if any.
    type Item = u64;
    type Error = io::Error;

    /// Attempts to drive this future to completion.
    ///
    /// Reads from from the `reader` into a shared buffer, before writing to If a
    /// Flushes all pending data before reading any more.
    fn poll(&mut self) -> Poll<u64, io::Error> {
        loop {
            if !self.writer.poll_write().is_ready() {
                debug!("writer not ready");
                return Ok(Async::NotReady);
            }

            // Try to flush pending bytes to the writer.
            if let Some(mut pending) = self.pending.take() {
                let psz = pending.len();
                debug!("writing {} pending bytes", psz);
                let wsz = try!((&*self.writer).write(&pending));
                debug!("wrote {} bytes", wsz);
                self.bytes_transferred += wsz as u64;
                if wsz < psz {
                    // If all of the pending bytes couldn't be complete, save the
                    // remainder for next time.
                    pending.drain(0..wsz);
                    self.pending = Some(pending);
                    return Ok(Async::NotReady);
                }
            }
            assert!(self.pending.is_none());

            // There's nothing pending, so try to proxy some data (if both sides are
            // ready for it).
            if !self.reader.poll_read().is_ready() || !self.writer.poll_write().is_ready() {
                debug!("writer or reader is not ready");
                return Ok(Async::NotReady);
            }

            // Read some data into our shared buffer.
            let mut buf = self.buf.borrow_mut();
            let rsz = try_nb!((&*self.reader).read(&mut buf));
            if rsz == 0 {
                // Nothing left to read, return the total number of bytes transferred.
                debug!("read end of stream");
                try!(self.writer.shutdown(net::Shutdown::Write));
                return Ok(self.bytes_transferred.into());
            }
            debug!("read {} bytes", rsz);

            // Attempt to write from the shared buffer.
            let wsz = try!((&*self.writer).write(&buf[..rsz]));
            debug!("wrote {} bytes", wsz);
            self.bytes_transferred += wsz as u64;
            if wsz < rsz {
                // Allocate a temporary buffer to the unwritten remainder for next time.
                let mut p = Vec::with_capacity(rsz - wsz);
                p.copy_from_slice(&buf[wsz..rsz]);
                self.pending = Some(p);
                return Ok(Async::NotReady);
            }

            // We shouldn't be looping if we couldn't write everything.
            assert!(self.pending.is_none());
        }
    }
}
