//! Inspired by tokio-socks5 example.

use futures::{Async, Future, Poll};
use std::io::{self, Read, Write};
use std::cell::RefCell;
use std::rc::Rc;
use tokio_io::AsyncWrite;

use lb::Socket;

/// A future representing reading all data from one side of a proxy connection and writing
/// it to another.
///
/// In the typical case, nothing allocations are required.  If the write side exhibits
/// backpressure, however, a buffer is allocated to
pub struct ProxyStream {
    reader: Rc<RefCell<Socket>>,
    writer: Rc<RefCell<Socket>>,

    // Holds transient data when copying between the reader and writer.
    buf: Rc<RefCell<Vec<u8>>>,

    // Holds data that can't be fully written.
    pending: Option<Vec<u8>>,

    // The number of bytes we've written so far.
    bytes_written: u64,

    completed: bool,
}

impl ProxyStream {
    pub fn new(r: Rc<RefCell<Socket>>,
               w: Rc<RefCell<Socket>>,
               b: Rc<RefCell<Vec<u8>>>)
               -> ProxyStream {
        ProxyStream {
            reader: r,
            writer: w,
            buf: b,
            pending: None,
            bytes_written: 0,
            completed: false,
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
    /// Reads from from the `reader` into a shared buffer, before writing to If a Flushes
    /// all pending data before reading any more.
    fn poll(&mut self) -> Poll<u64, io::Error> {
        trace!("poll");
        if self.completed {
            return Ok(self.bytes_written.into());
        }

        let mut writer = self.writer.borrow_mut();
        let mut reader = self.reader.borrow_mut();
        loop {
            // Try to flush pending bytes to the writer.
            if let Some(mut pending) = self.pending.take() {
                let psz = pending.len();
                trace!("writing {} pending bytes", psz);

                let wsz = try!(writer.write(&pending));
                trace!("wrote {} bytes", wsz);

                self.bytes_written += wsz as u64;
                if wsz < psz {
                    trace!("saving {} bytes", psz - wsz);
                    // If all of the pending bytes couldn't be complete, save the
                    // remainder for next time.
                    pending.drain(0..wsz);
                    self.pending = Some(pending);
                    return Ok(Async::NotReady);
                }
            }
            assert!(self.pending.is_none());

            // Read some data into our shared buffer.
            let mut buf = self.buf.borrow_mut();
            let rsz = try_nb!(reader.read(&mut buf));
            if rsz == 0 {
                // Nothing left to read, return the total number of bytes transferred.
                trace!("completed: {}B", self.bytes_written);
                self.completed = true;
                match writer.shutdown()? {
                    Async::NotReady => {
                        return Ok(Async::NotReady);
                    }
                    Async::Ready(_) => {
                        return Ok(self.bytes_written.into());
                    }
                }
            }
            trace!("read {} bytes", rsz);

            // Attempt to write from the shared buffer.
            match writer.write(&buf[..rsz]) {
                Ok(wsz) => {
                    trace!("wrote {} bytes", wsz);
                    self.bytes_written += wsz as u64;
                    if wsz < rsz {
                        trace!("saving {} bytes", rsz - wsz);
                        // Allocate a temporary buffer to the unwritten remainder for next
                        // time.
                        let mut p = Vec::with_capacity(rsz - wsz);
                        p.copy_from_slice(&buf[wsz..rsz]);
                        self.pending = Some(p);
                        return Ok(Async::NotReady);
                    }
                }
                Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                    let mut p = Vec::with_capacity(rsz);
                    p.copy_from_slice(&buf);
                    self.pending = Some(p);
                    return Ok(Async::NotReady);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }

            // We shouldn't be looping if we couldn't write everything.
            assert!(self.pending.is_none());
        }
    }
}