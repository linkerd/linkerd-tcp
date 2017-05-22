use super::Socket;
use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::rc::Rc;
//use tacho;
use tokio_io::AsyncWrite;

pub fn new(reader: Rc<RefCell<Socket>>,
           writer: Rc<RefCell<Socket>>,
           buf: Rc<RefCell<Vec<u8>>>)
           -> HalfDuplex {
    HalfDuplex {
        reader,
        writer,
        buf,
        pending: None,
        bytes_total: 0,
        completed: false,
        // bytes_total_count: metrics.counter("bytes_total".into()),
        // allocs_count: metrics.counter("allocs_count".into()),
    }
}

/// A future representing reading all data from one side of a proxy connection and writing
/// it to another.
///
/// In the typical case, nothing allocations are required.  If the write side exhibits
/// backpressure, however, a buffer is allocated to
pub struct HalfDuplex {
    reader: Rc<RefCell<Socket>>,
    writer: Rc<RefCell<Socket>>,

    // Holds transient data when copying between the reader and writer.
    buf: Rc<RefCell<Vec<u8>>>,

    // Holds data that can't be fully written.
    pending: Option<Vec<u8>>,

    // The number of bytes we've written so far.
    bytes_total: u64,

    completed: bool,

    // bytes_total_count: tacho::Counter,
    // allocs_count: tacho::Counter,
}

impl Future for HalfDuplex {
    type Item = u64;
    type Error = io::Error;

    /// Reads from from the `reader` into a shared buffer before writing to `writer`.
    ///
    /// If all data cannot be written, the unwritten data is stored in a newly-allocated
    /// buffer. This pending data is flushed before any more data is read.
    fn poll(&mut self) -> Poll<u64, io::Error> {
        trace!("poll");
        let mut writer = self.writer.borrow_mut();
        let mut reader = self.reader.borrow_mut();
        loop {
            if self.completed {
                try_nb!(writer.shutdown());
                writer.tcp_shutdown(Shutdown::Write)?;
                trace!("completed");
                return Ok(self.bytes_total.into());
            }

            // Try to flush pending bytes to the writer.
            if let Some(mut pending) = self.pending.take() {
                let psz = pending.len();
                trace!("writing {} pending bytes", psz);

                let wsz = writer.write(&pending)?;
                trace!("wrote {} bytes", wsz);

                {
                    let wsz = wsz as u64;
                    self.bytes_total += wsz;
                    //self.bytes_total_count.incr(wsz);
                }
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
                trace!("completing: {}B", self.bytes_total);
                self.completed = true;
                try_nb!(writer.shutdown());
                writer.tcp_shutdown(Shutdown::Write)?;
                trace!("completed: {}B", self.bytes_total);
                return Ok(self.bytes_total.into());
            }
            trace!("read {} bytes", rsz);

            // Attempt to write from the shared buffer.
            match writer.write(&buf[..rsz]) {
                Ok(wsz) => {
                    trace!("wrote {} bytes", wsz);
                    {
                        let wsz = wsz as u64;
                        self.bytes_total += wsz;
                        //self.bytes_total_count.incr(wsz);
                    }
                    if wsz < rsz {
                        trace!("saving {} bytes", rsz - wsz);
                        // Allocate a temporary buffer to the unwritten remainder for next
                        // time.
                        //self.allocs_count.incr(1);
                        let mut p = Vec::with_capacity(rsz - wsz);
                        p.copy_from_slice(&buf[wsz..rsz]);
                        self.pending = Some(p);
                        return Ok(Async::NotReady);
                    }
                }
                Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                    //self.allocs_count.incr(1);
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
