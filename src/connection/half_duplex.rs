use super::Connection;
use super::Ctx;
use futures::{Async, Future, Poll};
use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::rc::Rc;
//use tacho;

use tokio_io::AsyncWrite;

pub fn new<R, W>(
    reader: Rc<RefCell<Connection<R>>>,
    writer: Rc<RefCell<Connection<W>>>,
    buf: Rc<RefCell<Vec<u8>>>,
) -> HalfDuplex<R, W>
where
    R: Ctx,
    W: Ctx,
{
    HalfDuplex {
        reader,
        writer,
        buf,
        pending: None,
        bytes_total: 0,
        should_shutdown: false,
        // bytes_total_count: metrics.counter("bytes_total".into()),
        // allocs_count: metrics.counter("allocs_count".into()),
    }
}

/// A future representing reading all data from one side of a proxy connection and writing
/// it to another.
///
/// In the typical case, nothing allocations are required.  If the write side exhibits
/// backpressure, however, a buffer is allocated to
pub struct HalfDuplex<R, W> {
    reader: Rc<RefCell<Connection<R>>>,
    writer: Rc<RefCell<Connection<W>>>,

    // Holds transient data when copying between the reader and writer.
    buf: Rc<RefCell<Vec<u8>>>,

    // Holds data that can't be fully written.
    pending: Option<Vec<u8>>,

    // The number of bytes we've written so far.
    bytes_total: usize,

    // Indicates that that the reader has returned 0 and the writer should be shut down.
    should_shutdown: bool,

    // bytes_total_count: tacho::Counter,
    // allocs_count: tacho::Counter,
}

impl<R, W> Future for HalfDuplex<R, W>
where
    R: Ctx,
    W: Ctx,
{
    type Item = usize;
    type Error = io::Error;

    /// Reads from from the `reader` into a shared buffer before writing to `writer`.
    ///
    /// If all data cannot be written, the unwritten data is stored in a newly-allocated
    /// buffer. This pending data is flushed before any more data is read.
    fn poll(&mut self) -> Poll<usize, io::Error> {
        trace!("poll");
        let mut writer = self.writer.borrow_mut();
        let mut reader = self.reader.borrow_mut();

        // Because writer.socket.shutdown may return WouldBlock, we may already be
        // shutting down and need to resume graceful shutdown.
        if self.should_shutdown {
            try_nb!(writer.socket.shutdown());
            writer.socket.tcp_shutdown(Shutdown::Write)?;
            return Ok(Async::Ready(self.bytes_total));
        }

        // If we've read more than we were able to write previously, then write all of it
        // until the write would block.
        if let Some(mut pending) = self.pending.take() {
            trace!("writing {} pending bytes", pending.len());
            while !pending.is_empty() {
                match writer.socket.write(&pending) {
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        self.pending = Some(pending);
                        return Ok(Async::NotReady);
                    }
                    Err(e) => return Err(e),
                    Ok(wsz) => {
                        // Drop the portion of the buffer that we've already written.
                        // There may or may not be more pending data remaining.
                        pending.drain(0..wsz);
                        self.bytes_total += wsz;
                        writer.ctx.wrote(wsz);
                    }
                }
            }
        }

        // Read and write data until one of the endpoints is not ready. All data is read
        // into a thread-global transfer buffer and then written from this buffer. If all
        // data cannot be written, it is copied into a newly-allocated local buffer to be
        // flushed later.
        loop {
            assert!(self.pending.is_none());

            let mut rbuf = self.buf.borrow_mut();
            let rsz = try_nb!(reader.socket.read(&mut rbuf));
            reader.ctx.read(rsz);
            if rsz == 0 {
                self.should_shutdown = true;
                try_nb!(writer.socket.shutdown());
                writer.socket.tcp_shutdown(Shutdown::Write)?;
                return Ok(Async::Ready(self.bytes_total));
            }

            let mut wbuf = &rbuf[..rsz];
            while !wbuf.is_empty() {
                match writer.socket.write(wbuf) {
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        let mut p = vec![0; wbuf.len()];
                        p.copy_from_slice(wbuf);
                        self.pending = Some(p);
                        return Ok(Async::NotReady);
                    }
                    Err(e) => return Err(e),
                    Ok(wsz) => {
                        self.bytes_total += wsz;
                        writer.ctx.wrote(wsz);
                        wbuf = &wbuf[wsz..];
                    }
                }
            }
        }
    }
}
