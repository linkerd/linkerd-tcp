//! A simple Layer 3 proxy. Currently TCP only.

extern crate tokio_core;
extern crate futures;

use std::env;
use std::io;

use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Core;
use tokio_core::io::{EasyBuf, Io, Codec};

use futures::{Future, Stream, Sink};

fn main() {
    // The source addr for clients to connect to.
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:7575".to_string());
    let addr = addr.parse().unwrap();

    // The target addr to proxy connections to.
    let target_addr = env::args().nth(2).unwrap_or("127.0.0.1:8080".to_string());
    let target_addr = target_addr.parse().unwrap();

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let listener = TcpListener::bind(&addr, &handle).unwrap();
    println!("Listening on: {}", addr);

    let srv = listener.incoming().for_each(move |(ingress, addr)| {
        println!("New Connection: {}", addr);
        //   sink      , stream
        let (in_sink, in_stream) = ingress.framed(Http10Request).split();
        let handler = TcpStream::connect(&target_addr, &handle).and_then(|egress| {
            //   sink     , stream
            let (out_sink, out_stream) = egress.framed(Http10Response).split();
            out_sink.send_all(in_stream).and_then(|_| in_sink.send_all(out_stream))
        });
        handle.spawn(handler.then(|_| Ok(())));

        Ok(())
    });

    // let handle = core.handle();
    // handle.spawn(srv.map_err(|e| panic!("srv error: {}", e)));
    core.run(srv).unwrap();
}

pub struct Http10Request;

impl Http10Request {
    // Given a [u8], find the position of two newlines. Return Some(position) if found, None otherwise.
    fn find_two_newlines(buf: &[u8]) -> Option<usize> {
        println!("buf: {:?}", buf);
        println!("buf.len(): {:?}", buf.len());
        // TODO: find a nicer, more functional way of doing this.
        let mut result = None;

        if buf.len() < 4 {
            return result;
        } else {
            for i in 1..buf.len() {
                if buf[i] == ('\r' as u8) && buf[i + 1] == ('\n' as u8) &&
                   buf[i + 2] == ('\r' as u8) && buf[i + 3] == ('\n' as u8) {
                    result = Some(i);
                    break;
                }
            }
            return result;
        }
    }
}

// A Codec for finding when two newlines occur. Not really an HTTP-specific codec,
// instead think of it as a simple framer
impl Codec for Http10Request {
    type In = EasyBuf;
    type Out = EasyBuf;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<EasyBuf>, io::Error> {
        println!("Http10Request::decode");
        // find where in the document there are two newlines
        match Http10Request::find_two_newlines(buf.as_slice()) {
            Some(i) => {
                println!("Found an HTTP/1.0 request");
                Ok(Some(buf.drain_to(i + 1).into()))
            }
            None => {
                println!("Not enough for an HTTP/1.0 request");
                Ok(None)
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut EasyBuf) -> io::Result<EasyBuf> {
        println!("Http10Request::decode_eof");
        let amt = buf.len();
        let rv: io::Result<EasyBuf> = Ok(buf.drain_to(amt));
        rv
    }

    fn encode(&mut self, msg: EasyBuf, into: &mut Vec<u8>) {
        println!("Http10Request::encode to body {:?}", msg.as_slice());
        into.extend_from_slice(msg.as_slice());
    }
}

pub struct Http10Response;

// This Codec simply copies everything in the buffer.
impl Codec for Http10Response {
    type In = EasyBuf;
    type Out = EasyBuf;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<EasyBuf>, io::Error> {
        println!("HTTP10Response::decode");
        let amt = buf.len();
        Ok(Some(buf.drain_to(amt)))
    }

    fn decode_eof(&mut self, buf: &mut EasyBuf) -> io::Result<EasyBuf> {
        println!("Http10Response::decode_eof");
        let amt = buf.len();
        let rv: io::Result<EasyBuf> = Ok(buf.drain_to(amt));
        rv
    }

    fn encode(&mut self, msg: EasyBuf, into: &mut Vec<u8>) {
        println!("Http10Response::encode");
        into.extend_from_slice(msg.as_slice());
    }
}
