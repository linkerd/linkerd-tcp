use futures::{Async, Future, Poll, Stream};
use hyper::Client;
use std::boxed::Box;
use std::{io, time};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpListener;

pub mod config;

use namerd;
use Balancer;

const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_MAX_WAITERS: usize = 8;
const DEFAULT_NAMERD_SECONDS: u64 = 60;

// TODO use another error type that indicates config error more clearly.
pub fn run(config_str: &str,
           handle: &Handle)
           -> io::Result<Box<Future<Item = (), Error = io::Error>>> {
    let app = config::from_str(config_str)?;

    let mut runner = Runner::new();

    let bufsz = app.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
    let buf = Rc::new(RefCell::new(vec![0;bufsz]));
    for proxy in &app.proxies {
        let addrs = {
            let n = &proxy.namerd;
            let interval = n.interval
                .unwrap_or_else(|| time::Duration::new(DEFAULT_NAMERD_SECONDS, 0));
            let ns = n.namespace.clone().unwrap_or_else(|| "default".into());
            info!("Updating {} in {} from {} every {}s",
                  n.path,
                  ns,
                  n.addr,
                  interval.as_secs());

            let client = Client::new(&handle.clone());
            namerd::resolve(client, n.addr, interval, &ns, &n.path)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "namerd error"))
        };

        let balancer = {
            let handle = handle.clone();
            let maxw = proxy.max_waiters.unwrap_or(DEFAULT_MAX_WAITERS);
            Balancer::new(addrs, buf.clone(), handle.clone()).into_shared(maxw)
        };

        for server in &proxy.servers {
            info!("Listening on {}", server.addr);
            // TODO TLS
            let listener = {
                TcpListener::bind(&server.addr, &handle.clone()).unwrap()
            };
            let f = listener.incoming().forward(balancer.clone());
            runner.add(f)
        }
    }

    Ok(Box::new(runner))
}

/// Tracks a list of `F`-typed `Future`s until are complete.
pub struct Runner<F>(VecDeque<F>);

impl<F> Runner<F> {
    fn new() -> Runner<F> {
        Runner(VecDeque::new())
    }

    fn add(&mut self, f: F) {
        self.0.push_back(f);
    }
}

impl<F> Future for Runner<F>
    where F: Future<Error = io::Error>
{
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<(), io::Error> {
        let sz = self.0.len();
        for _ in 0..sz {
            let mut f = self.0.pop_front().unwrap();
            match f.poll()? {
                Async::Ready(_) => {}
                Async::NotReady => {
                    self.0.push_back(f);
                }
            }
        }
        if self.0.is_empty() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}