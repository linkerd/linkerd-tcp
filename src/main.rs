//! A TCP proxy for the linkerd service mesh.

#[macro_use]
extern crate clap;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
extern crate linkerd_tcp;
extern crate rand;
extern crate tokio_core;

use clap::{Arg, App};
use hyper::Client;
use futures::{Async, Future, Poll, Stream};
use std::{fs, time};
use std::collections::VecDeque;
use std::io::{self, Read};
use tokio_core::reactor::Core;
use tokio_core::net::*;

use linkerd_tcp::*;

// Start a single server.
fn main() {
    // Configure the logger from the RUST_LOG environment variable.
    drop(env_logger::init());

    // Parse and load command-line options.
    let opts = mk_app().get_matches();
    let config_path = opts.value_of(CONFIG_PATH_ARG).unwrap();
    let app = {
        let mut f = fs::File::open(config_path).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        config::from_str(&s).unwrap()
    };

    // TODO split this into more threads...

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let mut runner = Runner::new();
    for proxy in app.proxies.iter() {
        let ref namerd = proxy.namerd;
        let namerd_interval = namerd.interval
            .unwrap_or(time::Duration::new(DEFAULT_NAMERD_SECONDS, 0));
        let namerd_ns = namerd.namespace.clone().unwrap_or("default".into());
        let namerd_path = namerd.path.clone();
        let addrs = {
            let client = Client::new(&handle.clone());
            namerd::resolve(client, namerd.addr, namerd_interval, namerd_ns, namerd_path)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "namerd error"))
        };
        info!("Updating {} from {} every {}s",
              namerd.path,
              namerd.addr,
              namerd_interval.as_secs());

        let balancer = {
            let handle = handle.clone();
            let bufsz = proxy.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
            let maxw = proxy.max_waiters.unwrap_or(DEFAULT_MAX_WAITERS);
            Balancer::new(addrs, bufsz, handle.clone()).into_shared(maxw)
        };

        for server in proxy.servers.iter() {
            info!("Listening on {}", server.addr);
            let listener = TcpListener::bind(&server.addr, &handle.clone()).unwrap();
            let f = listener.incoming().forward(balancer.clone());
            runner.add(f)
        }
    }

    core.run(runner).unwrap();
    info!("Closing.")
}

const CONFIG_PATH_ARG: &'static str = "PATH";
const DEFAULT_BUFFER_SIZE: usize = 65535;
const DEFAULT_MAX_WAITERS: usize = 8;
const DEFAULT_NAMERD_SECONDS: u64 = 60;

fn mk_app() -> App<'static, 'static> {
    App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::with_name(CONFIG_PATH_ARG)
            .required(true)
            .index(1)
            .help("Config file path."))
}

struct Runner<F>(VecDeque<F>);

impl<F: Future> Runner<F> {
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
        if self.0.len() == 0 {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}