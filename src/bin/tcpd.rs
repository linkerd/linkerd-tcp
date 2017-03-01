//! A simple Layer 4 proxy. Currently TCP only.

#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate futures;
extern crate galadriel;
extern crate getopts;
extern crate rand;
extern crate tokio_core;

use futures::{Future, Stream};
use rand::{Rng, thread_rng};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::env;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::process;
use std::sync::{Arc, atomic, RwLock};
use std::thread;
use std::time::Duration;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};

use galadriel::transfer::BufferedTransfer;

const WINDOW_SIZE: usize = 64 * 1024;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let opts = {
        let mut opts = getopts::Options::new();
        opts.optflag("h", "help", "print this help menu");
        opts.optopt("l",
                    "listen-addr",
                    "listen on the given addr:port [default: 0.0.0.0:7575]",
                    "ADDR");
        opts.optopt("n",
                    "namerd-addr",
                    "Connect to Namerd's HTTP API on addr:port [default: 127.0.0.1:4180]",
                    "ADDR");
        opts
    };

    let matches = opts.parse(&args[1..]).unwrap();
    drop(env_logger::init());

    let listen_addr = matches.opt_str("listen-addr")
        .unwrap_or("0.0.0.0:7575".to_string())
        .parse()
        .unwrap();

    let namerd_addr = matches.opt_str("namerd-addr")
        .unwrap_or("127.0.0.1:4180".to_string())
        .parse()
        .unwrap();

    let target_path;
    if matches.free.len() == 1 {
        let path = matches.free[0].clone();
        if path.starts_with("/") {
            target_path = path;
        } else {
            let ref mut err = io::stderr();
            let _ = writeln!(err, "invalid path: {}", path);
            print_usage(err, &program, &opts);
            process::exit(64);
        }
    } else {
        let ref mut err = io::stderr();
        let _ = writeln!(err, "missing TARGET");
        print_usage(err, &program, &opts);
        process::exit(64);
    }

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();

    let proxy = {
        let handle = core.handle();
        let listener = TcpListener::bind(&listen_addr, &handle).unwrap();
        info!("Listening on {}", listen_addr);

        let namerd = NamerdLookup::periodic(&namerd_addr, Duration::from_secs(3));
        info!("Querying namerd for {} on {}", namerd_addr, &target_path);

        let buffer = Rc::new(RefCell::new(vec![0; WINDOW_SIZE]));
        listener.incoming().for_each(move |(up_stream, up_addr)| {
            let down_endpoint = namerd.endpoint();
            let down_addr = down_endpoint.addr();

            debug!("Proxying {} to {}", up_addr, down_addr);
            let connect = TcpStream::connect(&down_addr, &handle);
            let tx = {
                let buffer = buffer.clone();
                connect.and_then(|down_stream| transmit_duplex(up_stream, down_stream, buffer))
            };
            handle.spawn(tx.then(move |res| {
                match res {
                    Err(e) => error!("Error proxying {} to {}: {}", up_addr, down_addr, e),
                    Ok((down_bytes, up_bytes)) => {
                        debug!("Proxied {} to {}: down={}B up={}B",
                               up_addr,
                               down_addr,
                               down_bytes,
                               up_bytes)
                    }
                };
                Ok(())
            }));
            Ok(())
        })
    };

    // You can run multiple `core.run()` in multiple threads, and use
    // SO_REUSEPORT tokio does not currently support thread pool
    // executors but you can thread.spawn N times.
    core.run(proxy).unwrap();
}

fn print_usage(out: &mut Write, program: &str, opts: &getopts::Options) {
    let brief = format!("Usage: {} TARGET [options]", program);
    let _ = write!(out, "{}", opts.usage(&brief));
}

fn transmit_duplex(up_stream: TcpStream,
                   down_stream: TcpStream,
                   buffer: Rc<RefCell<Vec<u8>>>)
                   -> Box<Future<Item = (u64, u64), Error = io::Error>> {
    let up = Rc::new(up_stream);
    let down = Rc::new(down_stream);
    let down_tx = BufferedTransfer::new(up.clone(), down.clone(), buffer.clone());
    let up_tx = BufferedTransfer::new(down, up, buffer.clone());
    Box::new(down_tx.join(up_tx))
}

trait WithAddr {
    fn addr(&self) -> SocketAddr;
}

trait Endpointer {
    type Endpoint: WithAddr;
    fn endpoint(&self) -> Self::Endpoint;
}

#[derive(Debug, PartialEq)]
struct EndpointState {
    weight: f32,
    load: f64,
}

struct NamerdLookup {
    is_running: Arc<atomic::AtomicBool>,
    thread: Rc<RefCell<thread::JoinHandle<()>>>,
    endpoints: Arc<RwLock<HashMap<SocketAddr, Arc<RwLock<EndpointState>>>>>,
}

impl NamerdLookup {
    fn periodic(addr: &SocketAddr, period: Duration) -> NamerdLookup {
        let is_running = Arc::new(atomic::AtomicBool::new(true));
        let endpoints = {
            let mut set = HashMap::new();
            // FIXME
            set.insert("127.0.0.1:8080".to_string().parse().unwrap(),
                       Arc::new(RwLock::new(EndpointState {
                           weight: 1.0,
                           load: 0.0,
                       })));
            Arc::new(RwLock::new(set))
        };
        let thread = {
            let is_running = is_running.clone();
            let endpoints = endpoints.clone();
            let addr = addr.clone();
            thread::Builder::new()
                .name(format!("namerd-{}-{}", addr.ip(), addr.port()).to_string())
                .spawn(move || while is_running.load(atomic::Ordering::Relaxed) {
                    debug!("TODO hit the resolve api at {}...", addr);
                    let eps = endpoints.write().unwrap();
                    drop(eps);
                    thread::sleep(period);
                })
                .unwrap()
        };
        NamerdLookup {
            is_running: is_running,
            thread: Rc::new(RefCell::new(thread)),
            endpoints: endpoints,
        }
    }
}

impl Drop for NamerdLookup {
    fn drop(&mut self) {
        info!("shutting down namerd");
        self.is_running.store(false, atomic::Ordering::SeqCst);
        // let mut thread = self.thread.borrow_mut();
        // thread.join().unwrap();
    }
}

struct Endpoint {
    addr: SocketAddr,
    state: Arc<RwLock<EndpointState>>,
}

impl WithAddr for Endpoint {
    fn addr(&self) -> SocketAddr {
        self.addr.clone()
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        let mut state = self.state.write().unwrap();
        assert!(state.load >= 1.0);
        state.load -= 1.0;
    }
}

impl Endpointer for NamerdLookup {
    type Endpoint = Endpoint;

    fn endpoint(&self) -> Endpoint {
        let endpoints = self.endpoints.read().unwrap();
        let addrs: Vec<(&SocketAddr, &Arc<RwLock<EndpointState>>)> = endpoints.iter().collect();
        // XXX for now we just choose a node at random.
        // We should employ some strategy that combines load and weight
        let (addr, state): (&SocketAddr, &Arc<RwLock<EndpointState>>) =
            *thread_rng().choose(&addrs).unwrap();
        let addr = addr.clone();
        let state = (*state).clone();
        {
            let mut state = state.write().unwrap();
            debug!("Using {} {:?}", addr, *state);
            state.load += 1.0;
        }
        Endpoint {
            addr: addr,
            state: state,
        }
    }
}
