//! A simple Layer 4 proxy. Currently TCP only.

#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate futures;
extern crate galadriel;
extern crate getopts;
#[macro_use]
extern crate hyper;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;
extern crate url;

use futures::{Future, Stream};
use hyper::Client;
use hyper::status::StatusCode;
use rand::{Rng, thread_rng};
use serde_json as json;
use std::cell::RefCell;
use std::collections::{HashSet, HashMap};
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
use url::Url;

use galadriel::transfer::BufferedTransfer;

const WINDOW_SIZE: usize = 64 * 1024;

fn stderr(msg: String) {
    let _ = writeln!(&mut io::stderr(), "{}", msg);
}

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
        opts.optopt("N",
                    "namerd-ns",
                    "Namerd namespace [default: default]",
                    "NS");
        opts.optopt("i",
                    "namerd-interval",
                    "Namerd poll interval, in seconds [default: 60]",
                    "SECONDS");
        opts
    };

    let flags = opts.parse(&args[1..]).unwrap();
    drop(env_logger::init());

    let listen_addr = flags.opt_str("listen-addr")
        .unwrap_or("0.0.0.0:7575".to_string())
        .parse()
        .unwrap();

    let namerd_addr = flags.opt_str("namerd-addr")
        .unwrap_or("127.0.0.1:4180".to_string())
        .parse()
        .unwrap();

    let namerd_interval = flags.opt_str("namerd-interval")
        .map(|s| Duration::from_secs(s.parse().unwrap()))
        .unwrap_or(Duration::from_secs(60));

    let namerd_ns = flags.opt_str("namerd-ns")
        .unwrap_or("default".to_string());

    let target_path;
    if flags.free.len() == 1 {
        let path = flags.free[0].clone();
        if path.starts_with("/") {
            target_path = path;
        } else {
            stderr(format!("invalid path: {}", path));
            print_usage(&mut io::stderr(), &program, &opts);
            process::exit(64);
        }
    } else {
        stderr("missing TARGET".to_string());
        print_usage(&mut io::stderr(), &program, &opts);
        process::exit(64);
    }

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();

    let proxy = {
        let handle = core.handle();
        let listener = TcpListener::bind(&listen_addr, &handle).unwrap();
        info!("Listening on {}", listen_addr);

        let namerd = NamerdLookup::periodic(&namerd_addr,
                                            namerd_interval,
                                            namerd_ns,
                                            target_path.clone());
        info!("Querying namerd for {} on {}", namerd_addr, target_path);

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
    thread: thread::JoinHandle<()>,
    endpoints: Arc<RwLock<HashMap<SocketAddr, Arc<RwLock<EndpointState>>>>>,
}

impl NamerdLookup {
    fn periodic(addr: &SocketAddr,
                period: Duration,
                namespace: String,
                name: String)
                -> NamerdLookup {
        let is_running = Arc::new(atomic::AtomicBool::new(true));
        let endpoints: Arc<RwLock<HashMap<SocketAddr, Arc<RwLock<EndpointState>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let thread = {
            let is_running = is_running.clone();
            let endpoints = endpoints.clone();
            let addr = addr.clone();
            let url = {
                let base = format!("http://{}:{}/api/1/resolve/{}",
                                   addr.ip(),
                                   addr.port().to_string(),
                                   namespace);
                Url::parse_with_params(&base, &[("path", &name)]).unwrap()
            };
            thread::Builder::new()
                .name(format!("namerd-{}-{}", addr.ip(), addr.port()).to_string())
                .spawn(move || {
                    let client = Client::new();
                    while is_running.load(atomic::Ordering::Relaxed) {
                        match client.get(url.clone()).send() {
                            Err(e) => stderr(format!("Failed to fetch addresses: {}", e)),
                            Ok(rsp) => {
                                if rsp.status == StatusCode::Ok {
                                    let parsed: json::Result<NamerdResponse> =
                                        json::from_reader(rsp);
                                    match parsed {
                                        Err(e) => {
                                            stderr(format!("Failed to parse response: {}", e))
                                        }
                                        Ok(rsp) => {
                                            // TODO extract weights from meta.
                                            let addrs = rsp.addrs
                                                .iter()
                                                .map(|na| {
                                                    let ip = na.ip.parse().unwrap();
                                                    SocketAddr::new(ip, na.port)
                                                })
                                                .collect::<HashSet<SocketAddr>>();
                                            let mut eps = endpoints.write().unwrap();
                                            let rm_keys = eps.keys()
                                                .filter_map(|&a| if addrs.contains(&a) {
                                                    None
                                                } else {
                                                    Some(a.clone())
                                                })
                                                .collect::<HashSet<SocketAddr>>();
                                            for k in rm_keys.iter() {
                                                println!("removing {:?}", k);
                                                eps.remove(&k);
                                            }
                                            for addr in addrs.iter() {
                                                if !eps.contains_key(&addr) {
                                                    println!("Adding {}", addr);
                                                    eps.insert(addr.clone(),
                                                               Arc::new(RwLock::new(EndpointState {
                                                                   weight: 1.0,
                                                                   load: 0.0,
                                                               })));
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    stderr(format!("Failed to fetch addresses: {}", rsp.status));
                                }
                            }
                        }
                        thread::sleep(period);
                    }
                })
                .unwrap()
        };
        NamerdLookup {
            is_running: is_running,
            thread: thread,
            endpoints: endpoints,
        }
    }
}

impl Drop for NamerdLookup {
    fn drop(&mut self) {
        info!("shutting down namerd");
        self.is_running.store(false, atomic::Ordering::SeqCst);
        // self.thread.join().unwrap();
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

impl Endpointer for NamerdLookup {
    type Endpoint = Endpoint;

    fn endpoint(&self) -> Endpoint {
        let endpoints = self.endpoints.read().unwrap();
        let addrs: Vec<(&SocketAddr, &Arc<RwLock<EndpointState>>)> = endpoints.iter().collect();

        // TODO choose an endpoint more intelligently.
        // E.g.
        // - if there are more nodes, choose two distinct nodes (at random)
        // - choose based on weight and load
        let (addr, state): (&SocketAddr, &Arc<RwLock<EndpointState>>) =
            *thread_rng().choose(&addrs).unwrap();
        let addr = addr.clone();
        let state = (*state).clone();

        // XXX currently we use a simple load metric (# active conns).
        // This load metric should be pluggable to account for
        // throughput, etc.
        {
            let mut state = state.write().unwrap();
            state.load += 1.0;
            debug!("Using {} {:?}", addr, *state);
        }
        Endpoint {
            addr: addr,
            state: state,
        }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        let mut state = self.state.write().unwrap();
        assert!(state.load >= 1.0);
        state.load -= 1.0;
    }
}

#[derive(Debug, Deserialize)]
struct NamerdResponse {
    #[serde(rename = "type")]
    kind: String,
    addrs: Vec<NamerdAddr>,
    meta: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct NamerdAddr {
    ip: String,
    port: u16,
    meta: HashMap<String, String>,
}
