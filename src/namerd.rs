//! Namerd Endpointer

use hyper::Client;
use hyper::status::StatusCode;
use rand::{Rng, thread_rng};
use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;
use std::time::Duration;
use serde_json as json;
use std::thread;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{self, AtomicBool};
use url::Url;

#[derive(Debug, PartialEq)]
struct EndpointState {
    weight: f32,
    load: f64,
}

type EndpointMap = Arc<RwLock<HashMap<SocketAddr, Arc<RwLock<EndpointState>>>>>;

pub struct Endpointer {
    is_running: Arc<AtomicBool>,
    #[allow(dead_code)]
    thread: thread::JoinHandle<()>,
    endpoints: EndpointMap,
}

impl Endpointer {
    pub fn periodic(addr: &SocketAddr,
                    period: Duration,
                    namespace: String,
                    target: String)
                    -> Endpointer {
        let is_running = Arc::new(AtomicBool::new(true));
        let endpoints: EndpointMap = Arc::new(RwLock::new(HashMap::new()));

        let name = format!("namerd-{}-{}-{}",
                           addr.ip(),
                           addr.port().to_string(),
                           target);

        let base = format!("http://{}:{}/api/1/resolve/{}",
                           addr.ip(),
                           addr.port().to_string(),
                           namespace);

        let thread = {
            let is_running = is_running.clone();
            let endpoints = endpoints.clone();
            let url = Url::parse_with_params(&base, &[("path", &target)]).unwrap();
            thread::Builder::new()
                .name(name)
                .spawn(move || {
                    let client = Client::new();
                    while is_running.load(atomic::Ordering::Relaxed) {
                        match client.get(url.clone()).send() {
                            Err(e) => warn!("Error fetching addresses: {}", e),
                            Ok(rsp) => {
                                if rsp.status != StatusCode::Ok {
                                    warn!("Failed to fetch addresses: {}", rsp.status);
                                } else {
                                    let r: json::Result<NamerdResponse> = json::from_reader(rsp);
                                    match r {
                                        Err(e) => warn!("Failed to parse response: {}", e),
                                        Ok(rsp) => update(rsp.addrs, endpoints.clone()),
                                    }
                                }
                            }
                        }
                        thread::sleep(period);
                    }
                })
                .unwrap()
        };
        Endpointer {
            is_running: is_running,
            thread: thread,
            endpoints: endpoints,
        }
    }
}

fn update(namerd_addrs: Vec<NamerdAddr>, endpoints: EndpointMap) {
    // TODO weights
    let addrs = namerd_addrs.iter()
        .map(|na| {
            let ip = na.ip.parse().unwrap();
            SocketAddr::new(ip, na.port)
        })
        .collect::<HashSet<SocketAddr>>();

    let mut eps = endpoints.write().unwrap();

    // Figure out which endpoints have been removed. And then remove them.
    let rm_keys = eps.keys()
        .filter_map(|&a| if addrs.contains(&a) {
            None
        } else {
            Some(a.clone())
        })
        .collect::<HashSet<SocketAddr>>();
    for k in rm_keys.iter() {
        trace!("removing {:?}", k);
        eps.remove(&k);
    }

    // Finally, add new endpoints.
    for addr in addrs.iter() {
        if !eps.contains_key(&addr) {
            trace!("Adding {}", addr);
            let state = Arc::new(RwLock::new(EndpointState {
                weight: 1.0,
                load: 0.0,
            }));
            eps.insert(addr.clone(), state);
        }
    }
}

impl Drop for Endpointer {
    fn drop(&mut self) {
        info!("shutting down namerd");
        self.is_running.store(false, atomic::Ordering::SeqCst);
        // self.thread.join().unwrap();
    }
}

pub struct Endpoint {
    addr: SocketAddr,
    state: Arc<RwLock<EndpointState>>,
}

impl ::WithAddr for Endpoint {
    fn addr(&self) -> SocketAddr {
        self.addr.clone()
    }
}

impl ::Endpointer for Endpointer {
    type Endpoint = Endpoint;

    fn endpoint(&self) -> Endpoint {
        let endpoints = self.endpoints.read().unwrap();
        let addrs: Vec<(&SocketAddr, &Arc<RwLock<EndpointState>>)> = endpoints.iter().collect();

        // Select two nodes at random and choose the lesser-loaded node.
        // TODO Weighting.
        let (addr, state) = {
            let (addr0, state0): (&SocketAddr, &Arc<RwLock<EndpointState>>) =
                *thread_rng().choose(&addrs).unwrap();
            let (addr1, state1): (&SocketAddr, &Arc<RwLock<EndpointState>>) =
                *thread_rng().choose(&addrs).unwrap();
            let load0 = (*state0).read().unwrap().load;
            let load1 = (*state1).read().unwrap().load;
            trace!("Choosing between {} @{} and {} @{}",
                   addr0,
                   load0,
                   addr1,
                   load1);
            if load0 < load1 {
                (addr0.clone(), (*state0).clone())
            } else {
                (addr1.clone(), (*state1).clone())
            }
        };

        // XXX currently we use a simple load metric (# active conns).
        // This load metric should be pluggable to account for
        // throughput, etc.
        {
            let mut state = state.write().unwrap();
            state.load += 1.0;
            trace!("Using {} {:?}", addr, *state);
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
        trace!("releasing endpoint: {:?}", *state);
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
