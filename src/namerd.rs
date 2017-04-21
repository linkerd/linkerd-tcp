//! Namerd Endpointer

use bytes::{Buf, BufMut, IntoBuf, Bytes, BytesMut};
use futures::{Future, Stream, future};
use hyper::{Body, Chunk, Client};
use hyper::client::Connect;
use hyper::status::StatusCode;
use serde_json as json;
use std::{f32, net, time};
use std::collections::HashMap;
use std::rc::Rc;
use tacho::{self, Timing};
use tokio_timer::Timer;
use url::Url;

#[derive(Debug)]
pub struct NamerdError(String);

type AddrsFuture = Box<Future<Item = Option<Vec<::WeightedAddr>>, Error = ()>>;
type AddrsStream = Box<Stream<Item = Vec<::WeightedAddr>, Error = ()>>;

#[derive(Clone)]
struct Stats {
    request_latency_ms: tacho::Stat,
    success_count: tacho::Counter,
    failure_count: tacho::Counter,
}
impl Stats {
    fn new(metrics: tacho::Scope) -> Stats {
        let metrics = metrics.labeled("service".into(), "namerd".into());
        Stats {
            request_latency_ms: metrics.stat("namerd_request_latency_ms".into()),
            success_count: metrics.counter("namerd_success_count".into()),
            failure_count: metrics.counter("namerd_failure_count".into()),
        }
    }
}

/// Make a Resolver that periodically polls namerd to resolve a name
/// to a set of addresses.
///
/// The returned stream never completes.
pub fn resolve<C>(base_url: &str,
                  client: Client<C>,
                  period: time::Duration,
                  namespace: &str,
                  target: &str,
                  metrics: tacho::Scope)
                  -> AddrsStream
    where C: Connect
{
    let url = {
        let base = format!("{}/api/1/resolve/{}", base_url, namespace);
        Url::parse_with_params(&base, &[("path", &target)]).expect("invalid namerd url")
    };
    let stats = Stats::new(metrics);
    let client = Rc::new(client);
    let init = request(client.clone(), url.clone(), stats.clone());
    let updates = Timer::default()
        .interval(period)
        .then(move |_| request(client.clone(), url.clone(), stats.clone()));
    Box::new(init.into_stream().chain(updates).filter_map(|opt| opt))
}


fn request<C: Connect>(client: Rc<Client<C>>, url: Url, stats: Stats) -> AddrsFuture {
    debug!("Polling namerd at {}", url.to_string());
    let mut stats = stats;
    let rsp = future::lazy(|| Ok(tacho::Timing::start())).and_then(move |start_t| {
        client
            .get(url)
            .then(|rsp| match rsp {
                      Ok(rsp) => {
                          match *rsp.status() {
                              StatusCode::Ok => parse_body(rsp.body()),
                              status => {
                info!("error: bad response: {}", status);
                future::ok(None).boxed()
            }
                          }
                      }
                      Err(e) => {
                error!("failed to read response: {}", e);
                future::ok(None).boxed()
            }
                  })
            .then(move |rsp| {
                stats.request_latency_ms.add(start_t.elapsed_ms());
                if rsp.as_ref().ok().and_then(|r| r.as_ref()).is_some() {
                    stats.success_count.incr(1);
                } else {
                    stats.failure_count.incr(1);
                }
                rsp
            })
    });
    Box::new(rsp)
}


fn parse_body(body: Body) -> AddrsFuture {
    trace!("parsing namerd response");
    body.collect()
        .then(|res| match res {
                  Ok(ref chunks) => Ok(parse_chunks(chunks)),
                  Err(e) => {
            info!("error: {}", e);
            Ok(None)
        }
              })
        .boxed()
}

fn bytes_in(chunks: &[Chunk]) -> usize {
    let mut sz = 0;
    for c in chunks {
        sz += (*c).len();
    }
    sz
}

fn to_buf(chunks: &[Chunk]) -> Bytes {
    let mut buf = BytesMut::with_capacity(bytes_in(chunks));
    for c in chunks {
        buf.put_slice(&*c)
    }
    buf.freeze()
}

fn parse_chunks(chunks: &[Chunk]) -> Option<Vec<::WeightedAddr>> {
    let r = to_buf(chunks).into_buf().reader();
    let result: json::Result<NamerdResponse> = json::from_reader(r);
    match result {
        Ok(ref nrsp) if nrsp.kind == "bound" => Some(to_weighted_addrs(&nrsp.addrs)),
        Ok(_) => Some(vec![]),
        Err(e) => {
            info!("error parsing response: {}", e);
            None
        }
    }
}

fn to_weighted_addrs(namerd_addrs: &[NamerdAddr]) -> Vec<::WeightedAddr> {
    // We never intentionally clear the EndpointMap.
    let mut weighted_addrs: Vec<::WeightedAddr> = Vec::new();
    for na in namerd_addrs {
        let addr = net::SocketAddr::new(na.ip.parse().unwrap(), na.port);
        let w = na.meta.endpoint_addr_weight.unwrap_or(1.0);
        weighted_addrs.push(::WeightedAddr(addr, w));
    }
    weighted_addrs
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
    meta: Meta,
}

#[derive(Debug, Deserialize)]
struct Meta {
    authority: Option<String>,

    #[serde(rename = "nodeName")]
    node_name: Option<String>,

    endpoint_addr_weight: Option<f32>,
}
