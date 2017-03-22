//! Namerd Endpointer

use bytes::{Buf, BufMut, IntoBuf, Bytes, BytesMut};
use futures::{Future, Stream, future};
use hyper::{Body, Chunk, Client};
use hyper::client::Connect;
use hyper::status::StatusCode;
use std::{f32, net, time};
use std::collections::HashMap;
use serde_json as json;
use tokio_timer::Timer;
use url::Url;

#[derive(Debug)]
pub struct NamerdError(String);

type AddrsFuture = Box<Future<Item = Vec<::WeightedAddr>, Error = NamerdError>>;
type AddrsStream = Box<Stream<Item = Vec<::WeightedAddr>, Error = NamerdError>>;

/// Make a Resolver that periodically polls namerd to resolve a name
/// to a set of addresses.
pub fn resolve<C: Connect>(client: Client<C>,
                           addr: net::SocketAddr,
                           period: time::Duration,
                           namespace: &str,
                           target: &str)
                           -> AddrsStream {
    let url = {
        let base = format!("http://{}:{}/api/1/resolve/{}",
                           addr.ip(),
                           addr.port().to_string(),
                           namespace);
        Url::parse_with_params(&base, &[("path", &target)]).unwrap()
    };

    let init = request(&client, url.clone());
    let updates = Timer::default()
        .interval(period)
        .map_err(|e| NamerdError(format!("timer error: {}", e)))
        .then(move |_| request(&client, url.clone()));

    Box::new(init.into_stream().chain(updates))
}

fn request<C: Connect>(client: &Client<C>, url: Url) -> AddrsFuture {
    debug!("Polling namerd at {}", url.to_string());
    let rsp = client.get(url)
        .map_err(|e| NamerdError(format!("request failed: {}", e)))
        .and_then(|rsp| match *rsp.status() {
            StatusCode::Ok => parse_body(rsp.body()),
            status => invalid_rsp(status),
        });
    Box::new(rsp)
}

fn invalid_rsp(status: StatusCode) -> AddrsFuture {
    trace!("namerd error: {}", status);
    future::err(NamerdError(format!("unexpected response status: {}", status))).boxed()
}

fn parse_body(body: Body) -> AddrsFuture {
    trace!("parsing namerd response");
    body.collect()
        .map_err(|e| NamerdError(format!("failed to read response: {}", e)))
        .and_then(|chunks| parse_chunks(&chunks))
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

fn parse_chunks(chunks: &[Chunk]) -> Result<Vec<::WeightedAddr>, NamerdError> {
    let r = to_buf(chunks).into_buf().reader();
    let result: json::Result<NamerdResponse> = json::from_reader(r);
    match result {
        Err(e) => Err(NamerdError(format!("parse error: {}", e))),
        Ok(ref nrsp) if nrsp.kind == "bound" => Ok(to_weighted_addrs(&nrsp.addrs)),
        Ok(_) => Ok(vec![]),
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
