use super::WeightedAddr;
use super::namer::namerd::Namerd;
use std::time::Duration;
use tacho;
use url::{self, Url};
use std::net;
use std::collections::HashMap;
use std::iter::FromIterator;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidPeriod(u64),
    InvalidBaseUrl(String, url::ParseError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NamerdConfig {
    pub base_url: String,
    pub period_secs: u64,
    pub namespace: String,
}

impl NamerdConfig {
    pub fn into_namerd(self, metrics: &tacho::Scope) -> Result<Namerd> {
        if self.period_secs == 0 {
            return Err(Error::InvalidPeriod(self.period_secs));
        }
        let period = Duration::from_secs(self.period_secs);

        if let Err(e) = Url::parse(&self.base_url) {
            return Err(Error::InvalidBaseUrl(self.base_url, e));
        }

        let metrics = metrics.clone().prefixed("resolver").labeled(
            "namespace".into(),
            self.namespace.clone(),
        );
        let namerd = Namerd::new(self.base_url, period, self.namespace, metrics);
        Ok(namerd)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConstantConfig {
    addrs: HashMap<String, Vec<Addr>>
}

impl ConstantConfig {
    pub fn into_namer(mut self) -> Result<HashMap<String, Vec<WeightedAddr>>> {
        let weighted_iter = self.addrs.drain().map(|(path, addrs)| (path, to_weighted_addrs(&addrs)));
        let weighted_addrs = HashMap::from_iter(weighted_iter);
        Ok(weighted_addrs)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Addr {
    ip: String,
    port: u16,
    weight: Option<f64>,
}

fn to_weighted_addrs(addrs: &[Addr]) -> Vec<WeightedAddr> {
    // We never intentionally clear the EndpointMap.
    let mut dsts: Vec<WeightedAddr> = Vec::new();
    let mut sum = 0.0;
    for na in addrs {
        let addr = net::SocketAddr::new(na.ip.parse().unwrap(), na.port);
        let w = na.weight.unwrap_or(1.0);
        sum += w;
        dsts.push(WeightedAddr::new(addr, w));
    }
    // Normalize weights on [0.0, 0.1].
    for dst in &mut dsts {
        dst.weight /= sum;
    }
    dsts
}

