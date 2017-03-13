use serde_json;
use serde_yaml;
use std::{io, net, time};

pub fn from_str(mut txt: &str) -> io::Result<AppConfig> {
    txt = txt.trim_left();
    if txt.starts_with('{') {
        serde_json::from_str(txt).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    } else {
        serde_yaml::from_str(txt).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppConfig {
    pub proxies: Vec<ProxyConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProxyConfig {
    pub servers: Vec<ServerConfig>,
    pub namerd: NamerdConfig,
    pub client: Option<ClientConfig>,
    pub buffer_size: Option<usize>,
    pub max_waiters: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerConfig {
    pub addr: net::SocketAddr,
    pub tls: Option<TlsServerConfig>,
}

/// TODO
#[derive(Serialize, Deserialize, Debug)]
pub struct TlsServerConfig();

#[derive(Serialize, Deserialize, Debug)]
pub struct NamerdConfig {
    pub addr: net::SocketAddr,
    pub path: String,
    pub namespace: Option<String>,
    pub interval: Option<time::Duration>,
}

/// TODO
#[derive(Serialize, Deserialize, Debug)]
pub struct ClientConfig();

#[test]
fn parse_simple_yaml() {
    let yaml = "
proxies:
  - servers:
      - addr: 0.0.0.0:4321
      - addr: 0.0.0.0:4322
    namer:
      addr: 127.0.0.1:4180
      path: /svc/default
    ";
    let app = AppConfig::parse(yaml).unwrap();
    assert!(app.proxies.len() == 1);
}

#[test]
fn parse_simple_json() {
    let json = "{\"proxies\": [{\"servers\": [{\"addr\": \"0.0.0.0:4321\"}, {\"addr\": \
                \"0.0.0.0:4322\"}], \"namer\": {\"addr\": \"127.0.0.1:4180\", \"path\": \
                \"/svc/default\"}}]}";
    let app = AppConfig::parse(json).unwrap();
    assert!(app.proxies.len() == 1);
}
