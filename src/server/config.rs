use super::server::{self, Server};
use std::net;


#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum ServerConfig {
    #[serde(rename = "io.l5d.tcp")]
    Tcp { addr: net::SocketAddr },

    // TODO support cypher suites
    // TODO support client validation
    // TODO supoprt persistence?
    #[serde(rename = "io.l5d.tls", rename_all = "camelCase")]
    Tls {
        addr: net::SocketAddr,
        alpn_protocols: Option<Vec<String>>,
        default_identity: Option<TlsServerIdentityConfig>,
        identities: Option<HashMap<String, TlsServerIdentityConfig>>,
    },
}

impl ServerConfig {
    fn mk_server(router: Router, buf: Rc<RefCell<Vec<u8>>>) -> Server {
        // srv_metrics: metrics.scope("srv", listen_addr.into()),
        // metrics: metrics,
        server::new(router, buf)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsServerIdentityConfig {
    pub certs: Vec<String>,
    pub private_key: String,
}
