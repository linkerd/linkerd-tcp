use app::config::TlsServerIdentity as IdentityConfig;
use rustls::{Certificate, ResolvesServerCert, SignatureScheme, sign};
use rustls::internal::pemfile;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

pub fn new(ids: &Option<HashMap<String, TlsServerIdentity>>,
           def: &Option<TlsServerIdentity>)
           -> io::Result<Sni> {
    if def.is_none() && ids.map(|ids| ids.is_empty()).unwrap_or(true) {
        return io::Error::new(io::ErrorKind::Other, "No TLS server identities specified");
    }
    Sni {
        default: default.as_ref().map(|c| ServerIdentity::load(c)),
        identities: {
            let mut ids = HashMap::new();
            if let Some(ref identities) = *identities {
                for (k, c) in identities {
                    let k = k.clone();
                    ids.insert(k, ServerIdentity::load(c));
                }
            }
            Arc::new(ids)
        },
    }

}

pub struct Sni {
    default: Option<ServerIdentity>,
    identities: Arc<HashMap<String, ServerIdentity>>,
}

fn to_chain_and_signer(id: &ServerIdentity) -> sign::CertChainAndSigner {
    (id.certs.clone(), id.key.clone())
}

impl ResolvesServerCert for Sni {
    fn resolve(&self,
               server_name: Option<&str>,
               _sigschemes: &[SignatureScheme])
               -> Option<sign::CertChainAndSigner> {
        debug!("finding cert resolver for {:?}", server_name);
        server_name
            .and_then(|n| {
                          debug!("found match for {}", n);
                          self.identities.get(n)
                      })
            .or_else(|| {
                         debug!("reverting to default");
                         self.default.as_ref()
                     })
            .map(to_chain_and_signer)
    }
}

struct ServerIdentity {
    certs: Vec<Certificate>,
    key: Arc<Box<sign::Signer>>,
}

impl ServerIdentity {
    fn load(c: &IdentityConfig) -> ServerIdentity {
        let mut certs = vec![];
        for p in &c.certs {
            certs.append(&mut load_certs(p));
        }
        ServerIdentity {
            certs: certs,
            key: Arc::new(load_private_key(&c.private_key)),
        }
    }
}

// from rustls example
fn load_certs(filename: &str) -> Vec<Certificate> {
    let certfile = File::open(filename).expect("cannot open certificate file");
    let mut r = BufReader::new(certfile);
    pemfile::certs(&mut r).unwrap()
}

// from rustls example
fn load_private_key(filename: &str) -> Box<sign::Signer> {
    let keyfile = File::open(filename).expect("cannot open private key file");
    let mut r = BufReader::new(keyfile);
    let keys = pemfile::rsa_private_keys(&mut r).unwrap();
    assert_eq!(keys.len(), 1);
    Box::new(sign::RSASigner::new(&keys[0]).expect("Invalid RSA private key"))
}
