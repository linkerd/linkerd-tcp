mod balancer;
mod config;
mod connector;
mod endpoint;
mod factory;

pub type DstConnection = super::Connection<EndpointCtx>;

pub use self::balancer::{Balancer, Connect};
pub use self::config::BalancerConfig;
pub use self::endpoint::EndpointCtx;
pub use self::factory::BalancerFactory;

/// A weighted concrete destination address.
#[derive(Clone, Debug)]
pub struct DstAddr {
    pub addr: ::std::net::SocketAddr,
    pub weight: f32,
}

impl DstAddr {
    pub fn new(addr: ::std::net::SocketAddr, weight: f32) -> DstAddr {
        DstAddr {
            addr: addr,
            weight: weight,
        }
    }
}
