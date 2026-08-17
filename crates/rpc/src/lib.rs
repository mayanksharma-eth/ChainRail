//! RPC gateway: health-aware multi-endpoint JSON-RPC with failover.
//!
//! ChainRail talks to blockchains only through this crate. Third-party RPC
//! providers are treated as untrusted and unreliable: they rate-limit, they
//! return stale data, they lie about block numbers during incidents, and they
//! go down. See `docs/threat-model.md#compromised-rpc-provider`.

pub mod gateway;
pub mod health;

pub use gateway::{EndpointStatus, Idempotency, RpcGateway, RpcRegistry};
pub use health::{BreakerState, EndpointHealth, HealthConfig};
