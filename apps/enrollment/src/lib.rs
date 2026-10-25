//! Native ABI adapter. All membership, transport and trust rules live in the core.
mod ffi;
pub use llm_cluster::mesh::{Node, Peer, membership, rendezvous};
