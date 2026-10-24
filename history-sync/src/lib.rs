//! Prototype of conversation-history synchronization for MeshThing.
//!
//! After each completed task the main process ([`Leader`]) pushes history to
//! every worker ([`Replica`]) in the mesh, for any number of peers. Peers that
//! fall behind get a delta ([`SyncMessage::Append`]); peers that are new, have
//! gaps, or just saw a failover get a full [`SyncMessage::Snapshot`]. Each
//! leader has an epoch, so if the prompted device drops out a replica can be
//! [`elect`]ed and [`Replica::promote`]d, and the old main process is rejected
//! if it comes back.
//!
//! The crate is transport-agnostic: callers deliver [`SyncMessage`]s (encoded
//! with [`SyncMessage::to_bytes`]) over the P2P layer. [`sim::Cluster`] is an
//! in-memory stand-in for that layer.

mod election;
mod history;
mod leader;
mod message;
mod node;
mod replica;
pub mod sim;

pub use election::{Candidate, Elected, elect};
pub use history::{Entry, Epoch, History, NodeId, Role, Seq};
pub use leader::Leader;
pub use message::{Outgoing, SyncMessage};
pub use node::Node;
pub use replica::Replica;
