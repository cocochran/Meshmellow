//! Conversation-history synchronization across the mesh.
//!
//! [`leader`] pushes history to every [`replica`] after each task; [`node`]
//! switches a peer between the two roles, [`election`] picks a replacement
//! leader after failover, and [`sim`] is an in-memory cluster for tests and the
//! demo. Design notes: `docs/history-sync.md`.

pub mod election;
pub mod history;
pub mod leader;
pub mod message;
pub mod node;
pub mod replica;
pub mod sim;
