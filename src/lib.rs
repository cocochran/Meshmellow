//! MeshThing: a peer-to-peer mesh that splits one LLM across devices.

pub mod api;
pub mod evidence;
pub mod history;
pub mod jobs;
pub mod launch;
pub mod leader;
pub mod network;
pub mod node;
pub mod placement;
pub mod planning;
pub mod process;
pub mod protocol;
pub mod rpc_bridge;
pub mod rpc_plan;
pub mod verification;
pub mod worker;
