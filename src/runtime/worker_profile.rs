//! Optional loopback profiling controls inside the native worker's runtime owner.

use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, ensure};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::runtime::profiling::ProfileManager;

const API_STOP_GRACE: Duration = Duration::from_secs(5);

struct CancelProfilesOnDrop(ProfileManager);

impl Drop for CancelProfilesOnDrop {
    fn drop(&mut self) {
        self.0.request_shutdown();
    }
}

/// A bound, trusted local API. Merely binding it does not start a profile.
pub struct WorkerProfileControl {
    listener: TcpListener,
    manager: ProfileManager,
}

impl WorkerProfileControl {
    /// The supplied manager must use the worker's actual admission owner.
    /// `ProfileManager::new/from_file` and native core workers share the default
    /// process owner. Embedders using a custom gate must pass that same gate.
    pub async fn bind(manager: ProfileManager, address: SocketAddr) -> anyhow::Result<Self> {
        validate_address(address)?;
        let listener = TcpListener::bind(address)
            .await
            .with_context(|| format!("bind worker profile API {address}"))?;
        Ok(Self { listener, manager })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Run both services until the worker, API or host stop signal completes.
    /// The worker future must cancel its sessions when dropped (the native shard
    /// and ring workers do). Its admitted child leases survive through reaping.
    pub async fn run(
        self,
        worker: impl Future<Output = anyhow::Result<()>>,
        stop: impl Future<Output = anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        let Self { listener, manager } = self;
        let _cancel_on_drop = CancelProfilesOnDrop(manager.clone());
        info!(address = %listener.local_addr()?, "worker profile API listening (loopback)");
        let (stop_api, stopped) = oneshot::channel();
        let server = axum::serve(listener, manager.router())
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .into_future();
        tokio::pin!(server);
        let mut server_finished = false;
        let result = {
            // Keep worker ownership in this block so every exit path drops its
            // accept loop/sessions before waiting for profile cleanup below.
            tokio::pin!(worker);
            tokio::select! {
                result = &mut worker => result,
                result = &mut server => {
                    server_finished = true;
                    match result {
                        Ok(()) => Err(anyhow::anyhow!("worker profile API stopped unexpectedly")),
                        Err(error) => Err(error).context("worker profile API failed"),
                    }
                },
                result = stop => result,
            }
        };
        // Closes the manager to new starts before cancelling any live helper.
        let cleanup = manager.shutdown().await;
        let _ = stop_api.send(());
        if !server_finished {
            match timeout(API_STOP_GRACE, &mut server).await {
                Ok(Err(error)) => warn!(%error, "worker profile API shutdown failed"),
                Err(_) => warn!("worker profile API shutdown grace elapsed"),
                Ok(Ok(())) => {}
            }
        }
        if let Err(error) = &cleanup {
            warn!(%error, "worker profile cleanup failed; compute remains fenced");
        }
        result.and(cleanup)
    }
}

pub fn validate_address(address: SocketAddr) -> anyhow::Result<()> {
    ensure!(
        address.ip().is_loopback(),
        "worker profile API must bind to loopback"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_control_refuses_non_loopback_before_binding() {
        for address in ["0.0.0.0:8091", "192.0.2.1:8091", "[::]:8091"] {
            assert!(validate_address(address.parse().unwrap()).is_err());
        }
        for address in ["127.0.0.1:8091", "[::1]:0"] {
            validate_address(address.parse().unwrap()).unwrap();
        }
    }
}
