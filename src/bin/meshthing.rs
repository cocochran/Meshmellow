//! Terminal launcher for the bundled browser dashboard; never starts inference workers.
use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use std::{
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, ValueEnum)]
enum Source {
    Core,
    LiveDemo,
}
#[derive(Parser)]
#[command(about = "Start the MeshThing web dashboard and open it in your browser")]
struct Options {
    /// Local core or diagnostic state API.
    #[arg(long, value_enum, default_value = "core")]
    source: Source,
    /// Loopback HTTP runtime origin. Defaults to core :8080 or live-demo :8095.
    #[arg(long)]
    runtime: Option<String>,
    /// Dashboard port; zero chooses an available loopback port.
    #[arg(long, default_value_t = 0)]
    port: u16,
    /// Print the URL without launching a browser.
    #[arg(long)]
    no_open: bool,
}
struct Server(Option<Child>);
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn open_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut c = Command::new("open");
        c.arg(url);
        c
    };
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };
    #[cfg(windows)]
    let mut command = {
        let mut c = Command::new("rundll32.exe");
        c.args(["url.dll,FileProtocolHandler", url]);
        c
    };
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let options = Options::parse();
    let root = std::env::current_exe()?
        .parent()
        .context("executable has no parent")?
        .to_path_buf();
    let node: PathBuf = root
        .join("runtime")
        .join(if cfg!(windows) { "node.exe" } else { "node" });
    let script = root.join("web/scripts/serve.mjs");
    if !node.is_file() || !script.is_file() {
        bail!(
            "Missing bundled web resources. Extract the complete platform archive, keeping runtime/ and web/ beside this executable."
        )
    }
    let listener =
        TcpListener::bind(("127.0.0.1", options.port)).context("dashboard port is unavailable")?;
    let address = listener.local_addr()?;
    drop(listener);
    let source = match options.source {
        Source::Core => "core",
        Source::LiveDemo => "live-demo",
    };
    let runtime = options.runtime.unwrap_or_else(|| match options.source {
        Source::Core => "http://127.0.0.1:8080".into(),
        Source::LiveDemo => "http://127.0.0.1:8095".into(),
    });
    let mut command = Command::new(node);
    command
        .arg(script)
        .args([
            "--port",
            &address.port().to_string(),
            "--source",
            source,
            "--runtime",
            &runtime,
        ])
        .stdin(Stdio::null());
    let mut server = Server(Some(
        command
            .spawn()
            .context("could not launch bundled web server")?,
    ));
    let start = Instant::now();
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            break;
        }
        if server.0.as_mut().unwrap().try_wait()?.is_some() {
            bail!("web server exited during startup; see its terminal output above")
        }
        if start.elapsed() > Duration::from_secs(15) {
            bail!("web server did not become ready within15seconds")
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let url = format!("http://{address}");
    println!(
        "MeshThing dashboard: {url}\nPress Ctrl+C to stop the dashboard. Core and inference workers remain running."
    );
    if !options.no_open
        && let Err(error) = open_browser(&url)
    {
        eprintln!("Could not open a browser ({error}); open {url} manually.")
    }
    loop {
        tokio::select! {
            result=tokio::signal::ctrl_c()=>{result?;break},
            _=tokio::time::sleep(Duration::from_millis(300))=>{
                if let Some(status)=server.0.as_mut().unwrap().try_wait()?{bail!("web server exited: {status}")}
            }
        }
    }
    Ok(())
}
