//! Test stand-in for llama.cpp's `ggml-rpc-server`: a plain TCP echo server.
//!
//! Usage: `fake_rpc_server -H <host> -p <port> [--exit-after-ms <n>]`
//!
//! Takes the same `-H`/`-p` flags as `ggml-rpc-server` and ignores any other argument, so it
//! can be launched with real server flags. With `--exit-after-ms`, the process exits with a
//! failure status after that many milliseconds, to exercise restarts.

use std::io;
use std::net::{TcpListener, TcpStream};
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 50052;

/// Exit code used for the simulated crash, distinct from argument errors (1).
const CRASH_EXIT_CODE: i32 = 3;

struct Args {
    host: String,
    port: u16,
    exit_after: Option<Duration>,
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("fake_rpc_server: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(delay) = args.exit_after {
        thread::spawn(move || {
            thread::sleep(delay);
            std::process::exit(CRASH_EXIT_CODE);
        });
    }
    match serve(&args.host, args.port) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fake_rpc_server: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args {
        host: DEFAULT_HOST.to_string(),
        port: DEFAULT_PORT,
        exit_after: None,
    };
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "-H" | "--host" => parsed.host = flag_value(&mut args, &flag)?,
            "-p" | "--port" => parsed.port = parse_flag_value(&mut args, &flag)?,
            "--exit-after-ms" => {
                let millis = parse_flag_value(&mut args, &flag)?;
                parsed.exit_after = Some(Duration::from_millis(millis));
            }
            _ => {}
        }
    }
    Ok(parsed)
}

fn flag_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_flag_value<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<T, String> {
    let value = flag_value(args, flag)?;
    value
        .parse()
        .map_err(|_| format!("invalid value {value:?} for {flag}"))
}

/// Accepts connections forever, echoing each one on its own thread.
fn serve(host: &str, port: u16) -> io::Result<()> {
    let listener = TcpListener::bind((host, port))?;
    println!("fake_rpc_server listening on {}", listener.local_addr()?);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    if let Err(error) = echo(stream) {
                        eprintln!("fake_rpc_server: connection failed: {error}");
                    }
                });
            }
            Err(error) => eprintln!("fake_rpc_server: accept failed: {error}"),
        }
    }
    Ok(())
}

fn echo(stream: TcpStream) -> io::Result<()> {
    let mut reader = stream.try_clone()?;
    let mut writer = stream;
    io::copy(&mut reader, &mut writer)?;
    Ok(())
}
