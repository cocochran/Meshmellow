#!/usr/bin/env python3
"""Alternate RPC reply framing on one loaded model to reduce time-order bias."""

import argparse
import hashlib
import json
import os
import socket
import statistics
import subprocess
import time
from pathlib import Path

from benchmark import get_json, measure


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo-root", type=Path, required=True)
    parser.add_argument("--worker", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.demo_root.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-S",
           str(root / "ssh-control"), args.worker]

    def remote(command: str) -> None:
        subprocess.run([*ssh, command], check=True, timeout=30)

    def worker_start(experimental: bool) -> None:
        settings = ('DYLD_LIBRARY_PATH="$PWD/rpc-overlay-toggle" '
                    'GGML_RPC_COALESCE_TEST_FILE="$PWD/rpc-coalesce-control" '
                    if experimental else "")
        log_name = "rpc-reply-ab-experiment.log" if experimental else "rpc-reply-ab-restored.log"
        remote('cd "$HOME/llama-rpc-demo"; old=$(cat rpc.pid); '
               'kill -TERM "$old" 2>/dev/null || true; '
               'for n in {1..50}; do kill -0 "$old" 2>/dev/null || break; sleep 0.1; done; '
               'printf 0 > rpc-coalesce-control; chmod 600 rpc-coalesce-control; '
               'nohup env ' + settings + 'GGML_RPC_NO_RDMA=1 '
               './llama.cpp/build/bin/ggml-rpc-server -H 127.0.0.1 -p 50052 -c '
               f'> {log_name} 2>&1 < /dev/null & echo $! > rpc.pid; '
               'sleep 1; kill -0 "$(cat rpc.pid)"')

    payload = json.loads((root / "results/rpc-framing-01/control.json").read_text())["request"]
    report = {"test": "same-context alternating reply framing", "request": payload,
              "order": [0, 1, 1, 0, 1, 0, 0, 1], "runs": [],
              "worker_library_sha256": hashlib.sha256(
                  (root / "rpc-overlay/libggml-rpc.0.dylib").read_bytes()).hexdigest()}
    command = [str(root / "llama.cpp/build/bin/llama-server"), "-m",
               str(root / "models/Qwen_Qwen3.5-4B-Q4_K_M.gguf"),
               "--rpc", "127.0.0.1:50155", "--device", "RPC0,MTL0", "-ngl", "99",
               "--split-mode", "layer", "--tensor-split", "1,1", "-c", "8192", "-np", "1",
               "--reasoning", "off", "--cache-ram", "0", "--ctx-checkpoints", "0",
               "--host", "127.0.0.1", "--port", "8082", "-lv", "4"]
    process = None
    tunnel = None
    try:
        subprocess.run(["launchctl", "remove", "local.llama-rpc-demo"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        worker_start(True)
        tunnel_log = (args.output_dir / "tunnel.log").open("x")
        tunnel = subprocess.Popen([
            "ssh", "-N", "-o", "ControlMaster=no", "-o", "ControlPath=none",
            "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=30", "-L", "127.0.0.1:50155:127.0.0.1:50052",
            args.worker,
        ], stdout=tunnel_log, stderr=subprocess.STDOUT)
        tunnel_log.close()
        deadline = time.monotonic() + 20
        while True:
            if tunnel.poll() is not None:
                raise RuntimeError("Dedicated test tunnel exited")
            try:
                with socket.create_connection(("127.0.0.1", 50155), timeout=1):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("Dedicated tunnel did not open")
                time.sleep(0.2)
        with (args.output_dir / "server.log").open("x") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                       env={**os.environ, "GGML_RPC_NO_RDMA": "1"})
            deadline = time.monotonic() + 120
            while True:
                if process.poll() is not None:
                    raise RuntimeError("Test server exited")
                try:
                    if get_json("http://127.0.0.1:8082", "/health", 2).get("status") == "ok":
                        break
                except OSError:
                    pass
                if time.monotonic() > deadline:
                    raise TimeoutError("Model load exceeded budget")
                time.sleep(0.5)
            report["warmup"] = measure("http://127.0.0.1:8082", payload, 90)
            if report["warmup"]["status"] != "passed":
                raise RuntimeError("Warmup generation failed")
            for index, enabled in enumerate(report["order"]):
                remote(f'printf {enabled} > "$HOME/llama-rpc-demo/rpc-coalesce-control"')
                result = measure("http://127.0.0.1:8082", payload, 90)
                result.update(run=index + 1, reply_coalescing=bool(enabled))
                report["runs"].append(result)
                (args.output_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n")
                print(f"Run {index + 1}, coalescing={enabled}: "
                      f"{result.get('server_timings', {}).get('predicted_per_second')}", flush=True)
                if result["status"] != "passed":
                    raise RuntimeError("Generation failed; inspect saved result")
            report["summary"] = {}
            for enabled in [False, True]:
                runs = [run for run in report["runs"] if run["reply_coalescing"] == enabled]
                report["summary"][str(enabled)] = {
                    "median_tokens_per_second": statistics.median(
                        run["server_timings"]["predicted_per_second"] for run in runs),
                    "rates": [run["server_timings"]["predicted_per_second"] for run in runs]}
            report["all_outputs_identical"] = len({run["text"] for run in report["runs"]}) == 1
            (args.output_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report["summary"]), flush=True)
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if tunnel is not None:
            tunnel.terminate()
            try:
                tunnel.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tunnel.kill()
                tunnel.wait()
        worker_start(False)
        subprocess.run(["bash", str(root / "start.sh")], check=True, timeout=30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
