#!/usr/bin/env python3
"""Test an opt-in RPC library overlay on the existing two-Mac demo."""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

from benchmark import get_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo-root", type=Path, required=True)
    parser.add_argument("--worker", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=False)
    root = args.demo_root.resolve()
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-S",
           str(root / "ssh-control"), args.worker]

    def start_worker(coalesce: bool, original: bool = False) -> None:
        overlay = "" if original else 'DYLD_LIBRARY_PATH="$PWD/rpc-overlay" '
        remote = (
            'cd "$HOME/llama-rpc-demo"; '
            'old=$(cat rpc.pid); kill -TERM "$old" 2>/dev/null || true; '
            'for n in {1..50}; do kill -0 "$old" 2>/dev/null || break; sleep 0.1; done; '
            'nohup env ' + overlay + f'GGML_RPC_COALESCE={int(coalesce)} '
            'GGML_RPC_NO_RDMA=1 GGML_RPC_DEBUG=1 '
            './llama.cpp/build/bin/ggml-rpc-server -H 127.0.0.1 -p 50052 -c '
            '> rpc-frame-worker.log 2>&1 < /dev/null & echo $! > rpc.pid; '
            'sleep 1; kill -0 "$(cat rpc.pid)"'
        )
        subprocess.run([*ssh, remote], check=True, timeout=30)

    configs = [("control", False, False), ("client", True, False),
               ("worker", False, True), ("both", True, True),
               ("control-repeat", False, False)]
    summaries = []
    try:
        subprocess.run(["launchctl", "remove", "local.llama-rpc-demo"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for name, client, worker in configs:
            print(f"Starting {name}: client={client}, worker={worker}", flush=True)
            start_worker(worker)
            env = {**os.environ, "GGML_RPC_COALESCE": str(int(client)),
                   "GGML_RPC_NO_RDMA": "1", "GGML_RPC_DEBUG": "0",
                   "DYLD_LIBRARY_PATH": str(root / "rpc-overlay")}
            smoke = subprocess.run([str(root / "rpc-frame-smoke"), "127.0.0.1:50053"],
                                   env=env, capture_output=True, text=True, timeout=60)
            (args.output_dir / f"{name}-smoke.txt").write_text(smoke.stdout + smoke.stderr)
            smoke.check_returncode()
            log_path = args.output_dir / f"{name}.log"
            result_path = args.output_dir / f"{name}.json"
            command = [str(root / "llama.cpp/build/bin/llama-server"), "-m",
                       str(root / "models/Qwen_Qwen3.5-4B-Q4_K_M.gguf"),
                       "--rpc", "127.0.0.1:50053", "--device", "RPC0,MTL0",
                       "-ngl", "99", "--split-mode", "layer", "--tensor-split", "1,1",
                       "-c", "8192", "-np", "1", "--reasoning", "off",
                       "--cache-ram", "0", "--ctx-checkpoints", "0",
                       "--host", "127.0.0.1", "--port", "8082", "-lv", "4"]
            record = {"configuration": name, "client_coalescing": client,
                      "worker_coalescing": worker, "server_command": command}
            with log_path.open("x") as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                           env={**env, "DYLD_PRINT_LIBRARIES": "1"})
                try:
                    deadline = time.monotonic() + 120
                    while True:
                        if process.poll() is not None:
                            raise RuntimeError(f"Server exited; inspect {log_path}")
                        try:
                            if get_json("http://127.0.0.1:8082", "/health", 2).get("status") == "ok":
                                break
                        except OSError:
                            pass
                        if time.monotonic() > deadline:
                            raise TimeoutError("Model loading exceeded 120 seconds")
                        time.sleep(0.5)
                    if "rpc-overlay/libggml-rpc.0.dylib" not in log_path.read_text():
                        raise RuntimeError("Experimental library was not loaded")
                    subprocess.run([
                        sys.executable, str(Path(__file__).with_name("benchmark.py")),
                        "--base-url", "http://127.0.0.1:8082", "--model", str(root / "models/Qwen_Qwen3.5-4B-Q4_K_M.gguf"),
                        "--llama-cpp", str(root / "rpc-experiment"), "--transport", "ssh-rpc",
                        "--server-log", str(log_path), "--runs", "3", "--max-tokens", "128",
                        "--timeout", "90", "--output", str(result_path),
                    ], check=True, timeout=300)
                    result = json.loads(result_path.read_text())
                    record.update(result["summary"])
                    record["decode_tokens_per_second"] = [
                        run["server_timings"]["predicted_per_second"] for run in result["runs"]]
                    record["median_decode_tokens_per_second"] = statistics.median(record["decode_tokens_per_second"])
                    record["status"] = result["status"]
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            summaries.append(record)
            (args.output_dir / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
            print(json.dumps(record), flush=True)
    finally:
        print("Restoring original worker and demo", flush=True)
        start_worker(False, original=True)
        subprocess.run(["bash", str(root / "start.sh")], check=True, timeout=30)
    return 0


if __name__ == "__main__":
    sys.exit(main())
