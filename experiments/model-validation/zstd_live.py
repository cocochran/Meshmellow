#!/usr/bin/env python3
"""Compare opt-in zstd RPC responses on one loaded two-device model."""

import argparse
import hashlib
import json
import os
import re
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
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
           "-S", str(root / "ssh-control")]

    def remote(command: str) -> str:
        return subprocess.check_output([*ssh, args.worker, command], text=True, timeout=30).strip()

    forward = [*ssh, "-L", "127.0.0.1:50159:127.0.0.1:50054"]
    process = None
    forwarded = worker_started = False
    payload = json.loads((root / "results/rpc-framing-01/control.json").read_text())["request"]
    report = {"test": "same-context zstd level 1", "order": [0, 1, 1, 0, 1, 0, 0, 1],
              "request": payload, "runs": [], "protocol": "private experimental major 9",
              "library_sha256": hashlib.sha256(
                  (root / "rpc-overlay-zstd/libggml-rpc.0.dylib").read_bytes()).hexdigest()}
    try:
        subprocess.run(["launchctl", "remove", "local.llama-rpc-demo"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        remote('cd "$HOME/llama-rpc-demo"; printf 1 > zstd-control; chmod 600 zstd-control; '
               'nohup env DYLD_LIBRARY_PATH="$PWD/rpc-overlay-zstd" '
               'GGML_RPC_ZSTD=1 GGML_RPC_ZSTD_TEST_FILE="$PWD/zstd-control" '
               'GGML_RPC_NO_RDMA=1 GGML_RPC_DEBUG=1 '
               './llama.cpp/build/bin/ggml-rpc-server -H 127.0.0.1 -p 50054 -c '
               '> rpc-zstd.log 2>&1 < /dev/null & echo $! > rpc-zstd.pid; '
               'for n in {1..50}; do '
               'lsof -a -p "$(cat rpc-zstd.pid)" -iTCP:50054 -sTCP:LISTEN >/dev/null && exit 0; '
               'kill -0 "$(cat rpc-zstd.pid)" 2>/dev/null || exit 1; sleep 0.2; done; exit 1')
        worker_started = True
        subprocess.run([*forward, "-O", "forward", args.worker], check=True, timeout=30)
        forwarded = True
        env = {**os.environ, "GGML_RPC_ZSTD": "1", "GGML_RPC_NO_RDMA": "1",
               "GGML_RPC_BATCH_WRITES": "0", "GGML_RPC_COALESCE": "0",
               "DYLD_LIBRARY_PATH": str(root / "rpc-overlay-zstd")}
        smoke = subprocess.run([str(root / "rpc-frame-smoke-zstd"), "127.0.0.1:50159"],
                               env=env, capture_output=True, text=True, timeout=60)
        (args.output_dir / "tensor-checks.txt").write_text(smoke.stdout + smoke.stderr)
        smoke.check_returncode()
        command = [str(root / "llama.cpp/build/bin/llama-server"), "-m",
                   str(root / "models/Qwen_Qwen3.5-4B-Q4_K_M.gguf"),
                   "--rpc", "127.0.0.1:50159", "--device", "RPC0,MTL0", "-ngl", "99",
                   "--split-mode", "layer", "--tensor-split", "1,1", "-c", "8192", "-np", "1",
                   "--reasoning", "off", "--cache-ram", "0", "--ctx-checkpoints", "0",
                   "--host", "127.0.0.1", "--port", "8082", "-lv", "4"]
        report["server_command"] = command
        with (args.output_dir / "server.log").open("x") as log:
            process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 120
        while True:
            if process.poll() is not None:
                raise RuntimeError("Zstd test server exited")
            try:
                if get_json("http://127.0.0.1:8082", "/health", 2).get("status") == "ok":
                    break
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError("Zstd model load exceeded budget")
            time.sleep(0.5)
        report["warmup"] = measure("http://127.0.0.1:8082", payload, 90)
        if report["warmup"]["status"] != "passed":
            raise RuntimeError("Zstd warmup failed")
        for index, enabled in enumerate(report["order"]):
            remote(f'printf {enabled} > "$HOME/llama-rpc-demo/zstd-control"')
            offset = int(remote('wc -c < "$HOME/llama-rpc-demo/rpc-zstd.log"'))
            result = measure("http://127.0.0.1:8082", payload, 90)
            trace = remote(f'tail -c +{offset + 1} "$HOME/llama-rpc-demo/rpc-zstd.log"')
            frames = [tuple(map(int, row)) for row in re.findall(
                r"\[zstd\] raw=(\d+) wire=(\d+) level=(\d+)", trace)]
            result.update(run=index + 1, zstd_enabled=bool(enabled),
                          response_payload_raw_bytes=sum(row[0] for row in frames),
                          response_payload_wire_bytes=sum(row[1] for row in frames),
                          compressed_frames=sum(row[1] < row[0] for row in frames),
                          observed_frames=len(frames))
            report["runs"].append(result)
            (args.output_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            print(f"Run {index + 1}, zstd={enabled}, "
                  f"TPS={result.get('server_timings', {}).get('predicted_per_second')}, "
                  f"compressed frames={result['compressed_frames']}", flush=True)
            if result["status"] != "passed" or not frames or (enabled and not result["compressed_frames"]):
                raise RuntimeError("Generation/compression validation failed")
        report["summary"] = {}
        for enabled in [False, True]:
            rows = [row for row in report["runs"] if row["zstd_enabled"] == enabled]
            report["summary"][str(enabled)] = {
                "median_tokens_per_second": statistics.median(row["server_timings"]["predicted_per_second"] for row in rows),
                "rates": [row["server_timings"]["predicted_per_second"] for row in rows],
                "raw_payload_bytes": sum(row["response_payload_raw_bytes"] for row in rows),
                "wire_payload_bytes": sum(row["response_payload_wire_bytes"] for row in rows)}
        report["all_outputs_identical"] = len({row["text"] for row in report["runs"]}) == 1
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
        if worker_started:
            remote('kill -TERM "$(cat "$HOME/llama-rpc-demo/rpc-zstd.pid")"')
        if forwarded:
            subprocess.run([*forward, "-O", "cancel", args.worker], check=False, timeout=30)
        subprocess.run(["bash", str(root / "start.sh")], check=True, timeout=30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
