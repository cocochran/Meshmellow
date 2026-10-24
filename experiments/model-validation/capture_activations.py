#!/usr/bin/env python3
"""Capture synthetic-prompt RPC activation responses with an isolated overlay."""

import argparse
import json
import os
import socket
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
    args.output_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    samples = args.output_dir / "samples"
    samples.mkdir(mode=0o700)
    process = tunnel = None
    request = json.loads((root / "results/rpc-framing-01/control.json").read_text())["request"]
    try:
        subprocess.run(["launchctl", "remove", "local.llama-rpc-demo"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with (args.output_dir / "tunnel.log").open("x") as log:
            tunnel = subprocess.Popen([
                "ssh", "-N", "-o", "ControlMaster=no", "-o", "ControlPath=none",
                "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
                "-L", "127.0.0.1:50157:127.0.0.1:50052", args.worker,
            ], stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 20
        while True:
            if tunnel.poll() is not None:
                raise RuntimeError("Capture tunnel exited")
            try:
                with socket.create_connection(("127.0.0.1", 50157), timeout=1):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("Capture tunnel did not open")
                time.sleep(0.2)
        command = [str(root / "llama.cpp/build/bin/llama-server"), "-m",
                   str(root / "models/Qwen_Qwen3.5-4B-Q4_K_M.gguf"), "--rpc", "127.0.0.1:50157",
                   "--device", "RPC0,MTL0", "-ngl", "99", "--split-mode", "layer",
                   "--tensor-split", "1,1", "-c", "8192", "-np", "1", "--reasoning", "off",
                   "--cache-ram", "0", "--ctx-checkpoints", "0", "--host", "127.0.0.1",
                   "--port", "8082", "-lv", "4"]
        env = {**os.environ, "GGML_RPC_NO_RDMA": "1", "GGML_RPC_BATCH_WRITES": "0",
               "GGML_RPC_COALESCE": "0", "GGML_RPC_CAPTURE_DIR": str(samples),
               "DYLD_LIBRARY_PATH": str(root / "rpc-overlay-capture")}
        with (args.output_dir / "server.log").open("x") as log:
            process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 120
        while True:
            if process.poll() is not None:
                raise RuntimeError("Capture server exited")
            try:
                if get_json("http://127.0.0.1:8082", "/health", 2).get("status") == "ok":
                    break
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError("Capture server did not load")
            time.sleep(0.5)
        (samples / "enabled").touch()
        result = measure("http://127.0.0.1:8082", request, 90)
        (samples / "enabled").unlink()
        files = sorted(samples.glob("*.bin"))
        report = {"request": request, "generation": result,
                  "samples": [{"file": p.name, "bytes": p.stat().st_size} for p in files],
                  "timing_note": "Capture file I/O affects timings; do not use as throughput evidence."}
        (args.output_dir / "capture.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"Generation: {result['status']}; captured {len(files)} buffers", flush=True)
        return 0 if result["status"] == "passed" and files else 1
    finally:
        for child in [process, tunnel]:
            if child is not None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        subprocess.run(["bash", str(root / "start.sh")], check=True, timeout=30)


if __name__ == "__main__":
    raise SystemExit(main())
