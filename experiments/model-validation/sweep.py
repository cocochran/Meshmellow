#!/usr/bin/env python3
"""Compare RPC placements sequentially against the same model and prompt."""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

from benchmark import get_json

CONFIGURATIONS = [
    ("control-50-50", "1,1", []),
    ("no-snapshots-50-50", "1,1", ["--cache-ram", "0", "--ctx-checkpoints", "0"]),
    ("local-75-percent", "1,3", ["--cache-ram", "0", "--ctx-checkpoints", "0"]),
    ("remote-75-percent", "3,1", ["--cache-ram", "0", "--ctx-checkpoints", "0"]),
    ("control-50-50-repeat", "1,1", []),
    ("mtp-1", "1,1", ["--cache-ram", "0", "--ctx-checkpoints", "0",
                       "--spec-type", "draft-mtp", "--spec-draft-n-max", "1"]),
    ("mtp-2", "1,1", ["--cache-ram", "0", "--ctx-checkpoints", "0",
                       "--spec-type", "draft-mtp", "--spec-draft-n-max", "2"]),
    ("mtp-4", "1,1", ["--cache-ram", "0", "--ctx-checkpoints", "0",
                       "--spec-type", "draft-mtp", "--spec-draft-n-max", "4"]),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--llama-cpp", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rpc", default="127.0.0.1:50053")
    parser.add_argument("--port", type=int, default=8082)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--only", nargs="+", choices=[item[0] for item in CONFIGURATIONS])
    parser.add_argument("--profile", action="store_true", help="Log RPC command timings; affects performance")
    args = parser.parse_args()
    if args.runs < 1 or args.max_tokens < 1 or not 0 < args.port < 65536:
        parser.error("runs/max-tokens must be positive, port must be 1..65535")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    base = f"http://127.0.0.1:{args.port}"
    configurations = (CONFIGURATIONS[:5] if not args.only else
                      [item for item in CONFIGURATIONS if item[0] in args.only])
    summaries = []
    for name, split, options in configurations:
        log_path = args.output_dir / f"{name}.log"
        output_path = args.output_dir / f"{name}.json"
        server_command = [
            str(args.llama_cpp / "build/bin/llama-server"), "-m", str(args.model),
            "--rpc", args.rpc, "--device", "RPC0,MTL0",
            "-ngl", "99", "--split-mode", "layer", "--tensor-split", split,
            "-c", "8192", "-np", "1", "--reasoning", "off", "--host", "127.0.0.1",
            "--port", str(args.port), "-lv", "5" if args.profile else "4", *options,
        ]
        summary = {"configuration": name, "server_command": server_command,
                   "rpc_profiling": args.profile, "status": "failed"}
        print(f"Starting {name}: remote/local split {split}", flush=True)
        with log_path.open("x") as log:
            process = subprocess.Popen(server_command, stdout=log, stderr=subprocess.STDOUT,
                                       env={**os.environ, "GGML_RPC_NO_RDMA": "1",
                                            "GGML_RPC_DEBUG": "3" if args.profile else "0"})
            try:
                deadline = time.monotonic() + 120
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(f"Server exited with code {process.returncode}; see {log_path}")
                    try:
                        if get_json(base, "/health", 2).get("status") == "ok":
                            break
                    except OSError:
                        pass
                    if time.monotonic() > deadline:
                        raise TimeoutError("Model loading exceeded 120 seconds")
                    time.sleep(0.5)
                benchmark = subprocess.run([
                    sys.executable, str(Path(__file__).with_name("benchmark.py")),
                    "--base-url", base, "--model", str(args.model),
                    "--llama-cpp", str(args.llama_cpp), "--transport", "ssh-rpc",
                    "--server-log", str(log_path), "--runs", str(args.runs),
                    "--max-tokens", str(args.max_tokens),
                    "--timeout", "90", "--output", str(output_path),
                ], timeout=300)
                result = json.loads(output_path.read_text())
                summary["status"] = result["status"]
                if benchmark.returncode == 0:
                    summary.update(result["summary"])
                    summary["median_decode_tokens_per_second"] = statistics.median(
                        run["server_timings"]["predicted_per_second"] for run in result["runs"])
                    summary["decode_tokens_per_second"] = [
                        run["server_timings"]["predicted_per_second"] for run in result["runs"]]
                else:
                    summary["error"] = result.get("error", "Generation failed; inspect result JSON")
            except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                summary["error"] = str(error)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        summaries.append(summary)
        (args.output_dir / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
        print(json.dumps(summary), flush=True)
    return 0 if all(item["status"] == "passed" for item in summaries) else 1


if __name__ == "__main__":
    sys.exit(main())
