#!/usr/bin/env python3
"""Record reproducible llama.cpp chat runs; no third-party dependencies."""

import argparse
import hashlib
import http.client
import json
import platform
import statistics
import subprocess
import sys
import time
import urllib.request
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def command(*args: str) -> str:
    return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()


def events(lines: Iterable[bytes]) -> Iterator[str]:
    """Decode SSE data fields, including multi-line events."""
    data = []
    for raw in lines:
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if data:
                yield "\n".join(data)
                data = []
        elif line.startswith("data:"):
            data.append(line[5:].removeprefix(" "))
    if data:
        yield "\n".join(data)


def get_json(base: str, route: str, timeout: float) -> dict[str, Any]:
    with urllib.request.urlopen(base + route, timeout=timeout) as response:
        return json.load(response)


def allocation_evidence(log: str) -> list[str]:
    lines = log.splitlines()
    starts = [index for index, line in enumerate(lines)
              if "llama_server: initializing ..." in line]
    if not starts:
        return []
    return [line for line in lines[starts[-1]:] if any(
        marker in line for marker in
        ["model buffer size", "KV buffer size", "compute buffer size"])]


def measure(base: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    started = time.monotonic()
    result: dict[str, Any] = {"status": "failed", "text": "", "ttft_seconds": None}
    request = urllib.request.Request(
        base + "/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    finished = False
    done = False
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            for data in events(response):
                if time.monotonic() - started > timeout:
                    raise TimeoutError("Run exceeded time budget")
                if data == "[DONE]":
                    done = True
                    break
                event = json.loads(data)
                if "error" in event:
                    raise RuntimeError(str(event["error"]))
                if event.get("usage"):
                    result["usage"] = event["usage"]
                if event.get("timings"):
                    result["server_timings"] = event["timings"]
                for choice in event.get("choices", []):
                    content = choice.get("delta", {}).get("content") or ""
                    if content and result["ttft_seconds"] is None:
                        result["ttft_seconds"] = time.monotonic() - started
                    result["text"] += content
                    if choice.get("finish_reason") is not None:
                        finished = True
                        result["finish_reason"] = choice["finish_reason"]
        if not (done and finished and result["text"].strip()):
            raise RuntimeError("Incomplete or empty generation stream")
        result["status"] = "passed"
    except (OSError, ValueError, RuntimeError, http.client.HTTPException) as error:
        result["error"] = str(error)
    result["elapsed_seconds"] = time.monotonic() - started
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8081")
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--llama-cpp", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--transport", choices=["local", "ssh-rpc", "iroh-rpc"], required=True)
    parser.add_argument("--server-log", type=Path)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    if args.runs < 1 or args.max_tokens < 1 or args.timeout <= 0:
        parser.error("runs, max-tokens, and timeout must be positive")
    if args.output.exists():
        parser.error("output already exists; choose a new evidence file")
    base = args.base_url.rstrip("/")
    report: dict[str, Any] = {
        "schema_version": 1,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "transport": args.transport,
        "base_url": base,
        "platform": platform.platform(),
        "status": "failed",
        "runs": [],
        "limitations": [
            "Transport is operator-supplied; generated text does not prove remote compute.",
            "TTFT measures first visible content, not hidden reasoning or model load.",
            "No peak device memory or remote hardware telemetry is collected here.",
            "All runs use an already loaded model; run 1 is not a cold-load measurement.",
        ],
    }
    try:
        model_hash = hashlib.sha256()
        with args.model.open("rb") as model:
            for chunk in iter(lambda: model.read(8 * 1024 * 1024), b""):
                model_hash.update(chunk)
        report["model"] = {"filename": args.model.name, "bytes": args.model.stat().st_size,
                           "sha256": model_hash.hexdigest()}
        report["runtime_commit"] = command("git", "-C", str(args.llama_cpp), "rev-parse", "HEAD")
        report["runtime_dirty"] = bool(command("git", "-C", str(args.llama_cpp), "status", "--porcelain"))
        props = get_json(base, "/props", args.timeout)
        served_path = props.get("model_path") or props.get("model_alias", "")
        if Path(served_path).name != args.model.name:
            raise RuntimeError("Server model filename differs from the supplied model file")
        report["server"] = {key: props.get(key) for key in
                            ["model_alias", "model_ftype", "build_info", "total_slots"]}
        report["server"]["n_ctx"] = props.get("default_generation_settings", {}).get("n_ctx")
        report["server"]["chat_template_sha256"] = hashlib.sha256(
            props.get("chat_template", "").encode()).hexdigest()
        report["limitations"].append("Server model hash is not remotely verified; filename match only.")
        report["health"] = get_json(base, "/health", args.timeout)
        if args.server_log:
            report["startup_allocation_evidence"] = allocation_evidence(
                args.server_log.read_text(errors="replace"))
        payload = {
            "messages": [{"role": "user", "content": "Explain how splitting one language model across two computers differs from running two independent models. Use three short sentences."}],
            "max_tokens": args.max_tokens, "temperature": 0, "seed": 42,
            "stream": True, "stream_options": {"include_usage": True},
            "cache_prompt": False, "timings_per_token": True,
        }
        report["request"] = payload
        for index in range(args.runs):
            result = measure(base, payload, args.timeout)
            result["run"] = index + 1
            report["runs"].append(result)
            print(f"Run {index + 1}: {result['status']}; {result['elapsed_seconds']:.2f}s", flush=True)
            if result["status"] != "passed":
                break
        passed = [run for run in report["runs"] if run["status"] == "passed"]
        if len(passed) == args.runs:
            report["status"] = "passed"
            report["summary"] = {
                "runs": len(passed),
                "median_ttft_seconds": statistics.median(run["ttft_seconds"] for run in passed),
                "median_elapsed_seconds": statistics.median(run["elapsed_seconds"] for run in passed),
            }
    except (OSError, ValueError, RuntimeError, http.client.HTTPException,
            subprocess.CalledProcessError) as error:
        report["error"] = str(error)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    print(f"Evidence: {args.output}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
