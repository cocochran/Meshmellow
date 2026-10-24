#!/usr/bin/env python3
"""Exercise persistent prefix/suffix runners; optionally compare unsplit logits."""

import argparse
import array
import hashlib
import json
import math
import os
from pathlib import Path
import select
import shlex
import socket
import struct
import subprocess
import time
from typing import Any


class Runner:
    def __init__(self, command: list[str], log_path: Path) -> None:
        self.log = log_path.open("xb")
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=self.log)
        self.buffer = bytearray()
        self.step = self.pos = 0
        self.input, self.output = self.process.stdin, self.process.stdout
        try:
            self.info = json.loads(self.line(240))
            if self.info["protocol"] != 1:
                raise ValueError("unsupported runner protocol")
        except BaseException:
            self.close()
            raise

    def fill(self, deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([self.output], [], [], remaining)[0]:
            raise TimeoutError("runner response timed out")
        data = os.read(self.output.fileno(), 65536)
        if not data:
            raise RuntimeError("runner exited; inspect its log")
        self.buffer.extend(data)

    def line(self, timeout: float) -> bytes:
        deadline = time.monotonic() + timeout
        while b"\n" not in self.buffer:
            if len(self.buffer) > 65536:
                raise ValueError("oversized runner metadata")
            self.fill(deadline)
        end = self.buffer.index(b"\n")
        result = bytes(self.buffer[:end])
        del self.buffer[:end + 1]
        return result

    def execute(self, n: int, payload: bytes) -> tuple[dict[str, Any], bytes]:
        op = 1 if self.info["begin"] == 0 else 2
        header = struct.pack("!IIIII", 0x4D534831, op, self.step, self.pos, n)
        started = time.monotonic()
        self.input.write(header + payload)
        self.input.flush()
        result = json.loads(self.line(120))
        expected = n * self.info["embd"] if self.info["end"] < self.info["layers"] else None
        count = result["n_floats"]
        if result["step"] != self.step or count < 0 or count > 4_194_304:
            raise ValueError("invalid response metadata")
        if expected is not None and count != expected:
            raise ValueError("wrong activation shape")
        size = count * 4
        deadline = time.monotonic() + 120
        while len(self.buffer) < size:
            self.fill(deadline)
        data = bytes(self.buffer[:size])
        del self.buffer[:size]
        result.update(wall_ms=(time.monotonic() - started) * 1000,
                      request_bytes=len(header) + len(payload), response_tensor_bytes=size)
        self.step += 1
        self.pos += n
        return result, data

    def close(self) -> None:
        if self.process.stdin:
            try:
                self.process.stdin.close()
            except BrokenPipeError:
                pass
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.process.stdout.close()
        self.log.close()


class SocketRunner(Runner):
    def __init__(self, port: int) -> None:
        self.connection = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.connection.settimeout(None)
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.connection.sendall(b"MSH1\n")
        self.input = self.connection.makefile("wb")
        self.output = self.connection.makefile("rb", buffering=0)
        self.buffer = bytearray()
        self.step = self.pos = 0
        try:
            self.info = json.loads(self.line(240))
            if self.info["protocol"] != 1:
                raise ValueError("unsupported runner protocol")
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        try:
            self.input.close()
        except OSError:
            pass
        self.output.close()
        self.connection.close()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compare_logits(reference: bytes, actual: bytes) -> dict[str, float | bool]:
    x, y = array.array("f"), array.array("f")
    x.frombytes(reference)
    y.frombytes(actual)
    if len(x) != len(y) or not x or not all(math.isfinite(v) for v in [*x, *y]):
        raise ValueError("invalid logits")
    errors = [abs(a - b) for a, b in zip(x, y)]
    return {"max_abs_error": max(errors),
            "rmse": math.sqrt(sum(e * e for e in errors) / len(errors)),
            "identical_bytes": reference == actual}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--prefix-artifact", type=Path)
    parser.add_argument("--suffix-artifact", type=Path)
    parser.add_argument("--layers", type=int, required=True)
    parser.add_argument("--cut", type=int, required=True)
    parser.add_argument("--context", type=int, default=512)
    parser.add_argument("--tokens", type=int, default=24)
    parser.add_argument("--gpu-layers", type=int, default=99)
    parser.add_argument("--budget-bytes", type=int, default=11 * 1024 ** 3)
    parser.add_argument("--prompt", default="The capital of France is")
    parser.add_argument("--reference", action="store_true")
    parser.add_argument("--max-logit-error", type=float, default=0.01)
    parser.add_argument("--worker")
    parser.add_argument("--remote-runner")
    parser.add_argument("--remote-model")
    parser.add_argument("--control-path")
    parser.add_argument("--iroh-port", type=int,
                        help="Existing core bridge-connect loopback port; inference uses Iroh")
    parser.add_argument("--expected-sha256")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if not 0 < args.cut < args.layers or not 1 <= args.tokens <= 256:
        parser.error("invalid split or token count")
    if args.worker and not args.iroh_port and not (args.remote_runner and args.remote_model):
        parser.error("remote runner/model required")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    model_hash = file_hash(args.model)
    if args.expected_sha256 and model_hash != args.expected_sha256:
        raise ValueError("model checksum mismatch")
    ssh = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    if args.control_path:
        ssh += ["-S", args.control_path]
    if args.worker and not args.iroh_port:
        remote_hash = subprocess.check_output(
            [*ssh, args.worker, "shasum -a 256 " + shlex.quote(args.remote_model)],
            text=True, timeout=240).split()[0]
        if remote_hash != model_hash:
            raise ValueError("remote/local model checksum mismatch")

    def command(begin: int, end: int, remote: bool = False) -> list[str]:
        result = [str(args.runner.resolve()), str(args.model.resolve()), str(begin), str(end),
                  str(args.context), str(args.gpu_layers), str(args.budget_bytes),
                  args.prompt, "1" if args.reference else "0"]
        if begin == 0 and end == args.cut and args.prefix_artifact:
            result[1] = str(args.prefix_artifact.resolve())
        if begin == args.cut and args.suffix_artifact:
            result[1] = str(args.suffix_artifact.resolve())
        if remote:
            result[0:2] = [args.remote_runner, args.remote_model]
            result = [*ssh, args.worker, shlex.join(result)]
        return result

    runners = []
    report = {"model_sha256": model_hash, "model_bytes": args.model.stat().st_size,
              "runner_sha256": file_hash(args.runner), "cut": args.cut,
              "layers": args.layers, "context": args.context, "gpu_layers": args.gpu_layers,
              "worker": args.worker, "prompt": args.prompt, "steps": [],
              "transport": "existing core Iroh bridge" if args.iroh_port else "local pipes or SSH",
              "memory_note": "Observed GGML buffers at init/step boundaries, not a total allocation peak or hard memory cap. RSS includes process overhead; unified-memory categories may overlap.",
              "status": "running"}
    try:
        prefix = (SocketRunner(args.iroh_port) if args.iroh_port else
                  Runner(command(0, args.cut, bool(args.worker)), args.output_dir / "prefix.log"))
        runners.append(prefix)
        suffix = Runner(command(args.cut, args.layers), args.output_dir / "suffix.log")
        runners.append(suffix)
        reference = None
        if args.reference:
            reference = Runner(command(0, args.layers), args.output_dir / "reference.log")
            runners.append(reference)
        for runner in runners:
            if runner.info["layers"] != args.layers or any(
                    runner.info[key] != prefix.info[key] for key in ["embd", "vocab", "tokens"]):
                raise ValueError("incompatible model metadata")
        if prefix.info["begin"] != 0 or prefix.info["end"] != args.cut:
            raise ValueError("wrong prefix layer range")
        if args.iroh_port and prefix.info.get("model_sha256") != model_hash:
            raise ValueError("Iroh worker model checksum mismatch")
        report["initialization"] = [r.info for r in runners]
        for runner in runners:
            if runner.info.get("source_sha256") and runner.info["source_sha256"] != model_hash:
                raise ValueError("partition source identity mismatch")
        tokens = prefix.info["tokens"]
        output = bytearray()
        for index in range(args.tokens):
            n = len(tokens)
            if n > 128:
                raise ValueError("prompt exceeds prototype's 128-token prefill limit")
            payload = struct.pack("!" + "I" * n, *tokens)
            ref_result = ref_logits = None
            if reference:
                ref_result, ref_logits = reference.execute(n, payload)
            started = time.monotonic()
            pre, hidden = prefix.execute(n, payload)
            post, logits = suffix.execute(n, hidden)
            elapsed_ms = (time.monotonic() - started) * 1000
            row = {"prefix": pre, "suffix": post, "wall_ms": elapsed_ms,
                   "phase": "prefill" if index == 0 else "decode"}
            if reference:
                row["comparison"] = compare_logits(ref_logits, logits)
                row["comparison"]["same_greedy_token"] = ref_result["token"] == post["token"]
                if not row["comparison"]["same_greedy_token"] or row["comparison"]["max_abs_error"] > args.max_logit_error:
                    raise RuntimeError("partition equivalence failed")
            report["steps"].append(row)
            output.extend(bytes.fromhex(post["piece_hex"]))
            report["text"] = output.decode("utf-8", errors="replace")
            (args.output_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            print(f"step={index} token={post['token']} wall_ms={elapsed_ms:.2f} text={report['text']!r}", flush=True)
            if post["eog"]:
                break
            tokens = [post["token"]]
        decode = report["steps"][1:]
        report["decode_tokens_per_second"] = len(decode) * 1000 / sum(r["wall_ms"] for r in decode) if decode else None
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        for runner in reversed(runners):
            runner.close()
        (args.output_dir / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
