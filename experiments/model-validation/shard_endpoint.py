#!/usr/bin/env python3
"""Single-session loopback adapter for the existing opaque Iroh core bridge."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import select
import socket
import subprocess
import threading


def digest(path: str | Path) -> str:
    sha = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            sha.update(block)
    return sha.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--source-sha256", help="Original model identity for an exported shard")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command.pop(0)
    if len(args.command) != 9:
        parser.error("supply the runner and its eight arguments after --")
    model_hash = digest(args.command[1])
    if model_hash != args.expected_sha256:
        raise ValueError("worker model checksum mismatch")
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", args.port))
        listener.listen(1)
        print(json.dumps({"listening": args.port, "model_sha256": model_hash}), flush=True)
        connection, _ = listener.accept()
    with connection:
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connection.settimeout(30)
        preface = b""
        while len(preface) < 5:
            part = connection.recv(5 - len(preface))
            if not part:
                raise EOFError("missing client preface")
            preface += part
        if preface != b"MSH1\n":
            raise ValueError("invalid client preface")
        connection.settimeout(None)
        process = subprocess.Popen(args.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        def receive() -> None:
            try:
                while block := connection.recv(65536):
                    process.stdin.write(block)
                    process.stdin.flush()
            except (OSError, BrokenPipeError):
                pass
            finally:
                try:
                    process.stdin.close()
                except OSError:
                    pass
        try:
            if not select.select([process.stdout], [], [], 240)[0]:
                raise TimeoutError("worker initialization timed out")
            first = process.stdout.readline(65536)
            info = json.loads(first)
            if args.source_sha256 and info.get("source_sha256") != args.source_sha256:
                raise ValueError("exported shard source identity mismatch")
            info.update(model_sha256=args.source_sha256 or model_hash,
                        artifact_sha256=model_hash, runner_sha256=digest(args.command[0]))
            connection.sendall((json.dumps(info) + "\n").encode())
            receiver = threading.Thread(target=receive, daemon=True)
            receiver.start()
            while block := process.stdout.read1(65536):
                connection.sendall(block)
            process.wait(timeout=10)
            if process.returncode:
                raise RuntimeError("partition worker failed")
        finally:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
