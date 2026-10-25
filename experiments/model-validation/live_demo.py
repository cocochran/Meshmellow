"""Loopback-only live demo for an already-running Iroh shard pipeline."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

from jinja2 import Environment


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8095)
    args = parser.parse_args()
    base = json.loads(args.config.read_text())
    environment = Environment(trim_blocks=True, lstrip_blocks=True)
    def fail(message: str) -> None:
        raise ValueError(message)
    environment.globals["raise_exception"] = fail
    template = environment.from_string(args.template.read_text())
    active = threading.Lock()
    state_lock = threading.Lock()
    changed = threading.Condition(state_lock)
    latest = {"event": "idle"}
    if base.get("paused"):
        latest.update(event="paused", message=base["paused"])
    sequence = 0
    args.results.mkdir(parents=True, exist_ok=True)

    def publish(value: dict) -> None:
        nonlocal sequence
        with changed:
            if value["event"] == "loading":
                latest.clear()
                latest["devices"] = [{"status": "loading", "executions": 0, "compute_total_ms": 0,
                                      "wall_total_ms": 0, "input_total_bytes": 0, "output_total_bytes": 0}
                                     for _ in base["stages"]]
                latest["trace"] = []
            latest.update(value)
            if value["event"] == "token":
                latest["output_steps"] = value["step"] + 1
            if value["event"] == "ready":
                latest["initialization"] = value["stages"]
                for device, info in zip(latest["devices"], value["stages"]):
                    device.update(status="ready", allocated_bytes=info["allocated_bytes"],
                                  begin=info["begin"], end=info["end"], layers=info["layers"])
            if value["event"] in {"stage_start", "stage_end"}:
                device = latest["devices"][value["stage"]]
                if value["event"] == "stage_start":
                    device.update(status="inflight", input_bytes=value["input_bytes"])
                else:
                    result = value["result"]
                    device.update(status="returned", last=result, input_bytes=value["input_bytes"],
                                  output_bytes=value["output_bytes"])
                    device["executions"] += 1
                    device["compute_total_ms"] += result["compute_ms"]
                    device["wall_total_ms"] += result["wall_ms"]
                    device["input_total_bytes"] += value["input_bytes"]
                    device["output_total_bytes"] += value["output_bytes"]
                latest["trace"].append({**value, "time_ms": time.time() * 1000})
                latest["trace"] = latest["trace"][-24:]
            sequence += 1
            changed.notify_all()

    def generate(question: str) -> None:
        process = None
        try:
            run = args.results / uuid.uuid4().hex
            run.mkdir()
            config = json.loads(json.dumps(base))
            config["stages"][0]["command"][7] = template.render(
                messages=[{"role": "system", "content": "You are a helpful assistant."},
                          {"role": "user", "content": question}],
                add_generation_prompt=True, enable_thinking=False, tools=None)
            config_path = run / "config.json"
            config_path.write_text(json.dumps(config, indent=2) + "\n")
            elapsed = decode = 0
            with (run / "coordinator.log").open("wb") as log:
                coordinator = "ring_trial.py" if config.get("execution") == "p2p_ring" else "pipeline_trial.py"
                process = subprocess.Popen([sys.executable, str(Path(__file__).with_name(coordinator)),
                                            "--config", str(config_path), "--output", str(run / "output"), "--json-events"],
                                           stdout=subprocess.PIPE, stderr=log, text=True)
                for line in process.stdout:
                    value = json.loads(line)
                    if value.get("event") == "token" and value.get("phase") == "decode":
                        elapsed += value["wall_ms"]
                        decode += 1
                        value["rate"] = decode * 1000 / elapsed
                    publish(value)
                process.stdout.close()
                code = process.wait()
            result_path = run / "output/results.json"
            result = json.loads(result_path.read_text()) if result_path.exists() else {}
            if code != 0:
                publish({"event": "error", "message": result.get("error", "A worker stopped; keep the iPad open and retry.")})
            else:
                publish({"event": "done", "text": result["text"], "rate": result["decode_steps_per_second"],
                         "steps": len(result["steps"]), "run_id": run.name})
        except Exception as error:
            publish({"event": "error", "message": str(error)})
        finally:
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            active.release()
            with changed:
                changed.notify_all()

    class Handler(BaseHTTPRequestHandler):
        def respond(self, code: int, data: bytes, kind: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", kind)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            if self.path.split("?")[0] == "/events":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                seen = -1
                try:
                    while True:
                        with changed:
                            if sequence == seen:
                                changed.wait(timeout=10)
                            seen = sequence
                            data = json.dumps({**latest, "active": active.locked()}).encode()
                        self.wfile.write(b"data: " + data + b"\n\n")
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return
            elif self.path.split("?")[0] == "/state":
                with state_lock:
                    state = {**latest, "active": active.locked()}
                self.respond(200, json.dumps(state).encode(), "application/json")
            elif self.path.split("?")[0] == "/":
                self.respond(200, Path(__file__).with_name("live_demo.html").read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/favicon.ico":
                self.respond(204, b"", "image/x-icon")
            else:
                self.send_error(404)

        def do_POST(self) -> None:
            if base.get("paused"):
                self.respond(503, json.dumps({"error":base["paused"]}).encode(), "application/json")
                return
            if self.path != "/run":
                self.send_error(404)
                return
            origin = self.headers.get("Origin")
            if origin and origin not in {f"http://127.0.0.1:{args.port}", f"http://localhost:{args.port}"}:
                self.send_error(403)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 8192 or not self.headers.get("Content-Type", "").startswith("application/json"):
                    raise ValueError("expected a short JSON prompt")
                question = json.loads(self.rfile.read(length))["prompt"].strip()
                if not 1 <= len(question) <= 500:
                    raise ValueError("use a prompt of 1-500 characters")
            except (ValueError, KeyError, TypeError, AttributeError):
                self.send_error(400, "Use a short prompt")
                return
            if not active.acquire(blocking=False):
                self.respond(409, b'{"active":true}', "application/json")
                return
            publish({"event": "loading", "prompt": question})
            threading.Thread(target=generate, args=(question,), daemon=True).start()
            self.respond(202, b'{"active":true}', "application/json")

    print(f"Live demo: http://127.0.0.1:{args.port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
