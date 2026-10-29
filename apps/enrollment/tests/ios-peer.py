"""Companion for iPad XCTest, using a full invitation copied by the test UI.
Run with the dedicated simulator UUID before test-without-building. No keys read.
"""
import json
import pathlib
import subprocess
import sys
import tempfile
import time

repo = pathlib.Path(__file__).resolve().parents[3]
with tempfile.TemporaryDirectory(prefix="mesh-ui-peer-") as directory:
    peer = subprocess.Popen([str(repo / "target/debug/llm-cluster"), "mesh", "--state-dir", directory], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    def command(op, **fields):
        peer.stdin.write(json.dumps(dict(op=op, **fields)) + "\n")
        peer.stdin.flush()
        response = json.loads(peer.stdout.readline())
        if not response["ok"]: raise RuntimeError(response["error"])
        return response["state"]
    try:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            clipboard = subprocess.run(["xcrun", "simctl", "pbpaste", sys.argv[1]], capture_output=True, text=True)
            ticket = clipboard.stdout.strip()
            if ticket.startswith("meshmellow://join/"):
                command("join", invitation=ticket, device_name="Validation peer")
                print("Join submitted from a shared invitation through the core + Iroh", flush=True)
                break
            time.sleep(1)
        else: raise AssertionError("The test UI did not copy an invitation")
        while time.monotonic() < deadline:
            state = command("refresh")
            if state["mesh"]:
                assert len(state["members"]) == 2
                print("PASS: iPad UI approved the core peer; membership persisted", flush=True)
                break
            time.sleep(1)
        else: raise AssertionError("UI did not approve the request")
    finally:
        peer.stdin.close()
        peer.wait(timeout=35)
