"""Companion for the iPad XCTest: resolve its invitation, request admission,
and verify the UI's approval over a real Iroh connection. No private keys read.
Run with the simulator UUID as the only argument before test-without-building. Both clients must use the same configured lookup node.
"""
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import time

root = pathlib.Path(__file__).resolve().parents[1]
alphabet_source = (root / "src/membership.rs").read_text().split("pub const EMOJI")[1].split("];", 1)[0]
alphabet = re.findall(r'"([^"\n]+)"', alphabet_source)
assert len(alphabet) == 64

with tempfile.TemporaryDirectory(prefix="mesh-ui-peer-") as directory:
    peer = subprocess.Popen([str(root / "target/debug/meshthing-enrollment"), directory], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    def command(op, **fields):
        peer.stdin.write(json.dumps(dict(op=op, **fields)) + "\n")
        peer.stdin.flush()
        response = json.loads(peer.stdout.readline())
        if not response["ok"]:
            raise RuntimeError(response["error"])
        return response["state"]
    try:
        deadline = time.monotonic() + 180
        joined = False
        while time.monotonic() < deadline:
            container = subprocess.run(["xcrun", "simctl", "get_app_container", sys.argv[1], "org.meshthing.mobile", "data"], capture_output=True, text=True)
            state_file = pathlib.Path(container.stdout.strip()) / "Library/Application Support/Enrollment/membership.json"
            if container.returncode == 0 and state_file.exists():
                state = json.loads(state_file.read_text())
                if state.get("ledger") and state.get("invite"):
                    issuer = state["ledger"]["genesis"]["root"]
                    invitation = state["invite"]["id"]
                    bits = int.from_bytes(hashlib.sha256(f"meshthing/emoji/1:{issuer}:{invitation}".encode()).digest()[:6], "big")
                    code = " ".join(alphabet[(bits >> (42 - i * 6)) & 63] for i in range(8))
                    try:
                        command("join", code=code, device_name="Validation peer")
                    except RuntimeError:
                        # The issuer persists before publishing its signed locator.
                        time.sleep(1)
                        continue
                    print("Join submitted through Internet invitation lookup + Iroh", flush=True)
                    joined = True
                    break
            time.sleep(1)
        assert joined, "No invitation discovered"
        while time.monotonic() < deadline:
            state = command("refresh")
            if state["mesh"]:
                assert len(state["members"]) == 2
                print("PASS: iPad UI approved the discovered peer; membership persisted", flush=True)
                break
            time.sleep(1)
        else:
            raise AssertionError("UI did not approve the request")
    finally:
        peer.stdin.close()
        peer.wait(timeout=15)
