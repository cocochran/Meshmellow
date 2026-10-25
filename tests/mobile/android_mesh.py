"""Run the real-phone instrumentation against a separate core CLI peer.
Usage: python3 tests/mobile/android_mesh.py ADB_SERIAL
Add --background after the serial to run the foreground-service suite
(build the test APK with -PbackgroundTest=true).
Install the debug app and androidTest APK first. No emulator is started.
"""
import json
import os
import pathlib
import select
import subprocess
import sys
import tempfile

repo = pathlib.Path(__file__).resolve().parents[2]
adb = os.environ.get("ADB", "/Users/stephenson.439/Library/Android/sdk/platform-tools/adb")
serial = sys.argv[1]
runner = "BackgroundInstrumentation" if "--background" in sys.argv[2:] else "MeshInstrumentation"
assert not serial.startswith("emulator-"), "This validation requires a physical phone"
with tempfile.TemporaryDirectory(prefix="layercake-core-peer-") as directory:
    with open("/tmp/layercake-core-peer-stderr.log", "w") as errors:
        core = subprocess.Popen([str(repo / "target/debug/llm-cluster"), "mesh", "--state-dir", directory], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors, text=True)
        def command(op, **fields):
            core.stdin.write(json.dumps(dict(op=op, **fields)) + "\n")
            core.stdin.flush()
            assert select.select([core.stdout], [], [], 15)[0], f"Core command {op} did not respond within 15 seconds"
            response = json.loads(core.stdout.readline())
            assert response["ok"], response.get("error")
            return response["state"]
        instrument = None
        try:
            identity = command("snapshot")["id"]
            instrument = subprocess.Popen([adb, "-s", serial, "shell", "am", "instrument", "-w", "-r", "-e", "expectedPeer", identity, f"org.meshthing.mobile.test/org.meshthing.mobile.{runner}"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            passed = False
            for line in instrument.stdout:
                if line.startswith("INSTRUMENTATION_STATUS: ticket="):
                    ticket = line.split("ticket=", 1)[1].strip()
                    for attempt in range(3):
                        try:
                            command("join", invitation=ticket, device_name="Core validation companion")
                            break
                        except AssertionError:
                            if attempt == 2:
                                raise
                    print("Core submitted signed invitation to the physical phone through Iroh", flush=True)
                elif line.startswith("INSTRUMENTATION_STATUS: background="):
                    observed = command("snapshot")
                    assert observed.get("connected_peers"), "CLI peer lost the background phone"
                    print("PASS: independent CLI still sees the phone's live control connection", flush=True)
                elif "PASS:" in line:
                    passed = True
                    print(line.strip(), flush=True)
                elif "error=" in line or "FAIL" in line or "shortMsg" in line or "longMsg" in line:
                    print(line.strip(), flush=True)
            assert instrument.wait(timeout=15) == 0 and passed, "Phone integration did not pass"
            state = command("snapshot")
            assert state["mesh"] and len(state["members"]) == 2
            print("PASS: core CLI persisted the same two-member mesh", flush=True)
        finally:
            if instrument and instrument.poll() is None:
                instrument.terminate()
            core.stdin.close()
            core.wait(timeout=35)
