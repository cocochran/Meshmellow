"""Exercise the extracted native browser launcher without Node on PATH."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.request
import zipfile


class Runtime(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"jobs":[]}'
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', required=True)
    args = parser.parse_args()
    windows = 'windows' in args.target
    stem = 'meshthing-' + args.target
    archive = Path('dist') / (stem + ('.zip' if windows else '.tar.gz'))
    expected = archive.with_name(archive.name + '.sha256').read_text().split()[0]
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == expected
    runtime = ThreadingHTTPServer(('127.0.0.1', 0), Runtime)
    threading.Thread(target=runtime.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory() as directory:
        extracted = Path(directory)
        if windows:
            with zipfile.ZipFile(archive) as package:
                package.extractall(extracted)
        else:
            with tarfile.open(archive) as package:
                package.extractall(extracted, filter='data')
        root = extracted / stem
        build = json.loads((root / 'build.json').read_text())
        for name, digest in build['executables'].items():
            assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest
        assert (root / 'runtime/Node-LICENSE.txt').stat().st_size > 1000
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        command = [str(root / ('meshthing.exe' if windows else 'meshthing')),
                   '--no-open', '--port', str(port), '--runtime',
                   f'http://127.0.0.1:{runtime.server_port}']
        with (extracted / 'launcher.log').open('w+') as log:
            process = subprocess.Popen(command, cwd=directory, env={**os.environ, 'PATH': ''},
                                       stdout=log, stderr=subprocess.STDOUT)
            try:
                for _ in range(150):
                    if process.poll() is not None:
                        raise RuntimeError('Launcher exited during startup')
                    try:
                        with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/mesh', timeout=1) as response:
                            assert response.status == 200
                        break
                    except OSError:
                        time.sleep(0.1)
                else:
                    raise RuntimeError('Packaged dashboard did not start')
                if windows:
                    # CI has no interactive terminal to deliver Ctrl+C. Stop only our owned process tree.
                    subprocess.run([os.path.join(os.environ['SystemRoot'], 'System32', 'taskkill.exe'),
                                    '/PID', str(process.pid), '/T', '/F'], check=True, capture_output=True)
                else:
                    process.send_signal(signal.SIGINT)
                process.wait(timeout=10)
                if not windows:
                    assert process.returncode == 0
                try:
                    urllib.request.urlopen(f'http://127.0.0.1:{port}', timeout=1)
                except OSError:
                    pass
                else:
                    raise RuntimeError('Dashboard server remained running after launcher shutdown')
            except Exception:
                log.seek(0)
                print(log.read())
                raise
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
    runtime.shutdown()
    runtime.server_close()
    print('PASS: archive checksums, standalone browser server, API response, and process cleanup')


if __name__ == '__main__':
    main()
