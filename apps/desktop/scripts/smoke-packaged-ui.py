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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', required=True)
    args = parser.parse_args()
    windows = 'windows' in args.target
    stem = 'meshthing-' + args.target
    archive = Path('dist') / (stem + ('.zip' if windows else '.tar.gz'))
    expected = archive.with_name(archive.name + '.sha256').read_text().split()[0]
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == expected
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
        assert not (root / 'runtime').exists()
        assert any((root / 'third-party-licenses').glob('*manrope*/LICENSE'))
        assert any((root / 'third-party-licenses').glob('*dm-sans*/LICENSE'))
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        command = [str(root / ('meshthing.exe' if windows else 'meshthing')),
                   '--no-open', '--port', str(port), '--state-dir', str(extracted / 'state')]
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
                        with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/status', timeout=2) as response:
                            status = json.load(response)
                            assert status['pid'] == process.pid
                            assert status['api']['base_url'] == f'http://127.0.0.1:{port}/v1'
                            assert not status['api']['inference_ready']
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
    print('PASS: archive checksums, standalone browser server, API response, and process cleanup')


if __name__ == '__main__':
    main()
