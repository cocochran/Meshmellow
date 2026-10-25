"""Package terminal executables, browser dashboard, and a standalone Node runtime."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import zipfile


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--target-dir', default=os.environ.get('CARGO_TARGET_DIR', 'target'))
    parser.add_argument('--node', default=shutil.which('node'))
    args = parser.parse_args()
    windows = 'windows' in args.target
    extension = '.exe' if windows else ''
    release = Path(args.target_dir) / args.target / 'release'
    binaries = [release / (name + extension) for name in ['meshthing', 'llm-cluster']]
    for binary in binaries:
        if not binary.is_file():
            raise SystemExit(f'Release executable missing: {binary}')
    if not args.node or not (Path('apps/web/build/handler.js')).is_file():
        raise SystemExit('Build apps/web and install the official Node distribution before packaging.')
    node = Path(args.node).resolve()
    node_version = subprocess.check_output([str(node), '--version'], text=True).strip()
    output = Path('dist')
    output.mkdir(exist_ok=True)
    stem = f'meshthing-{args.target}'
    with tempfile.TemporaryDirectory() as folder:
        bundle = Path(folder) / stem
        bundle.mkdir()
        for binary in binaries:
            executable = bundle / binary.name
            shutil.copy2(binary, executable)
            executable.chmod(0o755)
        runtime = bundle / 'runtime'
        runtime.mkdir()
        shutil.copy2(node, runtime / ('node' + extension))
        (runtime / ('node' + extension)).chmod(0o755)
        license_bytes = subprocess.check_output([
            str(node), '--input-type=module', '-e',
            'const r = await fetch(process.argv[1]); if (!r.ok) throw Error(`License HTTP ${r.status}`); process.stdout.write(await r.text());',
            f'https://raw.githubusercontent.com/nodejs/node/{node_version}/LICENSE',
        ], timeout=30)
        (runtime / 'Node-LICENSE.txt').write_bytes(license_bytes)
        shutil.copytree('apps/web/build', bundle / 'web/build')
        (bundle / 'web/scripts').mkdir()
        for name in ['serve.mjs', 'validation.mjs']:
            shutil.copy2(Path('apps/web/scripts') / name, bundle / 'web/scripts' / name)
        (bundle / 'web/package.json').write_text('{"type":"module"}\n', encoding='utf-8')
        # Preserve notices for dependencies compiled into the dashboard, including its fonts.
        notices = bundle / 'third-party-licenses'
        notices.mkdir()
        packages = []
        modules = Path('apps/web/node_modules')
        for manifest in sorted(modules.rglob('package.json')):
            parent = manifest.parent.parent
            if parent.name != 'node_modules' and not (parent.name.startswith('@') and parent.parent.name == 'node_modules'):
                continue
            package = json.loads(manifest.read_text(encoding='utf-8'))
            if not package.get('name') or not package.get('version'):
                continue
            label = re.sub(r'[^a-zA-Z0-9_.@+-]', '_', package['name'] + '@' + package['version'])
            files = [f for f in manifest.parent.iterdir() if f.is_file() and f.name.lower().startswith(('license', 'licence', 'copying', 'notice'))]
            if files:
                destination = notices / label
                destination.mkdir(exist_ok=True)
                for file in files:
                    shutil.copy2(file, destination / file.name)
            packages.append({key: package.get(key) for key in ['name', 'version', 'license']})
        (notices / 'packages.json').write_text(json.dumps(packages, indent=2) + '\n', encoding='utf-8')
        for font in ['manrope', 'dm-sans']:
            if not (modules / '@fontsource-variable' / font / 'LICENSE').is_file():
                raise SystemExit(f'Missing bundled font license: {font}')

        (bundle / 'build.json').write_text(json.dumps({
            'revision': args.revision, 'target': args.target,
            'executables': {binary.name: digest(binary) for binary in binaries},
            'node_version': node_version, 'node_sha256': digest(node),
        }, indent=2) + '\n', encoding='utf-8')
        (bundle / 'README.txt').write_text(
            'MeshThing terminal tools + browser dashboard\n\n'
            f'Run meshthing{extension} in a terminal to start the dashboard and open your default browser.\n'
            'The terminal stays running. Ctrl+C stops the dashboard only.\n'
            'Default data source: a separately running local core at http://127.0.0.1:8080.\n'
            'For the diagnostic demo: meshthing --source live-demo\n'
            'Use --runtime http://127.0.0.1:PORT for another loopback runtime, or --no-open.\n'
            f'Run llm-cluster{extension} --help for core commands.\n'
            'Keep web/ and runtime/ beside the executables. Node need not be installed.\n'
            'Neither launcher starts workers or downloads models. Native inference runners/model weights are separate.\n'
            'Development executables are unsigned and not notarized.\n'
            f'\nSource revision: {args.revision}\nTarget: {args.target}\n', encoding='utf-8')
        if windows:
            archive = output / f'{stem}.zip'
            with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as package:
                for path in sorted(bundle.rglob('*')):
                    if path.is_file():
                        package.write(path, path.relative_to(bundle.parent))
        else:
            archive = output / f'{stem}.tar.gz'
            with tarfile.open(archive, 'w:gz') as package:
                package.add(bundle, arcname=stem)
    (output / f'{archive.name}.sha256').write_text(f'{digest(archive)}  {archive.name}\n', encoding='utf-8')
    print(f'Packaged {archive}')


if __name__ == '__main__':
    main()
