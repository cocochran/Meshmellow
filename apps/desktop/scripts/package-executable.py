"""Package terminal executables, browser dashboard, in one Rust process."""
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
    args = parser.parse_args()
    windows = 'windows' in args.target
    extension = '.exe' if windows else ''
    release = Path(args.target_dir) / args.target / 'release'
    binaries = [release / (name + extension) for name in ['meshmellow', 'llm-cluster']]
    for binary in binaries:
        if not binary.is_file():
            raise SystemExit(f'Release executable missing: {binary}')
    if not Path('apps/web/build/index.html').is_file():
        raise SystemExit('Build apps/web before packaging.')
    output = Path('dist')
    output.mkdir(exist_ok=True)
    stem = f'meshmellow-{args.target}'
    with tempfile.TemporaryDirectory() as folder:
        bundle = Path(folder) / stem
        bundle.mkdir()
        for binary in binaries:
            executable = bundle / binary.name
            shutil.copy2(binary, executable)
            executable.chmod(0o755)
        shutil.copytree('apps/web/build', bundle / 'web/build')
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
        }, indent=2) + '\n', encoding='utf-8')
        (bundle / 'README.txt').write_text(
            'Meshmellow terminal tools + browser dashboard\n\n'
            f'Run meshmellow{extension} in a terminal to start the dashboard and open your default browser.\n'
            'The terminal stays running. Ctrl+C stops the runtime and its dashboard.\n'
            'The runtime owns membership, heartbeat, and the local OpenAI API.\n'
            'Use --upstream 127.0.0.1:PORT to attach a running inference backend, or --no-open.\n'
            f'Run llm-cluster{extension} --help for core commands.\n'
            'Keep web/ beside the executables. No Node runtime is needed.\n'
            'Inference runners and model weights remain separate.\n'
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
