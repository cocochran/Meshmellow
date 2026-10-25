"""Package a native release executable with its build identity and SHA-256."""

import argparse
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import zipfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    windows = "windows" in args.target
    name = "llm-cluster.exe" if windows else "llm-cluster"
    binary = Path("target") / args.target / "release" / name
    if not binary.is_file():
        raise SystemExit(f"Release executable missing: {binary}")
    output = Path("dist")
    output.mkdir(exist_ok=True)
    stem = f"meshthing-cli-{args.target}"
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory() as folder:
        bundle = Path(folder) / stem
        bundle.mkdir()
        executable = bundle / name
        executable.write_bytes(binary.read_bytes())
        executable.chmod(0o755)
        (bundle / "build.json").write_text(json.dumps({
            "revision": args.revision,
            "target": args.target,
            "executable": name,
            "sha256": digest,
        }, indent=2) + "\n", encoding="utf-8")
        (bundle / "README.txt").write_text(
            "MeshThing core CLI\n\n"
            f"Run {name} --help from a terminal.\n"
            "This archive contains the core CLI, not the desktop UI.\n"
            "Model weights and a compatible native inference runner are separate.\n"
            "The executable is unsigned; macOS notarization and Windows signing\n"
            "are not configured in these development builds.\n"
            f"\nSource revision: {args.revision}\nTarget: {args.target}\n",
            encoding="utf-8",
        )
        if windows:
            archive = output / f"{stem}.zip"
            with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as package:
                for path in sorted(bundle.iterdir()):
                    package.write(path, f"{stem}/{path.name}")
        else:
            archive = output / f"{stem}.tar.gz"
            with tarfile.open(archive, "w:gz") as package:
                package.add(bundle, arcname=stem)
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    (output / f"{archive.name}.sha256").write_text(
        f"{checksum}  {archive.name}\n", encoding="utf-8"
    )
    print(f"Packaged {archive}")


if __name__ == "__main__":
    main()
