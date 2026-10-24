"""Materialize a shard from a verified header and selected source HTTP ranges."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import time
from urllib.request import Request, urlopen


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--header", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    if args.output.exists() or not plan["url"].startswith("https://"):
        raise ValueError("output exists or source is not HTTPS")
    header = args.header.read_bytes()
    if hashlib.sha256(header).hexdigest() != plan["header_sha256"]:
        raise ValueError("header checksum mismatch")
    temporary = args.output.with_suffix(args.output.suffix + ".range.part")
    with temporary.open("wb") as destination:
        destination.write(header)
        destination.truncate(plan["artifact_bytes"])
    chunks = []
    for item in plan["ranges"]:
        for offset in range(0, item["size"], 64 * 1024 * 1024):
            size = min(64 * 1024 * 1024, item["size"] - offset)
            start, target = item["source"] + offset, item["destination"] + offset
            if start < 0 or target < len(header) or target + size > plan["artifact_bytes"]:
                raise ValueError("invalid artifact range")
            chunks.append((start, target, size))

    def fetch(chunk: tuple[int, int, int]) -> int:
        start, target, size = chunk
        end = start + size - 1
        for attempt in range(3):
            try:
                request = Request(plan["url"], headers={"Range": f"bytes={start}-{end}"})
                with urlopen(request, timeout=120) as response, temporary.open("r+b") as destination:
                    expected = f"bytes {start}-{end}/{plan['source_bytes']}"
                    if response.status != 206 or response.headers.get("Content-Range") != expected:
                        raise ValueError("server returned an unexpected HTTP range")
                    destination.seek(target)
                    remaining = size
                    while remaining:
                        block = response.read(min(1024 * 1024, remaining))
                        if not block:
                            raise EOFError("truncated HTTP range")
                        destination.write(block)
                        remaining -= len(block)
                return size
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(attempt + 1)
        raise RuntimeError("range retry exhausted")

    total = 0
    with ThreadPoolExecutor(max_workers=4) as pool:
        for size in pool.map(fetch, chunks):
            total += size
            print(json.dumps({"downloaded_tensor_bytes": total}), flush=True)
    digest = hashlib.sha256()
    with temporary.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != plan["artifact_sha256"]:
        raise ValueError("materialized artifact checksum mismatch")
    temporary.rename(args.output)
    print(json.dumps({"passed": True, "artifact_sha256": digest.hexdigest()}), flush=True)


if __name__ == "__main__":
    main()
