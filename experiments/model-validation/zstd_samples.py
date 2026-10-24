#!/usr/bin/env python3
"""Measure lossless zstd size and codec time on captured RPC buffers."""

import argparse
import ctypes
import hashlib
import json
import platform
import statistics
import time
from pathlib import Path


class Zstd:
    def __init__(self, path: Path) -> None:
        self.lib = ctypes.CDLL(str(path))
        pointer, size = ctypes.c_void_p, ctypes.c_size_t
        signatures = {
            "ZSTD_createCCtx": (pointer, []), "ZSTD_createDCtx": (pointer, []),
            "ZSTD_freeCCtx": (size, [pointer]), "ZSTD_freeDCtx": (size, [pointer]),
            "ZSTD_compressBound": (size, [size]),
            "ZSTD_compressCCtx": (size, [pointer, pointer, size, pointer, size, ctypes.c_int]),
            "ZSTD_decompressDCtx": (size, [pointer, pointer, size, pointer, size]),
            "ZSTD_isError": (ctypes.c_uint, [size]),
            "ZSTD_getErrorName": (ctypes.c_char_p, [size]),
            "ZSTD_versionString": (ctypes.c_char_p, []),
        }
        for name, (result, args) in signatures.items():
            function = getattr(self.lib, name)
            function.restype, function.argtypes = result, args
        self.compressor = self.lib.ZSTD_createCCtx()
        self.decompressor = self.lib.ZSTD_createDCtx()
        if not self.compressor or not self.decompressor:
            self.close()
            raise MemoryError("Could not allocate codec contexts")

    def close(self) -> None:
        self.lib.ZSTD_freeCCtx(self.compressor)
        self.lib.ZSTD_freeDCtx(self.decompressor)

    def check(self, result: int) -> int:
        if self.lib.ZSTD_isError(result):
            raise RuntimeError(self.lib.ZSTD_getErrorName(result).decode())
        return result


def shuffle(data: bytes) -> bytes:
    return b"".join(data[index::4] for index in range(4))


def unshuffle(data: bytes) -> bytes:
    result = bytearray(len(data))
    width = len(data) // 4
    for index in range(4):
        result[index::4] = data[index * width:(index + 1) * width]
    return bytes(result)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=30)
    args = parser.parse_args()
    if args.repeats < 1 or args.output.exists():
        parser.error("repeats must be positive and output must be a new file")
    files = sorted(args.samples.glob("*.bin"))
    if not files:
        parser.error("no captured buffers found")
    codec = Zstd(args.library)
    results = []
    try:
        for path in files:
            raw = path.read_bytes()
            for level, shuffled in [(-1, False), (1, False), (3, False), (1, True)]:
                if shuffled and len(raw) % 4:
                    continue
                prepared = shuffle(raw) if shuffled else raw
                source = ctypes.create_string_buffer(prepared)
                capacity = codec.lib.ZSTD_compressBound(len(raw))
                compressed = ctypes.create_string_buffer(capacity)
                decoded = ctypes.create_string_buffer(len(raw))
                def compress() -> int:
                    return codec.check(codec.lib.ZSTD_compressCCtx(
                        codec.compressor, compressed, capacity, source, len(raw), level))
                packed_size = compress()
                def decompress() -> int:
                    return codec.check(codec.lib.ZSTD_decompressDCtx(
                        codec.decompressor, decoded, len(raw), compressed, packed_size))
                if decompress() != len(raw):
                    raise RuntimeError(f"Wrong decompressed size: {path.name}")
                recovered = unshuffle(decoded.raw) if shuffled else decoded.raw
                if recovered != raw:
                    raise RuntimeError(f"Non-identical round trip: {path.name}")
                started = time.perf_counter_ns()
                for _ in range(args.repeats):
                    compress()
                compress_us = (time.perf_counter_ns() - started) / args.repeats / 1000
                started = time.perf_counter_ns()
                for _ in range(args.repeats):
                    decompress()
                decompress_us = (time.perf_counter_ns() - started) / args.repeats / 1000
                transform_us = 0
                if shuffled:
                    started = time.perf_counter_ns()
                    for _ in range(args.repeats):
                        unshuffle(shuffle(raw))
                    transform_us = (time.perf_counter_ns() - started) / args.repeats / 1000
                results.append({"sample": path.name, "source_sha256": hashlib.sha256(raw).hexdigest(),
                                "raw_bytes": len(raw), "compressed_bytes": packed_size,
                                "level": level, "shuffle": shuffled, "bit_exact": True,
                                "compress_us": compress_us, "decompress_us": decompress_us,
                                "transform_us": transform_us})
        summaries = []
        for size in sorted({row["raw_bytes"] for row in results}):
            for level, shuffled in [(-1, False), (1, False), (3, False), (1, True)]:
                rows = [row for row in results if row["raw_bytes"] == size and
                        row["level"] == level and row["shuffle"] == shuffled]
                if not rows:
                    continue
                summaries.append({"raw_bytes": size, "level": level, "shuffle": shuffled,
                                  "samples": len(rows),
                                  "median_compressed_bytes": statistics.median(row["compressed_bytes"] for row in rows),
                                  "median_codec_us": statistics.median(row["compress_us"] + row["decompress_us"] + row["transform_us"] for row in rows)})
        report = {"zstd_version": codec.lib.ZSTD_versionString().decode(),
                  "platform": platform.platform(), "repeats": args.repeats,
                  "limitations": ["Local CPU timings include ctypes call overhead.",
                                  "Byte-shuffle timings include Python transform cost.",
                                  "This is not an end-to-end network throughput measurement."],
                  "summary": summaries, "results": results}
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(summaries, indent=2))
    finally:
        codec.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
