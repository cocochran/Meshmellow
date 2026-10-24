"""Exercise contiguous local/Iroh stages, optionally against reference logits."""

import argparse
import json
from pathlib import Path
import struct
import time

from shard_trial import Runner, SocketRunner, compare_logits


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--json-events", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    specifications = config["stages"]
    if len(specifications) < 2 or not 1 <= config["tokens"] <= 256:
        raise ValueError("invalid pipeline configuration")
    args.output.mkdir(parents=True, exist_ok=False)
    runners = []
    reference = None
    report = {"status": "running", "config": config, "steps": [],
              "memory_note": "Runtime buffer samples are not total process peaks or hard caps."}
    try:
        for index, stage in enumerate(specifications):
            runner = (SocketRunner(stage["port"]) if "port" in stage else
                      Runner(stage["command"], args.output / f"stage-{index}.log"))
            runners.append(runner)
        first = runners[0].info
        begin = 0
        for specification, runner in zip(specifications, runners):
            info = runner.info
            if info["begin"] != begin or [info["begin"], info["end"]] != specification["range"]:
                raise ValueError("gap, overlap, or unexpected stage range")
            if not begin < info["end"] <= first["layers"] or any(
                    info[k] != first[k] for k in ["layers", "embd", "vocab"]):
                raise ValueError("incompatible model metadata")
            if config.get("source_sha256") and info.get("source_sha256") != config["source_sha256"]:
                raise ValueError("wrong source model identity")
            begin = info["end"]
        if begin != first["layers"]:
            raise ValueError("pipeline does not cover all layers")
        if "reference" in config:
            reference = Runner(config["reference"], args.output / "reference.log")
            if any(reference.info[k] != first[k] for k in ["layers", "embd", "vocab", "tokens"]):
                raise ValueError("incompatible reference metadata")
        report["initialization"] = [r.info for r in runners]
        tokens = first["tokens"]
        if not 1 <= len(tokens) <= 128:
            raise ValueError("prompt must fit the 128-token diagnostic prefill limit")
        if args.json_events:
            print(json.dumps({"event": "ready", "stages": report["initialization"]}), flush=True)
        output = bytearray()
        for index in range(config["tokens"]):
            payload = struct.pack("!" + "I" * len(tokens), *tokens)
            expected = ref_logits = None
            if reference:
                expected, ref_logits = reference.execute(len(tokens), payload)
            started = time.monotonic()
            results = []
            for stage_index, runner in enumerate(runners):
                input_bytes = len(payload)
                if args.json_events:
                    print(json.dumps({"event": "stage_start", "stage": stage_index, "step": index,
                                      "phase": "prefill" if index == 0 else "decode",
                                      "tokens": len(tokens), "input_bytes": input_bytes}), flush=True)
                result, payload = runner.execute(len(tokens), payload)
                results.append(result)
                if args.json_events:
                    print(json.dumps({"event": "stage_end", "stage": stage_index, "step": index,
                                      "input_bytes": input_bytes, "output_bytes": len(payload),
                                      "result": result}), flush=True)
            row = {"stages": results, "wall_ms": (time.monotonic() - started) * 1000,
                   "phase": "prefill" if index == 0 else "decode"}
            if reference:
                comparison = compare_logits(ref_logits, payload)
                comparison["same_greedy_token"] = expected["token"] == result["token"]
                row["comparison"] = comparison
                if comparison["max_abs_error"] > 0.02 or not comparison["same_greedy_token"]:
                    report["steps"].append(row)
                    raise RuntimeError("pipeline equivalence failed")
            report["steps"].append(row)
            output.extend(bytes.fromhex(result["piece_hex"]))
            report["text"] = output.decode(errors="replace")
            (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            if args.json_events:
                print(json.dumps({"event": "token", "step": index, "text": report["text"], **row}), flush=True)
            else:
                print(f"step={index} wall_ms={row['wall_ms']:.2f} text={report['text']!r}", flush=True)
            if result["eog"] and not reference:
                break
            tokens = [result["token"]]
        decode = report["steps"][1:]
        report["decode_steps_per_second"] = len(decode) * 1000 / sum(r["wall_ms"] for r in decode) if decode else None
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        if reference:
            reference.close()
        for runner in reversed(runners):
            runner.close()
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
