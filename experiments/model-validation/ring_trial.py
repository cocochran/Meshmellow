"""Mac prefix -> peer chain -> final peer callback, without returning activations."""

import argparse
import json
from pathlib import Path
import struct
import time

from shard_trial import Runner, SocketRunner


class RingRunner(SocketRunner):
    def execute_ring(self, n: int, payload: bytes, event) -> dict:
        self.input.write(struct.pack("!IIIII", 0x4D534831, 2, self.step, self.pos, n) + payload)
        self.input.flush()
        while True:
            response = json.loads(self.line(240))
            if response.get("kind") == "progress":
                event(response)
                continue
            if response.get("kind") != "result" or response.get("step") != self.step or response.get("n_floats") != 0:
                raise ValueError("invalid ring callback")
            self.step += 1
            self.pos += n
            return response


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--json-events", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"status": "running", "transport": "Iroh peer-to-peer ring; final callback directly to Mac",
              "config": config, "steps": [], "activation_bytes_returned_to_mac": 0}
    prefix = ring = None

    def emit(value: dict) -> None:
        if args.json_events:
            print(json.dumps(value), flush=True)

    try:
        prefix = Runner(config["stages"][0]["command"], args.output / "mac-native.log")
        ring = RingRunner(config["ring_port"])
        if ring.info.get("ring_protocol") != 1:
            raise ValueError("peer is not running the P2P ring")
        information = [prefix.info, *ring.info["chain_stages"]]
        if len(information) != len(config["stages"]):
            raise ValueError("wrong chain length")
        begin = 0
        for specification, info in zip(config["stages"], information):
            if [info["begin"], info["end"]] != specification["range"] or info["begin"] != begin:
                raise ValueError("wrong chain range")
            if any(info[key] != prefix.info[key] for key in ["layers", "embd", "vocab"]):
                raise ValueError("incompatible chain metadata")
            if info.get("source_sha256") != config["source_sha256"]:
                raise ValueError("wrong source model")
            begin = info["end"]
        if begin != prefix.info["layers"]:
            raise ValueError("incomplete model")
        report["initialization"] = information
        emit({"event": "ready", "stages": information})
        indices = {info["begin"]: index for index, info in enumerate(information)}
        ended = set()

        def progress(message: dict) -> None:
            if message["step"] != ring.step:
                return
            index = indices[message["begin"]]
            if message["event"] == "stage_end":
                ended.add(index)
            emit({**message, "stage": index})

        tokens = prefix.info["tokens"]
        if not 1 <= len(tokens) <= 128:
            raise ValueError("prompt exceeds the 128-token diagnostic prefill limit")
        output = bytearray()
        for step in range(config["tokens"]):
            ended.clear()
            payload = struct.pack("!" + "I" * len(tokens), *tokens)
            started = time.monotonic()
            emit({"event": "stage_start", "stage": 0, "step": step, "input_bytes": len(payload),
                  "phase": "prefill" if step == 0 else "decode"})
            pre, hidden = prefix.execute(len(tokens), payload)
            emit({"event": "stage_end", "stage": 0, "step": step, "input_bytes": len(payload),
                  "output_bytes": len(hidden), "result": pre})
            chain_started = time.monotonic()
            result = ring.execute_ring(len(tokens), hidden, progress)
            chain_ms = (time.monotonic() - chain_started) * 1000
            metrics = result["chain_metrics"]
            if len(metrics) != len(information) - 1:
                raise ValueError("missing peer execution metrics")
            for index, metric in enumerate(metrics, 1):
                if metric.get("node_id") != information[index].get("node_id") or metric["step"] != step:
                    raise ValueError("incorrect stage provenance")
                if index not in ended:
                    emit({"event": "stage_end", "stage": index, "step": step,
                          "input_bytes": metric["request_bytes"] - 20,
                          "output_bytes": metric["response_tensor_bytes"], "result": metric})
            row = {"stages": [pre, *metrics], "phase": "prefill" if step == 0 else "decode",
                   "wall_ms": (time.monotonic() - started) * 1000, "ring_wall_ms": chain_ms,
                   "transport_and_coordination_ms": max(0, chain_ms - sum(m["compute_ms"] for m in metrics))}
            report["steps"].append(row)
            output.extend(bytes.fromhex(result["piece_hex"]))
            report["text"] = output.decode(errors="replace")
            emit({"event": "token", "step": step, "text": report["text"], **row})
            (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            if result["eog"]:
                break
            tokens = [result["token"]]
        decode = report["steps"][1:]
        report["decode_steps_per_second"] = len(decode) * 1000 / sum(r["wall_ms"] for r in decode) if decode else None
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        if ring:
            ring.close()
        if prefix:
            prefix.close()
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
