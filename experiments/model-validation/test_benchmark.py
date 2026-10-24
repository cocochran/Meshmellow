import io
import json
import unittest
from unittest.mock import patch

from benchmark import allocation_evidence, events, measure


def stream(*items: dict | str) -> io.BytesIO:
    return io.BytesIO("".join(
        f"data: {item if isinstance(item, str) else json.dumps(item)}\n\n"
        for item in items
    ).encode())


class StreamingEvidenceTests(unittest.TestCase):
    def test_allocations_exclude_previous_server_sessions(self) -> None:
        self.assertEqual(allocation_evidence(
            "llama_server: initializing ...\nmodel buffer size = old\n"
            "llama_server: initializing ...\nmodel buffer size = current\n"
        ), ["model buffer size = current"])

    def test_sse_comments_and_multiline_data(self) -> None:
        self.assertEqual(list(events([
            b": keepalive\r\n", b"data: first\r\n", b"data: second\r\n", b"\r\n"
        ])), ["first\nsecond"])

    def test_complete_stream_records_text_usage_and_timings(self) -> None:
        response = stream(
            {"choices": [{"delta": {"role": "assistant"}}]},
            {"choices": [{"delta": {"content": "Hello"}}]},
            {"choices": [{"delta": {}, "finish_reason": "stop"}],
             "timings": {"predicted_n": 1}},
            {"choices": [], "usage": {"completion_tokens": 1}},
            "[DONE]",
        )
        with patch("urllib.request.urlopen", return_value=response):
            result = measure("http://localhost", {}, 5)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["text"], "Hello")
        self.assertEqual(result["usage"]["completion_tokens"], 1)
        self.assertEqual(result["server_timings"]["predicted_n"], 1)
        self.assertIsNotNone(result["ttft_seconds"])

    def test_truncated_stream_is_failure_even_with_partial_text(self) -> None:
        response = stream({"choices": [{"delta": {"content": "partial"}}]})
        with patch("urllib.request.urlopen", return_value=response):
            result = measure("http://localhost", {}, 5)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["text"], "partial")

    def test_empty_output_is_failure_even_with_done(self) -> None:
        response = stream(
            {"choices": [{"delta": {}, "finish_reason": "stop"}]}, "[DONE]"
        )
        with patch("urllib.request.urlopen", return_value=response):
            self.assertEqual(measure("http://localhost", {}, 5)["status"], "failed")

    def test_connection_failure_is_recorded(self) -> None:
        with patch("urllib.request.urlopen", side_effect=ConnectionResetError("RPC disconnected")):
            result = measure("http://localhost", {}, 5)
        self.assertEqual(result["status"], "failed")
        self.assertIn("RPC disconnected", result["error"])

    def test_malformed_stream_is_failure(self) -> None:
        with patch("urllib.request.urlopen", return_value=stream("{broken")):
            self.assertEqual(measure("http://localhost", {}, 5)["status"], "failed")

    def test_server_error_event_is_failure(self) -> None:
        with patch("urllib.request.urlopen", return_value=stream({"error": "worker lost"})):
            result = measure("http://localhost", {}, 5)
        self.assertEqual(result["status"], "failed")
        self.assertIn("worker lost", result["error"])

    def test_non_object_event_is_failure(self) -> None:
        with patch("urllib.request.urlopen", return_value=stream("[]")):
            self.assertEqual(measure("http://localhost", {}, 5)["status"], "failed")


if __name__ == "__main__":
    unittest.main()
