"""Offline transport contract: loopback servers, fake token, no official requests.

These tests exercise the HTTP boundary only. The queue decides whether a parsed
HTTP rejection proves no effect; ambiguous transport failures must never retry.
"""
import json
import os
import socket
import sys
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import submission_queue as sq

FAKE_TOKEN = "offline-test-token-never-a-real-credential-7138"
POST_PATH = "/api/v1/problems/muse_decode_layer/submissions"
PAYLOAD = {"code": "def kernel(x):\n    return x  # compilacao\n", "mode": "check"}


@contextmanager
def local_server(*responses):
    """Receive exactly the scripted requests; capture only synthetic test data."""
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            record = {"method": self.command, "path": self.path,
                      "headers": dict(self.headers.items()), "body": raw}
            with self.server.records_lock:
                self.server.records.append(record)
                response = self.server.responses.pop(0) if self.server.responses else {
                    "status": 503, "body": {"error": "unexpected repeated request"}}
            self.close_connection = True
            if response.get("action") == "close":
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()
                return
            if response.get("action") == "wait":
                self.server.release.wait(timeout=5)
                return
            body = response.get("body", {"job_id": "job-local-1"})
            raw_body = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
            self.send_response(response.get("status", 200))
            self.send_header("Content-Type", "application/json")
            if not response.get("omit_length"):
                self.send_header("Content-Length", str(len(raw_body)))
            self.send_header("Connection", "close")
            for name, value in response.get("headers", {}).items():
                self.send_header(name, value)
            self.end_headers()
            try:
                self.wfile.write(raw_body)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.records = []
    server.records_lock = threading.Lock()
    server.responses = list(responses)
    server.release = threading.Event()
    server.url = "http://127.0.0.1:%d" % server.server_port
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@contextmanager
def forbid_network():
    """Invalid URL tests cannot contact any host, even if validation regresses."""
    with mock.patch("socket.getaddrinfo", side_effect=AssertionError("validation attempted DNS")), \
         mock.patch("socket.socket.connect", side_effect=AssertionError("validation attempted connect")):
        yield


class TestHttpTransport(unittest.TestCase):
    def setUp(self):
        self.environment = mock.patch.dict(os.environ, {sq.TOKEN_ENV: FAKE_TOKEN})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def transport(self, server, **overrides):
        options = {"base_url": server.url, "token_env": sq.TOKEN_ENV,
                   "timeout": 1, "max_response_bytes": 1024 * 1024,
                   "allow_loopback": True}
        options.update(overrides)
        return sq.HttpTransport(**options)

    def secret_safe_exception(self, exception):
        self.assertNotIn(FAKE_TOKEN, str(exception), "exception must not disclose credentials")

    def test_post_transmits_only_code_and_mode_and_authenticates(self):
        expected = {"job_id": "job-local-1", "content_hash": "provider-hash"}
        with local_server({"status": 202, "body": expected}) as server:
            result = self.transport(server).post(POST_PATH, dict(PAYLOAD))
            self.assertEqual(result, (202, expected))
            self.assertEqual(len(server.records), 1)
            record = server.records[0]
            self.assertEqual((record["method"], record["path"]), ("POST", POST_PATH))
            self.assertEqual(json.loads(record["body"]), PAYLOAD)
            headers = {key.lower(): value for key, value in record["headers"].items()}
            self.assertTrue(headers.get("authorization") == "Bearer " + FAKE_TOKEN,
                            "local server must receive the configured bearer token")
            self.assertIn("application/json", headers.get("content-type", ""))
            self.assertNotIn(FAKE_TOKEN.encode(), record["body"])

    def test_configured_token_environment_is_used(self):
        alternate = FAKE_TOKEN + "-alternate"
        with local_server({"body": {"ok": True}}) as server, \
             mock.patch.dict(os.environ, {"OPENKERNEL_TEST_ONLY_TOKEN": alternate}):
            self.transport(server, token_env="OPENKERNEL_TEST_ONLY_TOKEN").get("/api/v1/me")
            headers = {key.lower(): value for key, value in server.records[0]["headers"].items()}
            self.assertTrue(headers.get("authorization") == "Bearer " + alternate,
                            "configured token variable must override the default")

    def test_official_origin_can_be_configured_without_network(self):
        with forbid_network():
            transport = sq.HttpTransport(base_url="https://openkernelchallenge.dev",
                                         token_env=sq.TOKEN_ENV, timeout=1,
                                         max_response_bytes=1024 * 1024,
                                         allow_loopback=False)
            self.assertIsInstance(transport, sq.HttpTransport)

    def test_all_allowed_get_endpoints(self):
        for path in ["/api/v1/problems", "/api/v1/me", "/api/v1/jobs", "/api/v1/jobs/job_123-safe"]:
            with self.subTest(path=path), local_server({"body": {"ok": True}}) as server:
                self.assertEqual(self.transport(server).get(path), (200, {"ok": True}))
                self.assertEqual(len(server.records), 1)
                self.assertEqual(server.records[0]["path"], path)
                headers = {key.lower(): value for key, value in server.records[0]["headers"].items()}
                self.assertTrue(headers.get("authorization") == "Bearer " + FAKE_TOKEN)

    def test_get_preserves_json_arrays(self):
        with local_server({"body": [{"id": "muse_decode_layer"}]}) as server:
            self.assertEqual(self.transport(server).get("/api/v1/problems"),
                             (200, [{"id": "muse_decode_layer"}]))

    def test_http_json_rejections_return_status_body_without_headers_or_retry(self):
        for status in [401, 403, 409, 422, 429, 500, 503]:
            body = {"error": "controlled rejection", "status": status}
            response = {"status": status, "body": body,
                        "headers": {"Retry-After": "120", "X-Private-Test": FAKE_TOKEN}}
            with self.subTest(status=status), local_server(response) as server:
                self.assertEqual(self.transport(server).post(POST_PATH, PAYLOAD), (status, body))
                self.assertEqual(len(server.records), 1, "transport must not retry HTTP failures")

    def test_redirect_is_never_followed_or_replayed(self):
        for method in ["get", "post"]:
            for status in [302, 307]:
                with self.subTest(method=method, status=status), local_server({"body": {"escaped": True}}) as target:
                    response = {"status": status, "body": {"redirect": True},
                                "headers": {"Location": target.url + "/api/v1/me"}}
                    with local_server(response) as source:
                        transport = self.transport(source)
                        expected_error = sq.TransportUnknownOutcome if method == "post" else sq.TransportReadError
                        try:
                            result = transport.post(POST_PATH, PAYLOAD) if method == "post" else transport.get("/api/v1/me")
                        except expected_error as error:
                            self.secret_safe_exception(error)
                        else:
                            # Returning the original 3xx is also valid; classification belongs to the queue.
                            self.assertEqual(result, (status, {"redirect": True}))
                        self.assertEqual(len(source.records), 1)
                        self.assertEqual(target.records, [], "Location must not receive a request or bearer token")

    def test_connection_cut_after_body_is_unknown_and_never_replayed(self):
        with local_server({"action": "close"}) as server:
            with self.assertRaises(sq.TransportUnknownOutcome) as caught:
                self.transport(server).post(POST_PATH, PAYLOAD)
            self.secret_safe_exception(caught.exception)
            self.assertEqual(len(server.records), 1)
            self.assertEqual(json.loads(server.records[0]["body"]), PAYLOAD)

    def test_timeout_after_body_is_unknown_and_never_replayed(self):
        with local_server({"action": "wait"}) as server:
            with self.assertRaises(sq.TransportUnknownOutcome) as caught:
                self.transport(server, timeout=0.1).post(POST_PATH, PAYLOAD)
            self.secret_safe_exception(caught.exception)
            self.assertEqual(len(server.records), 1)
            self.assertEqual(json.loads(server.records[0]["body"]), PAYLOAD)

    def test_get_connection_failure_is_read_error_without_retry(self):
        with local_server({"action": "close"}) as server:
            with self.assertRaises(sq.TransportReadError) as caught:
                self.transport(server).get("/api/v1/me")
            self.secret_safe_exception(caught.exception)
            self.assertEqual(len(server.records), 1)

    def test_invalid_json_is_typed_by_operation_and_does_not_leak_response(self):
        for method, expected_error in [("get", sq.TransportReadError), ("post", sq.TransportUnknownOutcome)]:
            for status in [200, 502]:
                response = {"status": status, "body": ("invalid-json " + FAKE_TOKEN).encode()}
                with self.subTest(method=method, status=status), local_server(response) as server:
                    transport = self.transport(server)
                    with self.assertRaises(expected_error) as caught:
                        transport.post(POST_PATH, PAYLOAD) if method == "post" else transport.get("/api/v1/me")
                    self.secret_safe_exception(caught.exception)
                    self.assertEqual(len(server.records), 1)

    def test_response_bound_applies_with_or_without_content_length(self):
        for method, expected_error in [("get", sq.TransportReadError), ("post", sq.TransportUnknownOutcome)]:
            for omit_length in [False, True]:
                body = {"padding": FAKE_TOKEN + "x" * 4096}
                with self.subTest(method=method, omit_length=omit_length), \
                     local_server({"body": body, "omit_length": omit_length}) as server:
                    transport = self.transport(server, max_response_bytes=128)
                    with self.assertRaises(expected_error) as caught:
                        transport.post(POST_PATH, PAYLOAD) if method == "post" else transport.get("/api/v1/me")
                    self.secret_safe_exception(caught.exception)
                    self.assertEqual(len(server.records), 1)

    def test_invalid_paths_fail_before_any_network(self):
        cases = [
            ("post", "/api/v1/rules/accept"),
            ("get", "/api/v1/rules/accept"),
            ("post", "/api/v1/me"),
            ("get", POST_PATH),
            ("post", "/api/v1/problems/not_a_kernel/submissions"),
            ("post", POST_PATH + "/../submissions"),
            ("get", "/api/v1/jobs/../../rules/accept"),
            ("get", "/api/v1/jobs/%2e%2e%2frules"),
            ("get", "https://evil.invalid/api/v1/me"),
            ("post", "https://evil.invalid" + POST_PATH),
            ("get", "//evil.invalid/api/v1/me"),
        ]
        with local_server() as server:
            transport = self.transport(server)
            for method, path in cases:
                with self.subTest(method=method, path=path), forbid_network():
                    with self.assertRaises(sq.TransportNoEffect) as caught:
                        transport.post(path, PAYLOAD) if method == "post" else transport.get(path)
                    self.secret_safe_exception(caught.exception)
            self.assertEqual(server.records, [])

    def test_invalid_origins_are_rejected_at_construction(self):
        bases = [
            "https://evil.invalid", "http://openkernelchallenge.dev",
            "https://openkernelchallenge.dev.evil.invalid",
            "https://openkernelchallenge.dev@evil.invalid",
            "https://openkernelchallenge.dev/extra-path",
            "https://openkernelchallenge.dev?redirect=1",
            "https://openkernelchallenge.dev#fragment",
            "http://127.0.0.1:12345",
        ]
        for base in bases:
            with self.subTest(base=base), forbid_network():
                with self.assertRaises(sq.TransportNoEffect) as caught:
                    sq.HttpTransport(base_url=base, token_env=sq.TOKEN_ENV,
                                     timeout=1, max_response_bytes=1024 * 1024,
                                     allow_loopback=False)
                self.secret_safe_exception(caught.exception)

    def test_missing_token_fails_before_network(self):
        with local_server() as server, mock.patch.dict(os.environ):
            os.environ.pop(sq.TOKEN_ENV, None)
            transport = self.transport(server)
            with forbid_network(), self.assertRaises(sq.TransportNoEffect) as caught:
                transport.post(POST_PATH, PAYLOAD)
            self.secret_safe_exception(caught.exception)
            self.assertEqual(server.records, [])

    def test_proxy_environment_is_ignored(self):
        with local_server({"status": 502, "body": {"error": "proxy was used"}}) as proxy:
            proxy_environment = {key: proxy.url for key in [
                "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]}
            proxy_environment.update({"NO_PROXY": "", "no_proxy": ""})
            with local_server({"body": {"job_id": "direct-only"}}) as server, \
                 mock.patch.dict(os.environ, proxy_environment):
                self.assertEqual(self.transport(server).post(POST_PATH, PAYLOAD),
                                 (200, {"job_id": "direct-only"}))
                self.assertEqual(len(server.records), 1)
                self.assertEqual(proxy.records, [], "ambient proxy must not receive the bearer token")


if __name__ == "__main__":
    unittest.main(verbosity=2)
