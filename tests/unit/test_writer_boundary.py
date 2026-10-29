"""Exercise the actual HTTP boundary without starting a Spark JVM in static CI."""

import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from worker import server


class WriterBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.previous_token = os.environ.get("WORKER_TOKEN")
        os.environ["WORKER_TOKEN"] = "unit-test-token"
        self.http = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.http.server_port}"

    def tearDown(self):
        self.http.shutdown()
        self.http.server_close()
        self.thread.join()
        if self.previous_token is None:
            os.environ.pop("WORKER_TOKEN", None)
        else:
            os.environ["WORKER_TOKEN"] = self.previous_token

    def rejected_post(self, path, data):
        req = Request(
            self.url + path,
            data=json.dumps(data).encode(),
            headers={"Authorization": "Bearer unit-test-token", "Content-Type": "application/json"},
        )
        try:
            urlopen(req, timeout=3)
        except HTTPError as error:
            return error.code, json.load(error)
        self.fail("Writer request unexpectedly accepted")

    def test_batch_is_rejected_while_realtime_owns_writer(self):
        server.LOCK.acquire()
        server.REALTIME_MODE = True
        try:
            code, response = self.rejected_post("/jobs", {"stage": "load", "date": "2026-01-30"})
            self.assertEqual(code, 409)
            self.assertTrue(response["realtime"])
        finally:
            server.REALTIME_MODE = False
            server.LOCK.release()

    def test_realtime_cannot_start_while_a_batch_owns_writer(self):
        server.LOCK.acquire()
        try:
            code, _ = self.rejected_post("/realtime/start", {})
            self.assertEqual(code, 409)
        finally:
            server.LOCK.release()
