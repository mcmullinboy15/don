"""Simple HTTP API server for the Bazel example."""

import http.server
import json
import os
import sys

sys.path.insert(0, ".")
from libs.common.src.utils import greet

# Read port from PORT env var (set by don's proxy), or default to 8080.
PORT = int(os.environ.get("PORT", "8080"))


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        body = json.dumps({"message": greet("world")})
        self.wfile.write(body.encode())

    def log_message(self, format, *args):
        print(f"[api] {args[0]}")


if __name__ == "__main__":
    server = http.server.HTTPServer(("127.0.0.1", PORT), Handler)
    print(f"API server listening on 127.0.0.1:{PORT}")
    server.serve_forever()
