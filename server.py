#!/usr/bin/env python3
"""Simple HTTP server for serving the election map app."""

import http.server
import os
import socketserver

PORT = int(os.environ.get("PORT", 8000))

Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("0.0.0.0", PORT), Handler) as httpd:
    print(f"Serving on port {PORT}")
    httpd.serve_forever()
