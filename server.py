#!/usr/bin/env python3
"""WSGI app for serving the election map as static files via gunicorn."""

import mimetypes
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MIME_OVERRIDES = {
    '.json': 'application/json',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.html': 'text/html',
    '.png': 'image/png',
    '.jpeg': 'image/jpeg',
    '.jpg': 'image/jpeg',
    '.ico': 'image/x-icon',
}


def app(environ, start_response):
    path = environ.get('PATH_INFO', '/').lstrip('/')
    if not path or path == '/':
        path = 'index.html'

    file_path = os.path.join(BASE_DIR, path)

    # Prevent directory traversal
    file_path = os.path.realpath(file_path)
    if not file_path.startswith(os.path.realpath(BASE_DIR)):
        start_response('403 Forbidden', [('Content-Type', 'text/plain')])
        return [b'Forbidden']

    if os.path.isfile(file_path):
        ext = os.path.splitext(file_path)[1].lower()
        content_type = MIME_OVERRIDES.get(ext) or mimetypes.guess_type(file_path)[0] or 'application/octet-stream'
        with open(file_path, 'rb') as f:
            content = f.read()
        start_response('200 OK', [
            ('Content-Type', content_type),
            ('Content-Length', str(len(content))),
        ])
        return [content]

    start_response('404 Not Found', [('Content-Type', 'text/plain')])
    return [b'Not Found']
