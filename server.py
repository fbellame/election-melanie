#!/usr/bin/env python3
"""WSGI app for serving the election map + visited addresses API via gunicorn."""

import json
import mimetypes
import os
import threading

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('DATA_DIR', BASE_DIR)
VISITED_FILE = os.path.join(DATA_DIR, 'visited.json')

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

# Thread lock for visited.json writes
_lock = threading.Lock()


def _load_visited():
    if os.path.exists(VISITED_FILE):
        with open(VISITED_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def _save_visited(data):
    with open(VISITED_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def _read_body(environ):
    try:
        length = int(environ.get('CONTENT_LENGTH', 0))
    except ValueError:
        length = 0
    if length > 0:
        return json.loads(environ['wsgi.input'].read(length))
    return {}


def _json_response(start_response, data, status='200 OK'):
    body = json.dumps(data, ensure_ascii=False).encode('utf-8')
    start_response(status, [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(body))),
    ])
    return [body]


def app(environ, start_response):
    path = environ.get('PATH_INFO', '/')
    method = environ.get('REQUEST_METHOD', 'GET')

    # --- API: Visited addresses ---
    if path == '/api/visited':
        if method == 'GET':
            visited = _load_visited()
            return _json_response(start_response, visited)

        elif method == 'POST':
            body = _read_body(environ)
            addr_id = body.get('id')
            user = body.get('user', 'anonymous')
            if not addr_id:
                return _json_response(start_response, {'error': 'missing id'}, '400 Bad Request')
            with _lock:
                visited = _load_visited()
                visited[addr_id] = {
                    'user': user,
                    'timestamp': body.get('timestamp', ''),
                }
                _save_visited(visited)
            return _json_response(start_response, {'ok': True})

        elif method == 'DELETE':
            body = _read_body(environ)
            addr_id = body.get('id')
            if not addr_id:
                return _json_response(start_response, {'error': 'missing id'}, '400 Bad Request')
            with _lock:
                visited = _load_visited()
                visited.pop(addr_id, None)
                _save_visited(visited)
            return _json_response(start_response, {'ok': True})

    # --- API: Bulk import visited ---
    if path == '/api/visited/import' and method == 'POST':
        body = _read_body(environ)
        entries = body.get('entries', {})
        with _lock:
            visited = _load_visited()
            visited.update(entries)
            _save_visited(visited)
        return _json_response(start_response, {'ok': True, 'total': len(visited)})

    # --- Static files ---
    static_path = path.lstrip('/')
    if not static_path or static_path == '/':
        static_path = 'index.html'

    file_path = os.path.join(BASE_DIR, static_path)
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
