#!/usr/bin/env python3
"""WSGI app for serving the election map + visited API via gunicorn.

Visited entries are stored per person record in Upstash Redis
(persistent, free tier).
Set UPSTASH_REDIS_URL and UPSTASH_REDIS_TOKEN environment variables.
Falls back to local JSON file if Redis is not configured.
"""

import json
import mimetypes
import os
import threading
import urllib.error
import urllib.parse
import urllib.request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Upstash Redis config
UPSTASH_URL = os.environ.get('UPSTASH_REDIS_URL', '')
UPSTASH_TOKEN = os.environ.get('UPSTASH_REDIS_TOKEN', '')
# Legacy key name kept for compatibility with existing deployed data.
REDIS_KEY = 'visited_addresses'

# Fallback local file (when Redis not configured)
VISITED_FILE = os.path.join(BASE_DIR, 'visited.json')
_lock = threading.Lock()

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


# --- Upstash Redis helpers (REST API, no driver needed) ---

def _redis_request(method, path, body=None):
    """Make a request to Upstash Redis REST API."""
    url = f"{UPSTASH_URL}{path}"
    data = json.dumps(body).encode('utf-8') if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Authorization', f'Bearer {UPSTASH_TOKEN}')
    req.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError, OSError) as e:
        print(f"Redis error: {e}")
        return None


def _redis_get_all():
    """Get all visited entries from Redis."""
    result = _redis_request('GET', f'/get/{REDIS_KEY}')
    if result and result.get('result'):
        try:
            return json.loads(result['result'])
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _redis_save_all(data):
    """Save all visited entries to Redis."""
    payload = json.dumps(data, ensure_ascii=False)
    # Use SET command via REST API
    result = _redis_request('POST', '', body=['SET', REDIS_KEY, payload])
    return result is not None


def _use_redis():
    return bool(UPSTASH_URL and UPSTASH_TOKEN)


# --- Local file fallback ---

def _file_load():
    if os.path.exists(VISITED_FILE):
        with open(VISITED_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def _file_save(data):
    with open(VISITED_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


# --- Storage abstraction ---

def load_visited():
    if _use_redis():
        return _redis_get_all()
    return _file_load()


def save_visited(data):
    if _use_redis():
        return _redis_save_all(data)
    _file_save(data)


# --- WSGI helpers ---

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


# --- WSGI app ---

def app(environ, start_response):
    path = environ.get('PATH_INFO', '/')
    method = environ.get('REQUEST_METHOD', 'GET')

    # --- API: Visited entries ---
    if path == '/api/visited':
        if method == 'GET':
            visited = load_visited()
            return _json_response(start_response, visited)

        elif method == 'POST':
            body = _read_body(environ)
            addr_id = body.get('id')
            user = body.get('user', 'anonymous')
            if not addr_id:
                return _json_response(start_response, {'error': 'missing id'}, '400 Bad Request')
            with _lock:
                visited = load_visited()
                visited[addr_id] = {
                    'user': user,
                    'timestamp': body.get('timestamp', ''),
                }
                save_visited(visited)
            return _json_response(start_response, {'ok': True})

        elif method == 'DELETE':
            body = _read_body(environ)
            addr_id = body.get('id')
            if not addr_id:
                return _json_response(start_response, {'error': 'missing id'}, '400 Bad Request')
            with _lock:
                visited = load_visited()
                visited.pop(addr_id, None)
                save_visited(visited)
            return _json_response(start_response, {'ok': True})

    # --- API: Bulk import visited ---
    if path == '/api/visited/import' and method == 'POST':
        body = _read_body(environ)
        entries = body.get('entries', {})
        with _lock:
            visited = load_visited()
            visited.update(entries)
            save_visited(visited)
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
