"""Sync (threading) lifecycle regression tests.

See ``tests/common/lifecycle_base.py`` for the scenario contract. Every
ordering decision is driven by threading events; the only yield used is a
barrier-protected scheduler tick for the parked GET.
"""
import io
import json
import threading

from engineio import packet
from engineio import payload as eio_payload
from engineio import server as eio_server

from tests.common.lifecycle_base import LifecycleTestMixin


class TestSyncLifecycle(LifecycleTestMixin):
    server_class = eio_server.Server

    def make_server(self, **kwargs):
        kwargs.setdefault('ping_interval', 1)
        kwargs.setdefault('ping_timeout', 1)
        return self.server_class(async_mode='threading', **kwargs)

    def _environ(self, method, sid, raw_body=None):
        query = 'EIO=4&transport=polling'
        if sid is not None:
            query += '&sid=' + sid
        environ = {'REQUEST_METHOD': method, 'QUERY_STRING': query}
        if raw_body is not None:
            data = raw_body.encode() if isinstance(raw_body, str) \
                else raw_body
            environ['CONTENT_LENGTH'] = str(len(data))
            environ['wsgi.input'] = io.BytesIO(data)
        return environ

    def _dispatch(self, server, environ):
        result = {}

        def start_response(status, headers, **kwargs):
            result['status'] = status
            result['headers'] = headers

        body = server.handle_request(environ, start_response)
        result['body'] = body[0] if isinstance(body, list) else body
        return result

    def connect(self, server):
        result = self._dispatch(
            server, self._environ('GET', None))
        for pkt in eio_payload.Payload(
                encoded_payload=result['body'].decode()).packets:
            if pkt.packet_type == packet.OPEN:
                data = pkt.data
                if not isinstance(data, dict):
                    data = json.loads(data)
                return data['sid']
        raise RuntimeError('no OPEN packet in connect response')

    def post(self, server, sid, raw_body):
        return self._dispatch(
            server, self._environ('POST', sid, raw_body))['status']

    def start_post(self, server, sid, raw_body):
        handle = {'status': None}
        thread = threading.Thread(
            target=self._run_post,
            args=(server, sid, raw_body, handle))
        handle['thread'] = thread
        thread.start()
        return handle

    def _run_post(self, server, sid, raw_body, handle):
        try:
            handle['status'] = self.post(server, sid, raw_body)
        except Exception as exc:  # pragma: no cover - surfaced by the test
            handle['status'] = 'EXC:' + type(exc).__name__
            handle['error'] = exc

    def join_handle(self, handle, timeout=5):
        handle['thread'].join(timeout)
        assert not handle['thread'].is_alive(), 'handle did not finish'

    def start_disconnect(self, server, sid):
        handle = {}
        thread = threading.Thread(
            target=self._run_disconnect, args=(server, sid, handle))
        handle['thread'] = thread
        thread.start()
        return handle

    def _run_disconnect(self, server, sid, handle):
        try:
            server.disconnect(sid)
            handle['status'] = '200 OK'
        except Exception as exc:  # pragma: no cover - surfaced by the test
            handle['status'] = 'EXC:' + type(exc).__name__
            handle['error'] = exc

    def handle_status(self, handle):
        return handle['status']

    def start_get(self, server, sid):
        handle = {}
        thread = threading.Thread(
            target=self._run_get, args=(server, sid, handle))
        handle['thread'] = thread
        handle['done'] = lambda: 'result' in handle
        handle['status'] = lambda: handle['result']['status']
        handle['body'] = lambda: handle['result']['body']
        thread.start()
        return {'handle': handle, 'done': handle['done'],
                'status': handle['status'], 'body': handle['body']}

    def _run_get(self, server, sid, handle):
        handle['result'] = self._dispatch(
            server, self._environ('GET', sid))

    def event(self):
        return threading.Event()

    def wait(self, barrier, timeout=5):
        assert barrier.wait(timeout), 'barrier timed out'

    def set(self, barrier):
        barrier.set()

    def sleep_tick(self):
        # Yield to let the long-polling GET reach queue.get(). The GET
        # completion itself is verified through an explicit join, not timing.
        threading.Event().wait(0.05)

    def install_handlers(self, server, record, message_gate=None,
                         disconnect_gate=None):
        def on_message(sid, data):
            record.append('message:' + str(data))
            if message_gate is not None:
                started, release = message_gate
                started.set()
                release.wait(5)

        def on_disconnect(sid, reason):
            record.append('disconnect:' + reason)
            if disconnect_gate is not None:
                started, release = disconnect_gate
                started.set()
                release.wait(5)

        server.on('message', on_message)
        server.on('disconnect', on_disconnect)
