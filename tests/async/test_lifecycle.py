"""Asyncio lifecycle regression tests.

Same scenarios and assertions as the sync suite in
``tests/common/test_lifecycle.py``, driven through the real ``AsyncServer``
HTTP path with an in-process request translation. Ordering is controlled by
asyncio events; no sleeps are used to guess execution order.
"""
import asyncio
import json

from engineio import async_server
from engineio import packet
from engineio import payload as eio_payload

from tests.common.lifecycle_base import (
    CLOSE,
    MSG_AFTER,
    MSG_BEFORE,
    MSG_LATE,
    join_packets,
)


class _AsyncInput:
    def __init__(self, body):
        self._body = body

    async def read(self, length=None):
        if length is None:
            data, self._body = self._body, b''
        else:
            data, self._body = self._body[:length], self._body[length:]
        return data


class TestAsyncLifecycle:
    server_class = async_server.AsyncServer

    def make_server(self, **kwargs):
        kwargs.setdefault('ping_interval', 1)
        kwargs.setdefault('ping_timeout', 1)
        return self.server_class(async_mode='asgi', **kwargs)

    def _environ(self, method, sid, raw_body=None):
        query = 'EIO=4&transport=polling'
        if sid is not None:
            query += '&sid=' + sid
        environ = {'REQUEST_METHOD': method, 'QUERY_STRING': query,
                   'HTTP_CONNECTION': '', 'HTTP_UPGRADE': ''}
        if raw_body is not None:
            data = raw_body.encode() if isinstance(raw_body, str) \
                else raw_body
            environ['CONTENT_LENGTH'] = str(len(data))
            environ['wsgi.input'] = _AsyncInput(data)
        return environ

    async def _dispatch(self, server, environ):
        original = server._async['translate_request']
        server._async['translate_request'] = lambda *a, **k: environ
        try:
            return await server.handle_request('request')
        finally:
            server._async['translate_request'] = original

    async def _make_response(self, status, headers, body, environ):
        return {'status': status, 'headers': headers, 'response': body}

    async def connect(self, server):
        server._async['make_response'] = self._make_response
        response = await self._dispatch(
            server, self._environ('GET', None))
        for pkt in eio_payload.Payload(
                encoded_payload=response['response'].decode()).packets:
            if pkt.packet_type == packet.OPEN:
                data = pkt.data
                if not isinstance(data, dict):
                    data = json.loads(data)
                return data['sid']
        raise RuntimeError('no OPEN packet in connect response')

    async def post(self, server, sid, raw_body):
        response = await self._dispatch(
            server, self._environ('POST', sid, raw_body))
        return response['status']

    def start_post(self, server, sid, raw_body):
        async def run_post():
            status = await self.post(server, sid, raw_body)
            return {'status': status}

        return asyncio.ensure_future(run_post())

    async def join_handle(self, handle, timeout=5):
        return await asyncio.wait_for(handle, timeout)

    async def handle_status(self, handle):
        return (await self.join_handle(handle))['status']

    def start_get(self, server, sid):
        task = asyncio.ensure_future(
            self._dispatch(server, self._environ('GET', sid)))
        return {
            'handle': task,
            'done': lambda: task.done(),
            'status': lambda: task.result()['status'],
            'body': lambda: task.result()['response'],
        }

    def event(self):
        return asyncio.Event()

    async def wait(self, barrier, timeout=5):
        await asyncio.wait_for(barrier.wait(), timeout)

    def set(self, barrier):
        barrier.set()

    async def sleep_tick(self):
        # Cooperative yield so the competing task can park on the queue.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    def install_handlers(self, server, record, message_gate=None,
                         disconnect_gate=None):
        async def on_message(sid, data):
            record.append('message:' + str(data))
            if message_gate is not None:
                started, release = message_gate
                started.set()
                await release.wait()

        async def on_disconnect(sid, reason):
            record.append('disconnect:' + reason)
            if disconnect_gate is not None:
                started, release = disconnect_gate
                started.set()
                await release.wait()

        server.on('message', on_message)
        server.on('disconnect', on_disconnect)

    def start_disconnect(self, server, sid):
        async def run_disconnect():
            await server.disconnect(sid)
            return {'status': '200 OK'}

        return asyncio.ensure_future(run_disconnect())

    # -----------------------------------------------------------------
    # Scenario 1: MESSAGE, CLOSE, MESSAGE in one polling payload
    # -----------------------------------------------------------------
    async def test_message_after_close_in_same_payload_is_ignored(self):
        server = self.make_server(async_handlers=False,
                                  monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = await self.connect(server)

        status = await self.post(
            server, sid, join_packets(MSG_BEFORE, CLOSE, MSG_AFTER))

        assert status.startswith('200')
        assert sid not in server.sockets
        assert [e for e in record if e.startswith('message:')] == \
            ['message:before']
        assert record.count('disconnect:client disconnect') == 1

    # -----------------------------------------------------------------
    # Scenario 2: a second POST that was not admitted before closing
    # -----------------------------------------------------------------
    async def test_concurrent_post_admitted_before_close_completes(self):
        server = self.make_server(monitor_clients=False)
        record = []
        first_message_started = self.event()
        release_first_message = self.event()
        disconnect_started = self.event()
        release_disconnect = self.event()
        self.install_handlers(
            server, record,
            message_gate=(first_message_started, release_first_message),
            disconnect_gate=(disconnect_started, release_disconnect))
        sid = await self.connect(server)

        first = self.start_post(
            server, sid, join_packets(MSG_BEFORE, CLOSE))
        await self.wait(first_message_started)
        await self.wait(disconnect_started)

        second = self.start_post(server, sid, MSG_LATE)
        assert (await self.handle_status(second)).startswith('400')
        assert 'message:late' not in record

        self.set(release_first_message)
        self.set(release_disconnect)
        assert (await self.handle_status(first)).startswith('200')
        assert [e for e in record if e.startswith('message:')] == \
            ['message:before']
        assert record.count('disconnect:client disconnect') == 1
        assert sid not in server.sockets

    # -----------------------------------------------------------------
    # Scenario 3: a GET already parked when closing starts
    # -----------------------------------------------------------------
    async def test_waiting_get_is_released_by_close(self):
        server = self.make_server(monitor_clients=False)
        record = []
        disconnect_started = self.event()
        release_disconnect = self.event()
        self.install_handlers(
            server, record,
            disconnect_gate=(disconnect_started, release_disconnect))
        sid = await self.connect(server)

        waiting_get = self.start_get(server, sid)
        await self.sleep_tick()
        assert not waiting_get['done']()

        close_post = self.start_post(server, sid, CLOSE)
        await self.wait(disconnect_started)
        self.set(release_disconnect)
        await self.join_handle(close_post)
        await self.join_handle(waiting_get['handle'])

        assert waiting_get['status']().startswith('200')
        decoded = eio_payload.Payload(
            encoded_payload=waiting_get['body']().decode()).packets
        assert decoded == []
        assert record == ['disconnect:client disconnect']

    # -----------------------------------------------------------------
    # Scenario 4: new POST while the disconnect callback is still running
    # -----------------------------------------------------------------
    async def test_new_post_blocked_while_disconnect_callback_runs(self):
        server = self.make_server(monitor_clients=False)
        record = []
        disconnect_started = self.event()
        release_disconnect = self.event()
        self.install_handlers(
            server, record,
            disconnect_gate=(disconnect_started, release_disconnect))
        sid = await self.connect(server)

        close_post = self.start_post(server, sid, CLOSE)
        await self.wait(disconnect_started)

        late_post = self.start_post(server, sid, MSG_LATE)
        assert (await self.handle_status(late_post)).startswith('400')
        assert 'message:late' not in record

        self.set(release_disconnect)
        await self.join_handle(close_post)
        assert record == ['disconnect:client disconnect']
        assert sid not in server.sockets

    # -----------------------------------------------------------------
    # Scenario 5: server initiated disconnect
    # -----------------------------------------------------------------
    async def test_server_disconnect_rejects_further_posts(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = await self.connect(server)

        first_get = self.start_get(server, sid)
        second_get = self.start_get(server, sid)
        await self.sleep_tick()
        disconnect_handle = self.start_disconnect(server, sid)
        await self.join_handle(disconnect_handle)
        await self.join_handle(first_get['handle'])
        await self.join_handle(second_get['handle'])

        assert record == ['disconnect:server disconnect']
        assert sid not in server.sockets
        assert (await self.post(server, sid, MSG_LATE)).startswith('400')
        assert 'message:late' not in record

    # -----------------------------------------------------------------
    # Scenario 6: malformed payload is not a clean disconnect
    # -----------------------------------------------------------------
    async def test_malformed_payload_is_bad_request_without_disconnect(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = await self.connect(server)

        assert (await self.post(server, sid, '\x1e')).startswith('400')
        assert record == []
        assert sid in server.sockets

    async def test_unknown_packet_type_is_protocol_error_disconnect(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = await self.connect(server)

        assert (await self.post(server, sid, '9')).startswith('400')
        assert record == ['disconnect:transport error']
        assert sid not in server.sockets

    async def test_valid_close_is_distinct_from_parse_failure(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = await self.connect(server)

        assert (await self.post(server, sid, CLOSE)).startswith('200')
        assert record == ['disconnect:client disconnect']
        assert sid not in server.sockets

        assert (await self.post(server, sid, MSG_LATE)).startswith('400')
        assert record == ['disconnect:client disconnect']

    # -----------------------------------------------------------------
    # Scenario 7: disconnect is reported exactly once
    # -----------------------------------------------------------------
    async def test_disconnect_event_fires_once(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = await self.connect(server)

        await self.post(server, sid, CLOSE)
        second_close = self.start_post(server, sid, CLOSE)
        assert (await self.handle_status(second_close)).startswith('400')

        assert record == ['disconnect:client disconnect']
