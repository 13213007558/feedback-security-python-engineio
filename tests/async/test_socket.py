import asyncio
import time
from unittest import mock

import pytest

from engineio import async_socket
from engineio import exceptions
from engineio import packet
from engineio import payload


class FakeAsyncWebSocket:
    """A scripted WebSocket transport for the async socket.

    ``responses`` is consumed by ``wait()``. Exception instances are raised;
    exception classes are raised with a generic message. ``hook`` may be a
    coroutine function and runs once, before the numbered ``wait`` call.
    """

    def __init__(self, responses=()):
        self.responses = list(responses)
        self.sent = []
        self.closed = False
        self.wait_calls = 0
        self.hook_at_wait = None
        self.hook = None

    async def wait(self):
        self.wait_calls += 1
        if self.hook is not None and self.wait_calls == self.hook_at_wait:
            await self.hook()
        if not self.responses:
            return None
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if isinstance(response, type) and issubclass(response, Exception):
            raise response('websocket failure')
        return response

    async def send(self, message):
        self.sent.append(message)

    async def close(self):
        self.closed = True


class RecordingAsyncQueue(asyncio.Queue):
    """Queue that records get/put operations for packet-routing assertions."""

    def __init__(self):
        super().__init__()
        self.events = []

    async def put(self, item):
        self.events.append(('put', item))
        await super().put(item)

    def put_nowait(self, item):
        self.events.append(('put', item))
        return super().put_nowait(item)

    async def get(self):
        item = await super().get()
        self.events.append(('get', item))
        return item

    def get_nowait(self):
        item = super().get_nowait()
        self.events.append(('get', item))
        return item


def probe_packet():
    return packet.Packet(packet.PING, data='probe').encode()


def upgrade_packet():
    return packet.Packet(packet.UPGRADE).encode()


class TestSocket:
    def _record_queue(self, s):
        s.queue = RecordingAsyncQueue()
        return s.queue

    def _queued(self, s):
        return list(s.queue._queue)

    def _gets(self, s):
        return [event for event in s.queue.events if event[0] == 'get']

    def _assert_still_polling(self, s):
        assert s.connected
        assert not s.upgrading
        assert not s.upgraded
        assert not s.closing
        assert not s.closed

    async def _polling_get(self, s):
        return await s.handle_get_request(
            {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=foo'})

    async def _post_message(self, s, data):
        pkt = packet.Packet(packet.MESSAGE, data=data)
        p = payload.Payload(packets=[pkt]).encode().encode('utf-8')
        await s.handle_post_request({
            'REQUEST_METHOD': 'POST',
            'QUERY_STRING': 'sid=foo',
            'CONTENT_LENGTH': len(p),
            'wsgi.input': self._get_read_mock_coro(p),
        })

    def _get_read_mock_coro(self, payload):
        mock_input = mock.MagicMock()
        mock_input.read = mock.AsyncMock()
        mock_input.read.return_value = payload
        return mock_input

    def _get_mock_server(self):
        mock_server = mock.Mock()
        mock_server.ping_timeout = 0.2
        mock_server.ping_interval = 0.2
        mock_server.ping_interval_grace_period = 0.001
        mock_server.async_handlers = False
        mock_server.max_http_buffer_size = 128
        mock_server._async = {
            'asyncio': True,
            'create_route': mock.MagicMock(),
            'translate_request': mock.MagicMock(),
            'make_response': mock.MagicMock(),
            'websocket': 'w',
        }
        mock_server._async['translate_request'].return_value = 'request'
        mock_server._async['make_response'].return_value = 'response'
        mock_server._trigger_event = mock.AsyncMock()

        def bg_task(target, *args, **kwargs):
            return asyncio.ensure_future(target(*args, **kwargs))

        def create_queue(*args, **kwargs):
            queue = asyncio.Queue(*args, **kwargs)
            queue.Empty = asyncio.QueueEmpty
            return queue

        mock_server.start_background_task = bg_task
        mock_server.create_queue = create_queue
        return mock_server

    async def test_create(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        assert s.server == mock_server
        assert s.sid == 'sid'
        assert not s.upgraded
        assert not s.closed
        assert hasattr(s.queue, 'get')
        assert hasattr(s.queue, 'put')
        assert hasattr(s.queue, 'task_done')
        assert hasattr(s.queue, 'join')

    async def test_empty_poll(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        with pytest.raises(exceptions.QueueEmpty):
            await s.poll()

    async def test_poll(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        await s.send(pkt1)
        await s.send(pkt2)
        assert await s.poll() == [pkt1, pkt2]

    async def test_poll_none(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        await s.queue.put(None)
        assert await s.poll() == []

    async def test_poll_none_after_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        pkt = packet.Packet(packet.MESSAGE, data='hello')
        await s.send(pkt)
        await s.queue.put(None)
        assert await s.poll() == [pkt]
        assert await s.poll() == []

    async def test_schedule_ping(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 0.01
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()

        async def schedule_ping_and_sleep():
            s.schedule_ping()
            await asyncio.sleep(0.05)

        await schedule_ping_and_sleep()
        assert s.last_ping is not None
        assert s.send.await_args_list[0][0][0].encode() == '2'

    async def test_schedule_ping_twice(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 0.1
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()

        async def schedule_ping_and_sleep():
            s.schedule_ping()
            await asyncio.sleep(0.2)

        s.schedule_ping()
        assert s.last_ping is None
        await schedule_ping_and_sleep()
        assert s.last_ping is not None
        assert s.send.await_count == 1

    async def test_schedule_ping_closed_socket(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 0.01
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()
        s.closed = True

        async def schedule_ping():
            s.schedule_ping()
            await asyncio.sleep(0.05)

        await schedule_ping()
        assert s.last_ping is None
        s.send.assert_not_awaited()

    async def test_pong(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.schedule_ping = mock.MagicMock()
        await s.receive(packet.Packet(packet.PONG, data='abc'))
        s.schedule_ping.assert_called_once_with()

    async def test_message_sync_handler(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        await s.receive(packet.Packet(packet.MESSAGE, data='foo'))
        mock_server._trigger_event.assert_awaited_once_with(
            'message', 'sid', 'foo', run_async=False
        )

    async def test_message_async_handler(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        mock_server.async_handlers = True
        await s.receive(packet.Packet(packet.MESSAGE, data='foo'))
        mock_server._trigger_event.assert_awaited_once_with(
            'message', 'sid', 'foo', run_async=True
        )

    async def test_invalid_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        with pytest.raises(exceptions.UnknownPacketError):
            await s.receive(packet.Packet(packet.OPEN))

    async def test_timeout(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 6
        mock_server.ping_interval_grace_period = 2
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.last_ping = time.time() - 9
        s.close = mock.AsyncMock()
        await s.send('packet')
        s.close.assert_awaited_once_with(
            wait=False, abort=False, reason=mock_server.reason.PING_TIMEOUT)

    async def test_polling_read(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        await s.send(pkt1)
        await s.send(pkt2)
        environ = {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=foo'}
        packets = await s.handle_get_request(environ)
        assert packets == [pkt1, pkt2]

    async def test_polling_read_error(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        environ = {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=foo'}
        with pytest.raises(exceptions.QueueEmpty):
            await s.handle_get_request(environ)

    async def test_polling_write(self):
        mock_server = self._get_mock_server()
        mock_server.max_http_buffer_size = 1000
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        p = payload.Payload(packets=[pkt1, pkt2]).encode().encode('utf-8')
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.receive = mock.AsyncMock()
        environ = {
            'REQUEST_METHOD': 'POST',
            'QUERY_STRING': 'sid=foo',
            'CONTENT_LENGTH': len(p),
            'wsgi.input': self._get_read_mock_coro(p),
        }
        await s.handle_post_request(environ)
        assert s.receive.await_count == 2
        assert s.receive.call_args_list[0][0][0].data == 'hello'
        assert s.receive.call_args_list[1][0][0].data == 'bye'

    async def test_polling_write_too_large(self):
        mock_server = self._get_mock_server()
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        p = payload.Payload(packets=[pkt1, pkt2]).encode().encode('utf-8')
        mock_server.max_http_buffer_size = len(p) - 1
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.receive = mock.AsyncMock()
        environ = {
            'REQUEST_METHOD': 'POST',
            'QUERY_STRING': 'sid=foo',
            'CONTENT_LENGTH': len(p),
            'wsgi.input': self._get_read_mock_coro(p),
        }
        with pytest.raises(exceptions.ContentTooLongError):
            await s.handle_post_request(environ)

    async def test_polling_write_without_content_length(self):
        mock_server = self._get_mock_server()
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        p = payload.Payload(packets=[pkt1, pkt2]).encode().encode('utf-8')
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.receive = mock.AsyncMock()
        environ = {
            'REQUEST_METHOD': 'POST',
            'QUERY_STRING': 'sid=foo',
            'wsgi.input': self._get_read_mock_coro(p),
        }
        await s.handle_post_request(environ)
        assert s.receive.await_count == 2
        assert environ['wsgi.input'].read.call_count == 1
        assert environ['wsgi.input'].read.call_args[0] == (0,)

    async def test_upgrade_handshake(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s._upgrade_websocket = mock.AsyncMock()
        environ = {
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'sid=foo',
            'HTTP_CONNECTION': 'Foo,Upgrade,Bar',
            'HTTP_UPGRADE': 'websocket',
        }
        await s.handle_get_request(environ)
        s._upgrade_websocket.assert_awaited_once_with(environ)

    async def test_upgrade(self):
        mock_server = self._get_mock_server()
        mock_server._async['websocket'] = mock.MagicMock()
        mock_ws = mock.AsyncMock()
        mock_server._async['websocket'].return_value = mock_ws
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        environ = "foo"
        await s._upgrade_websocket(environ)
        mock_server._async['websocket'].assert_called_once_with(
            s._websocket_handler, mock_server
        )
        mock_ws.assert_awaited_once_with(environ)

    async def test_upgrade_twice(self):
        mock_server = self._get_mock_server()
        mock_server._async['websocket'] = mock.MagicMock()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        s.upgraded = True
        environ = "foo"
        with pytest.raises(IOError):
            await s._upgrade_websocket(environ)

    async def test_upgrade_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        await s.receive(packet.Packet(packet.UPGRADE))
        r = await s.poll()
        assert len(r) == 1
        assert r[0].encode() == packet.Packet(packet.NOOP).encode()

    async def test_upgrade_no_probe(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        ws = mock.MagicMock()
        ws.wait = mock.AsyncMock()
        ws.wait.return_value = packet.Packet(packet.NOOP).encode()
        await s._websocket_handler(ws)
        assert not s.upgraded

    async def test_upgrade_no_upgrade_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.AsyncMock(return_value=None)
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        probe = 'probe'
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.NOOP).encode(),
        ]
        await s._websocket_handler(ws)
        ws.send.assert_awaited_once_with(
            packet.Packet(packet.PONG, data=probe).encode()
        )
        assert (await s.queue.get()).packet_type == packet.NOOP
        assert not s.upgraded

    async def test_upgrade_not_supported(self):
        mock_server = self._get_mock_server()
        mock_server._async['websocket'] = None
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        environ = "foo"
        await s._upgrade_websocket(environ)
        mock_server._bad_request.assert_called_once_with()

    async def test_close_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        s.close = mock.AsyncMock()
        await s.receive(packet.Packet(packet.CLOSE))
        s.close.assert_awaited_once_with(
            wait=False, abort=True,
            reason=mock_server.reason.CLIENT_DISCONNECT)

    async def test_websocket_read_write(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = False
        s.queue.join = mock.AsyncMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        s.poll = mock.AsyncMock(
            side_effect=[[packet.Packet(packet.MESSAGE, data=bar)], None]
        )
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        ws.wait.side_effect = [
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            None,
        ]
        ws.close = mock.AsyncMock()
        await s._websocket_handler(ws)
        assert s.connected
        assert s.upgraded
        assert mock_server._trigger_event.await_count == 2
        mock_server._trigger_event.assert_has_awaits(
            [
                mock.call('message', 'sid', 'foo', run_async=False),
                mock.call('disconnect', 'sid',
                          mock_server.reason.TRANSPORT_CLOSE, run_async=False),
            ]
        )
        ws.send.assert_awaited_with('4bar')
        ws.close.assert_awaited()

    async def test_websocket_upgrade_read_write(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.AsyncMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        probe = 'probe'
        s.poll = mock.AsyncMock(
            side_effect=[
                [packet.Packet(packet.MESSAGE, data=bar)],
                exceptions.QueueEmpty,
            ]
        )
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE).encode(),
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            None,
        ]
        ws.close = mock.AsyncMock()
        await s._websocket_handler(ws)
        assert s.upgraded
        assert mock_server._trigger_event.await_count == 2
        mock_server._trigger_event.assert_has_awaits(
            [
                mock.call('message', 'sid', 'foo', run_async=False),
                mock.call('disconnect', 'sid',
                          mock_server.reason.TRANSPORT_CLOSE, run_async=False),
            ]
        )
        ws.send.assert_awaited_with('4bar')
        ws.close.assert_awaited()

    async def test_websocket_upgrade_with_payload(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.AsyncMock(return_value=None)
        probe = 'probe'
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE, data='2').encode(),
        ]
        ws.close = mock.AsyncMock()
        await s._websocket_handler(ws)
        assert s.upgraded

    async def test_websocket_upgrade_with_backlog(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.AsyncMock(return_value=None)
        probe = 'probe'
        foo = 'foo'
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE, data='2').encode(),
        ]
        ws.close = mock.AsyncMock()
        s.upgrading = True
        await s.send(packet.Packet(packet.MESSAGE, data=foo))
        environ = {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=sid'}
        packets = await s.handle_get_request(environ)
        assert len(packets) == 1
        assert packets[0].encode() == '6'
        packets = await s.poll()
        assert len(packets) == 1
        assert packets[0].encode() == '4foo'

        await s._websocket_handler(ws)
        assert s.upgraded
        assert not s.upgrading
        packets = await s.handle_get_request(environ)
        assert len(packets) == 1
        assert packets[0].encode() == '6'

    async def test_websocket_read_write_wait_fail(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = False
        s.queue.join = mock.AsyncMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        s.poll = mock.AsyncMock(
            side_effect=[
                [packet.Packet(packet.MESSAGE, data=bar)],
                [packet.Packet(packet.MESSAGE, data=bar)],
                exceptions.QueueEmpty,
            ]
        )
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        ws.wait.side_effect = [
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            RuntimeError,
        ]
        ws.send.side_effect = [None, RuntimeError]
        ws.close = mock.AsyncMock()
        await s._websocket_handler(ws)
        assert s.closed

    async def test_websocket_upgrade_with_large_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.AsyncMock(return_value=None)
        probe = 'probe'
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE, data='2' * 128).encode(),
        ]
        with pytest.raises(ValueError):
            await s._websocket_handler(ws)
        assert not s.upgraded
        assert not s.upgrading

    async def test_websocket_ignore_invalid_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.connected = False
        s.queue.join = mock.AsyncMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        s.poll = mock.AsyncMock(
            side_effect=[
                [packet.Packet(packet.MESSAGE, data=bar)],
                exceptions.QueueEmpty,
            ]
        )
        ws = mock.MagicMock()
        ws.send = mock.AsyncMock()
        ws.wait = mock.AsyncMock()
        ws.wait.side_effect = [
            packet.Packet(packet.OPEN).encode(),
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            None,
        ]
        ws.close = mock.AsyncMock()
        await s._websocket_handler(ws)
        assert s.connected
        assert mock_server._trigger_event.await_count == 2
        mock_server._trigger_event.assert_has_awaits(
            [
                mock.call('message', 'sid', foo, run_async=False),
                mock.call('disconnect', 'sid',
                          mock_server.reason.TRANSPORT_CLOSE, run_async=False),
            ]
        )
        ws.send.assert_awaited_with('4bar')
        ws.close.assert_awaited()

    async def test_upgrade_probe_wrong_ping_data(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([
            packet.Packet(packet.PING, data='not-a-probe').encode(),
        ])
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert ws.wait_calls == 1
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert mock_server._trigger_event.await_count == 0

        assert (await self._polling_get(s))[0] is business
        assert self._queued(s) == []

    async def test_upgrade_probe_first_packet_is_not_ping(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([
            packet.Packet(packet.MESSAGE, data='probe').encode(),
        ])
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert (await self._polling_get(s))[0] is business

    async def test_upgrade_websocket_closed_while_waiting_for_probe(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([None])
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert (await self._polling_get(s))[0] is business

    async def test_upgrade_failure_after_pong_does_not_strand_polling(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([probe_packet(), None])
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert [p.packet_type for p in self._queued(s)
                if p.packet_type == packet.NOOP] == [packet.NOOP]

        packets = await self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.MESSAGE, packet.NOOP]
        assert packets[0] is business
        assert self._queued(s) == []
        assert mock_server._trigger_event.await_count == 0

    async def test_upgrade_wait_error_during_probe_cleans_up(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([RuntimeError('boom')])
        with pytest.raises(RuntimeError):
            await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert (await self._polling_get(s))[0] is business
        assert mock_server._trigger_event.await_count == 0

    async def test_upgrade_wait_error_after_pong_cleans_up(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeAsyncWebSocket([probe_packet(), RuntimeError('boom')])
        with pytest.raises(RuntimeError):
            await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert [p.packet_type for p in await self._polling_get(s)] == \
            [packet.NOOP]
        assert mock_server._trigger_event.await_count == 0

    async def test_upgrade_illegal_second_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([
            probe_packet(),
            packet.Packet(packet.MESSAGE, data='stolen').encode(),
        ])
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert mock_server._trigger_event.await_count == 0
        assert self._queued(s)[0] is business
        assert [p.packet_type for p in self._queued(s)[1:]] == \
            [packet.NOOP]
        assert self._gets(s) == []

        packets = await self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.MESSAGE, packet.NOOP]
        assert packets[0] is business
        assert self._queued(s) == []

    async def test_upgrade_malformed_second_packet_cleans_up(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([probe_packet(), ''])
        with pytest.raises(ValueError):
            await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert self._queued(s)[0] is business
        assert [p.packet_type for p in self._queued(s)[1:]] == \
            [packet.NOOP]
        packets = await self._polling_get(s)
        assert packets[0] is business
        assert mock_server._trigger_event.await_count == 0

    async def test_upgrade_unknown_packet_type_second_packet(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeAsyncWebSocket([probe_packet(), '9junk'])
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert [p.packet_type for p in await self._polling_get(s)] == \
            [packet.NOOP]

    async def test_upgrade_pong_send_error_cleans_up(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([probe_packet()])
        ws.send = mock.AsyncMock(side_effect=RuntimeError('pong failed'))
        with pytest.raises(RuntimeError):
            await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s)[0] is business
        assert self._gets(s) == []
        assert (await self._polling_get(s))[0] is business
        assert mock_server._trigger_event.await_count == 0

    async def test_upgrade_websocket_closed_before_upgrade_cleans_up(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        ws = FakeAsyncWebSocket([probe_packet(), OSError('closed')])
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert self._queued(s)[0] is business
        assert (await self._polling_get(s))[0] is business
        assert mock_server._trigger_event.await_count == 0

    async def test_simultaneous_upgrade_candidates(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        winner = FakeAsyncWebSocket([probe_packet(), upgrade_packet()])
        loser = FakeAsyncWebSocket([probe_packet(), upgrade_packet()])

        async def attempt_loser():
            await s._websocket_handler(loser)

        winner.hook_at_wait = 2
        winner.hook = attempt_loser
        await s._websocket_handler(winner)

        assert s.upgraded
        assert not s.upgrading
        assert loser.sent == []
        assert loser.wait_calls == 0
        assert loser.closed
        assert winner.sent.count(packet.Packet(packet.PONG,
                                               data='probe').encode()) == 1
        assert mock_server._trigger_event.await_count == 1
        mock_server._trigger_event.assert_awaited_once_with(
            'disconnect', 'foo', mock_server.reason.TRANSPORT_CLOSE,
            run_async=False)

    async def test_failed_candidate_cannot_take_backlog_then_retry(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)

        bad = FakeAsyncWebSocket([
            probe_packet(),
            packet.Packet(packet.PONG, data='x').encode()])
        rejected = FakeAsyncWebSocket([probe_packet(), upgrade_packet()])

        async def attempt_rejected():
            await s._websocket_handler(rejected)

        bad.hook_at_wait = 2
        bad.hook = attempt_rejected
        await s._websocket_handler(bad)

        self._assert_still_polling(s)
        assert business in self._queued(s)
        assert rejected.sent == []
        assert rejected.wait_calls == 0
        assert rejected.closed
        packets = await self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.MESSAGE, packet.NOOP]
        assert packets[0] is business
        assert mock_server._trigger_event.await_count == 0

        good = FakeAsyncWebSocket([probe_packet(), upgrade_packet()])
        await s._websocket_handler(good)
        assert s.upgraded
        assert good.sent.count('4pending') == 0
        assert good.sent.count(packet.Packet(packet.PONG,
                                             data='probe').encode()) == 1

    async def test_polling_get_during_probe_only_returns_noop(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        await s.send(business)
        results = []

        async def poll_during_probe():
            results.append(await self._polling_get(s))

        ws = FakeAsyncWebSocket([probe_packet(), upgrade_packet(), None])
        ws.hook_at_wait = 2
        ws.hook = poll_during_probe
        await s._websocket_handler(ws)

        assert s.upgraded
        assert len(results[0]) == 1
        assert results[0][0].packet_type == packet.NOOP
        assert business not in results[0]
        assert ws.sent.count('4pending') == 1

    async def test_polling_post_during_probe_is_dispatched_once(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)

        async def post_during_probe():
            await self._post_message(s, 'during-upgrade')

        ws = FakeAsyncWebSocket([probe_packet(), upgrade_packet(), None])
        ws.hook_at_wait = 2
        ws.hook = post_during_probe
        await s._websocket_handler(ws)

        assert s.upgraded
        message_calls = [call for call in
                         mock_server._trigger_event.await_args_list
                         if call.args[0] == 'message']
        assert message_calls == [
            mock.call('message', 'foo', 'during-upgrade', run_async=False)]
        # the POST is routed through the original polling transport, not
        # through the uncommitted candidate
        assert ws.sent == [
            packet.Packet(packet.PONG, data='probe').encode(),
            packet.Packet(packet.NOOP).encode(),
        ]

    async def test_failed_probe_does_not_consume_server_backlog(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        first = packet.Packet(packet.MESSAGE, data='first')
        second = packet.Packet(packet.MESSAGE, data='second')
        await s.send(first)

        async def send_second():
            await s.send(second)

        ws = FakeAsyncWebSocket([probe_packet(), None])
        ws.hook_at_wait = 2
        ws.hook = send_second
        await s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert self._queued(s)[0] is first
        assert self._queued(s)[1].packet_type == packet.NOOP
        assert self._queued(s)[2] is second
        packets = await self._polling_get(s)
        assert packets[0] is first
        assert packets[1].packet_type == packet.NOOP
        assert packets[2] is second
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]

    async def test_polling_round_trip_after_failed_probe(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeAsyncWebSocket([
            probe_packet(),
            packet.Packet(packet.PONG, data='x').encode(),
        ])
        await s._websocket_handler(ws)
        self._assert_still_polling(s)

        await self._post_message(s, 'from-polling')
        assert mock_server._trigger_event.await_count == 1
        mock_server._trigger_event.assert_awaited_once_with(
            'message', 'foo', 'from-polling', run_async=False)

        outbound = packet.Packet(packet.MESSAGE, data='to-polling')
        await s.send(outbound)
        packets = await self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.NOOP, packet.MESSAGE]
        assert packets[1] is outbound
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]

    async def test_upgrade_delivers_backlog_exactly_once_to_single_receiver(
            self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        first = packet.Packet(packet.MESSAGE, data='first')
        second = packet.Packet(packet.MESSAGE, data='second')
        await s.send(first)
        await s.send(second)

        async def post_during_probe():
            await self._post_message(s, 'from-client')

        ws = FakeAsyncWebSocket([probe_packet(), upgrade_packet(), None])
        ws.hook_at_wait = 2
        ws.hook = post_during_probe
        await s._websocket_handler(ws)

        assert s.upgraded
        assert s.closed
        assert ws.sent.count('4first') == 1
        assert ws.sent.count('4second') == 1
        # the writer drained the queue during teardown; no business packet
        # was left behind or duplicated for a later reader
        assert self._queued(s) == []
        mock_server.ping_interval = 0.01
        mock_server.ping_timeout = 0.01
        with pytest.raises(exceptions.QueueEmpty):
            await s.poll()
        message_calls = [call for call in
                         mock_server._trigger_event.await_args_list
                         if call.args[0] == 'message']
        assert message_calls == [
            mock.call('message', 'foo', 'from-client', run_async=False)]
        assert mock_server._trigger_event.await_args_list[-1] == \
            mock.call('disconnect', 'foo',
                      mock_server.reason.TRANSPORT_CLOSE, run_async=False)

    async def test_upgrade_without_connected_socket_starts_new_transport(
            self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = False
        self._record_queue(s)
        await s._websocket_handler(FakeAsyncWebSocket([None]))

        assert s.connected
        assert s.upgraded
        assert not s.upgrading
        assert s.closed

    async def test_disconnect_after_successful_upgrade_is_transport_close(
            self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeAsyncWebSocket([probe_packet(), upgrade_packet(), None])
        await s._websocket_handler(ws)

        assert s.upgraded
        assert s.closed
        assert not s.upgrading
        mock_server._trigger_event.assert_awaited_once_with(
            'disconnect', 'foo', mock_server.reason.TRANSPORT_CLOSE,
            run_async=False)

    async def test_send_after_close(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        await s.close(wait=False)
        with pytest.raises(exceptions.SocketIsClosedError):
            await s.send(packet.Packet(packet.NOOP))

    async def test_close_after_close(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        await s.close(wait=False)
        assert s.closed
        assert mock_server._trigger_event.await_count == 1
        mock_server._trigger_event.assert_awaited_once_with(
            'disconnect', 'sid', mock_server.reason.SERVER_DISCONNECT,
            run_async=False
        )
        await s.close()
        assert mock_server._trigger_event.await_count == 1

    async def test_close_and_wait(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.queue = mock.MagicMock()
        s.queue.put = mock.AsyncMock()
        s.queue.join = mock.AsyncMock()
        await s.close(wait=True)
        s.queue.join.assert_awaited_once_with()

    async def test_close_without_wait(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.queue = mock.MagicMock()
        s.queue.put = mock.AsyncMock()
        s.queue.join = mock.AsyncMock()
        await s.close(wait=False)
        assert s.queue.join.await_count == 0
