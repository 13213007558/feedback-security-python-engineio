import asyncio
import time
from unittest import mock

import pytest

from engineio import async_socket
from engineio import exceptions
from engineio import packet
from engineio import payload


class FakeMonotonicClock:
    """A controllable stand-in for the socket's monotonic clock."""
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class TestSocket:
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
        self.bg_tasks = []

        def bg_task(target, *args, **kwargs):
            task = asyncio.ensure_future(target(*args, **kwargs))
            self.bg_tasks.append(task)
            return task

        def create_queue(*args, **kwargs):
            queue = asyncio.Queue(*args, **kwargs)
            queue.Empty = asyncio.QueueEmpty
            return queue

        async def real_sleep(seconds):
            await asyncio.sleep(seconds)

        self.ping_cycles = []

        async def controlled_sleep(seconds):
            # Stand-in for the ping interval wait: instead of blocking for
            # real wall time, yield control once and then return. Each call
            # records a cycle so the test can assert on the number of pings
            # deterministically.
            self.ping_cycles.append(seconds)
            await asyncio.sleep(0)

        mock_server.start_background_task = bg_task
        mock_server.create_queue = create_queue
        mock_server.sleep = real_sleep
        self._controlled_sleep = controlled_sleep
        return mock_server

    def _use_controlled_sleep(self, mock_server):
        mock_server.sleep = self._controlled_sleep

    async def _drain_background_tasks(self):
        """Let every scheduled ping task run to completion."""
        for _ in range(100):
            await asyncio.sleep(0)
            if all(task.done() for task in self.bg_tasks):
                break
        await asyncio.gather(*self.bg_tasks, return_exceptions=True)

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
        self._use_controlled_sleep(mock_server)
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()
        s.schedule_ping()
        assert s.last_ping is None
        assert len(self.bg_tasks) == 1
        await self._drain_background_tasks()
        assert s.last_ping is not None
        assert s.send.await_args_list[0][0][0].encode() == '2'

    async def test_schedule_ping_twice(self):
        mock_server = self._get_mock_server()
        self._use_controlled_sleep(mock_server)
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()
        s.schedule_ping()
        # a second pong while the first wait cycle is still pending must be
        # ignored and must not start another scheduler chain
        s.schedule_ping()
        assert len(self.bg_tasks) == 1
        await self._drain_background_tasks()
        assert s.last_ping is not None
        assert s.send.await_count == 1

    async def test_duplicate_pong_does_not_start_second_chain(self):
        mock_server = self._get_mock_server()
        self._use_controlled_sleep(mock_server)
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()
        pong = packet.Packet(packet.PONG)

        # first pong starts the single wait cycle
        await s.receive(pong)
        generation = s.ping_generation
        assert s.last_ping is None
        assert len(self.bg_tasks) == 1
        # duplicated, early pongs while the cycle is pending are ignored
        await s.receive(packet.Packet(packet.PONG))
        await s.receive(packet.Packet(packet.PONG))
        assert len(self.bg_tasks) == 1
        assert s.ping_generation == generation

        await self._drain_background_tasks()
        assert s.send.await_count == 1
        assert s.send.await_args_list[0][0][0].packet_type == packet.PING

        # the next legitimate pong arms exactly one more cycle
        await s.receive(pong)
        assert len(self.bg_tasks) == 2
        await self._drain_background_tasks()
        assert s.send.await_count == 2

    async def test_normal_ping_pong_roundtrip(self):
        mock_server = self._get_mock_server()
        self._use_controlled_sleep(mock_server)
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.schedule_ping()
        await self._drain_background_tasks()
        sent = s.queue.get_nowait()
        assert sent.packet_type == packet.PING
        # the liveness timestamp uses the monotonic clock
        assert s.last_ping is not None
        # client answers in time; stub the scheduler so the pong does not
        # leave another background task parked behind the test
        s.schedule_ping = mock.MagicMock()
        await s.receive(packet.Packet(packet.PONG))
        s.schedule_ping.assert_called_once_with()

    async def test_schedule_ping_closing_socket(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()
        s.closing = True
        s.schedule_ping()
        assert self.bg_tasks == []
        assert s.last_ping is not None
        s.send.assert_not_awaited()

    async def test_schedule_ping_closed_socket(self):
        mock_server = self._get_mock_server()
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()
        s.closed = True

        async def schedule_ping():
            s.schedule_ping()
            await asyncio.sleep(0)

        await schedule_ping()
        assert self.bg_tasks == []
        assert s.last_ping is not None
        s.send.assert_not_awaited()

    async def test_task_waking_during_close_does_not_ping(self):
        mock_server = self._get_mock_server()
        gate = asyncio.Event()

        async def blocking_sleep(_seconds):
            # park the ping wait cycle until the test closes the socket and
            # then releases it, simulating the task waking during shutdown
            await gate.wait()

        mock_server.sleep = blocking_sleep
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.schedule_ping()
        assert s.last_ping is None
        await asyncio.sleep(0)  # let the background task enter the wait
        await s.close(wait=False, abort=True)
        assert s.closed
        gate.set()  # wake the sleeping task after close
        await asyncio.gather(*self.bg_tasks, return_exceptions=True)
        # the queue must not contain any PING packet sent by the stale task
        queued = []
        while True:
            try:
                queued.append(s.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        assert all(p is None or p.packet_type != packet.PING
                   for p in queued)
        # the stale task must not have refreshed the liveness timestamp
        assert s.last_ping is None

    async def test_stale_generation_does_not_ping(self):
        mock_server = self._get_mock_server()
        self._use_controlled_sleep(mock_server)
        s = async_socket.AsyncSocket(mock_server, 'sid')
        s.send = mock.AsyncMock()
        s.schedule_ping()
        stale_generation = s.ping_generation
        # a new cycle supersedes the sleeping one without waiting for it
        s.last_ping = s._now()
        s.schedule_ping()
        assert s.ping_generation == stale_generation + 1
        # when both wait cycles resolve, only the current generation may send
        await self._drain_background_tasks()
        assert s.send.await_count == 1
        assert s.send.await_args_list[0][0][0].packet_type == packet.PING

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
        fake_clock = FakeMonotonicClock()
        s._now = fake_clock
        s.last_ping = fake_clock()
        fake_clock.advance(9)
        s.close = mock.AsyncMock()
        await s.send('packet')
        s.close.assert_awaited_once_with(
            wait=False, abort=False, reason=mock_server.reason.PING_TIMEOUT)

    async def test_timeout_ignores_wall_clock_rollback(self):
        # rolling the wall clock backwards must not keep a dead client alive
        mock_server = self._get_mock_server()
        mock_server.ping_timeout = 20
        s = async_socket.AsyncSocket(mock_server, 'sid')
        fake_clock = FakeMonotonicClock()
        s._now = fake_clock
        s.last_ping = fake_clock()
        fake_clock.advance(21)  # real elapsed time exceeds the timeout
        with mock.patch('time.time', return_value=time.time() - 3600):
            s.close = mock.AsyncMock()
            assert not await s.check_ping_timeout()
        s.close.assert_awaited_once_with(
            wait=False, abort=False,
            reason=mock_server.reason.PING_TIMEOUT)

    async def test_liveness_ignores_wall_clock_forward_jump(self):
        # a forward wall clock jump must not cause a false timeout
        mock_server = self._get_mock_server()
        mock_server.ping_timeout = 20
        s = async_socket.AsyncSocket(mock_server, 'sid')
        fake_clock = FakeMonotonicClock()
        s._now = fake_clock
        s.last_ping = fake_clock()
        fake_clock.advance(1)  # barely any real elapsed time
        with mock.patch('time.time', return_value=time.time() + 3600):
            assert await s.check_ping_timeout()
            assert not s.closed

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
