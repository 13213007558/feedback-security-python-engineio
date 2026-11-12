import io
import queue
import time
from unittest import mock

import pytest

from engineio import exceptions
from engineio import packet
from engineio import payload
from engineio import socket


class ProbeFailure(Exception):
    """A failure raised by the fake transport while waiting for a packet."""


class FakeWebSocket:
    """A scripted WebSocket transport.

    ``responses`` is consumed by ``wait()``. Exception instances are raised
    instead of returned. Exception classes are raised with a generic message.
    ``hook`` runs once, immediately before the numbered ``wait`` call, which
    allows a test to observe the transport mid-probe.
    """

    def __init__(self, responses=()):
        self.responses = list(responses)
        self.sent = []
        self.closed = False
        self.wait_calls = 0
        self.hook_at_wait = None
        self.hook = None

    def wait(self):
        self.wait_calls += 1
        if self.hook is not None and self.wait_calls == self.hook_at_wait:
            self.hook()
        if not self.responses:
            return None
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if isinstance(response, type) and issubclass(response, Exception):
            raise response('websocket failure')
        return response

    def send(self, message):
        self.sent.append(message)

    def close(self):
        self.closed = True


class RecordingQueue(queue.Queue):
    """Queue that records get/put operations for packet-routing assertions."""

    def __init__(self):
        super().__init__()
        self.events = []

    def put(self, item, block=True, timeout=None):
        self.events.append(('put', item))
        return super().put(item, block=block, timeout=timeout)

    def get(self, block=True, timeout=None):
        item = super().get(block=block, timeout=timeout)
        self.events.append(('get', item))
        return item

    def task_done(self):
        self.events.append(('task_done',))
        return super().task_done()


def probe_packet():
    return packet.Packet(packet.PING, data='probe').encode()


def upgrade_packet():
    return packet.Packet(packet.UPGRADE).encode()


class TestSocket:
    def setup_method(self):
        self.bg_tasks = []

    def _record_queue(self, s):
        s.queue = RecordingQueue()
        return s.queue

    def _queued(self, s):
        return list(s.queue.queue)

    def _gets(self, s):
        return [event for event in s.queue.events if event[0] == 'get']

    def _assert_still_polling(self, s):
        assert s.connected
        assert not s.upgrading
        assert not s.upgraded
        assert not s.closing
        assert not s.closed

    def _get_mock_server(self):
        mock_server = mock.Mock()
        mock_server.ping_timeout = 0.2
        mock_server.ping_interval = 0.2
        mock_server.ping_interval_grace_period = 0.001
        mock_server.async_handlers = True
        mock_server.max_http_buffer_size = 128
        mock_server.sleep = time.sleep

        try:
            import queue
        except ImportError:
            import Queue as queue
        import threading

        mock_server._async = {
            'threading': threading.Thread,
            'queue': queue.Queue,
            'websocket': None,
        }

        def bg_task(target, *args, **kwargs):
            th = threading.Thread(target=target, args=args, kwargs=kwargs)
            self.bg_tasks.append(th)
            th.start()
            return th

        def create_queue(*args, **kwargs):
            return queue.Queue(*args, **kwargs)

        mock_server.start_background_task = bg_task
        mock_server.create_queue = create_queue
        mock_server.get_queue_empty_exception.return_value = queue.Empty
        return mock_server

    def _join_bg_tasks(self):
        for task in self.bg_tasks:
            task.join()

    def test_create(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        assert s.server == mock_server
        assert s.sid == 'sid'
        assert not s.upgraded
        assert not s.closed
        assert hasattr(s.queue, 'get')
        assert hasattr(s.queue, 'put')
        assert hasattr(s.queue, 'task_done')
        assert hasattr(s.queue, 'join')

    def test_empty_poll(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        with pytest.raises(exceptions.QueueEmpty):
            s.poll()

    def test_poll(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        s.send(pkt1)
        s.send(pkt2)
        assert s.poll() == [pkt1, pkt2]

    def test_poll_none(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.queue.put(None)
        assert s.poll() == []

    def test_poll_none_after_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        pkt = packet.Packet(packet.MESSAGE, data='hello')
        s.send(pkt)
        s.queue.put(None)
        assert s.poll() == [pkt]
        assert s.poll() == []

    def test_schedule_ping(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 0.01
        s = socket.Socket(mock_server, 'sid')
        s.send = mock.MagicMock()
        s.schedule_ping()
        for _ in range(10):
            time.sleep(0.05)
            if s.last_ping is not None:
                break
        assert s.last_ping is not None
        assert s.send.call_args_list[0][0][0].encode() == '2'

    def test_schedule_ping_twice(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 0.1
        s = socket.Socket(mock_server, 'sid')
        s.send = mock.MagicMock()
        s.schedule_ping()
        assert s.last_ping is None
        time.sleep(0.01)
        s.schedule_ping()
        time.sleep(0.2)
        assert s.last_ping is not None
        assert s.send.call_count == 1

    def test_schedule_ping_closed_socket(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 0.01
        s = socket.Socket(mock_server, 'sid')
        s.send = mock.MagicMock()
        s.closed = True
        s.schedule_ping()
        time.sleep(0.05)
        assert s.last_ping is None
        s.send.assert_not_called()

    def test_pong(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.schedule_ping = mock.MagicMock()
        s.receive(packet.Packet(packet.PONG))
        s.schedule_ping.assert_called_once_with()

    def test_message_async_handler(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.receive(packet.Packet(packet.MESSAGE, data='foo'))
        mock_server._trigger_event.assert_called_once_with(
            'message', 'sid', 'foo', run_async=True
        )

    def test_message_sync_handler(self):
        mock_server = self._get_mock_server()
        mock_server.async_handlers = False
        s = socket.Socket(mock_server, 'sid')
        s.receive(packet.Packet(packet.MESSAGE, data='foo'))
        mock_server._trigger_event.assert_called_once_with(
            'message', 'sid', 'foo', run_async=False
        )

    def test_invalid_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        with pytest.raises(exceptions.UnknownPacketError):
            s.receive(packet.Packet(packet.OPEN))

    def test_timeout(self):
        mock_server = self._get_mock_server()
        mock_server.ping_interval = 6
        mock_server.ping_interval_grace_period = 2
        s = socket.Socket(mock_server, 'sid')
        s.last_ping = time.time() - 9
        s.close = mock.MagicMock()
        s.send('packet')
        s.close.assert_called_once_with(wait=False, abort=False,
                                        reason=mock_server.reason.PING_TIMEOUT)

    def test_polling_read(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        s.send(pkt1)
        s.send(pkt2)
        environ = {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=foo'}
        start_response = mock.MagicMock()
        packets = s.handle_get_request(environ, start_response)
        assert packets == [pkt1, pkt2]

    def test_polling_read_error(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        environ = {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=foo'}
        start_response = mock.MagicMock()
        with pytest.raises(exceptions.QueueEmpty):
            s.handle_get_request(environ, start_response)

    def test_polling_write(self):
        mock_server = self._get_mock_server()
        mock_server.max_http_buffer_size = 1000
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        p = payload.Payload(packets=[pkt1, pkt2]).encode().encode('utf-8')
        s = socket.Socket(mock_server, 'foo')
        s.receive = mock.MagicMock()
        environ = {
            'REQUEST_METHOD': 'POST',
            'QUERY_STRING': 'sid=foo',
            'CONTENT_LENGTH': len(p),
            'wsgi.input': io.BytesIO(p),
        }
        s.handle_post_request(environ)
        assert s.receive.call_count == 2

    def test_polling_write_too_large(self):
        mock_server = self._get_mock_server()
        pkt1 = packet.Packet(packet.MESSAGE, data='hello')
        pkt2 = packet.Packet(packet.MESSAGE, data='bye')
        p = payload.Payload(packets=[pkt1, pkt2]).encode().encode('utf-8')
        mock_server.max_http_buffer_size = len(p) - 1
        s = socket.Socket(mock_server, 'foo')
        s.receive = mock.MagicMock()
        environ = {
            'REQUEST_METHOD': 'POST',
            'QUERY_STRING': 'sid=foo',
            'CONTENT_LENGTH': len(p),
            'wsgi.input': io.BytesIO(p),
        }
        with pytest.raises(exceptions.ContentTooLongError):
            s.handle_post_request(environ)

    def test_upgrade_handshake(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s._upgrade_websocket = mock.MagicMock()
        environ = {
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'sid=foo',
            'HTTP_CONNECTION': 'Foo,Upgrade,Bar',
            'HTTP_UPGRADE': 'websocket',
        }
        start_response = mock.MagicMock()
        s.handle_get_request(environ, start_response)
        s._upgrade_websocket.assert_called_once_with(environ, start_response)

    def test_upgrade(self):
        mock_server = self._get_mock_server()
        mock_server._async['websocket'] = mock.MagicMock()
        mock_ws = mock.MagicMock()
        mock_server._async['websocket'].return_value = mock_ws
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        environ = "foo"
        start_response = "bar"
        s._upgrade_websocket(environ, start_response)
        mock_server._async['websocket'].assert_called_once_with(
            s._websocket_handler, mock_server
        )
        mock_ws.assert_called_once_with(environ, start_response)

    def test_upgrade_twice(self):
        mock_server = self._get_mock_server()
        mock_server._async['websocket'] = mock.MagicMock()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.upgraded = True
        environ = "foo"
        start_response = "bar"
        with pytest.raises(IOError):
            s._upgrade_websocket(environ, start_response)

    def test_upgrade_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.receive(packet.Packet(packet.UPGRADE))
        r = s.poll()
        assert len(r) == 1
        assert r[0].encode() == packet.Packet(packet.NOOP).encode()

    def test_upgrade_no_probe(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        ws = mock.MagicMock()
        ws.wait.return_value = packet.Packet(packet.NOOP).encode()
        s._websocket_handler(ws)
        assert not s.upgraded

    def test_upgrade_no_upgrade_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.MagicMock(return_value=None)
        ws = mock.MagicMock()
        probe = 'probe'
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.NOOP).encode(),
        ]
        s._websocket_handler(ws)
        ws.send.assert_called_once_with(
            packet.Packet(packet.PONG, data=probe).encode()
        )
        assert s.queue.get().packet_type == packet.NOOP
        assert not s.upgraded

    def test_close_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.close = mock.MagicMock()
        s.receive(packet.Packet(packet.CLOSE))
        s.close.assert_called_once_with(
            wait=False, abort=True,
            reason=mock_server.reason.CLIENT_DISCONNECT)

    def test_invalid_packet_type(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        pkt = packet.Packet(packet_type=99)
        with pytest.raises(exceptions.UnknownPacketError):
            s.receive(pkt)

    def test_upgrade_not_supported(self):
        mock_server = self._get_mock_server()
        mock_server._async['websocket'] = None
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        environ = "foo"
        start_response = "bar"
        s._upgrade_websocket(environ, start_response)
        mock_server._bad_request.assert_called_once_with()

    def test_websocket_read_write(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = False
        s.queue.join = mock.MagicMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        s.poll = mock.MagicMock(
            side_effect=[
                [packet.Packet(packet.MESSAGE, data=bar)],
                exceptions.QueueEmpty,
            ]
        )
        ws = mock.MagicMock()
        ws.wait.side_effect = [
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            None,
        ]
        s._websocket_handler(ws)
        self._join_bg_tasks()
        assert s.connected
        assert s.upgraded
        assert mock_server._trigger_event.call_count == 2
        mock_server._trigger_event.assert_has_calls(
            [
                mock.call('message', 'sid', 'foo', run_async=True),
                mock.call('disconnect', 'sid',
                          mock_server.reason.TRANSPORT_CLOSE,
                          run_async=False)
            ]
        )
        ws.send.assert_called_with('4bar')

    def test_websocket_upgrade_read_write(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.MagicMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        probe = 'probe'
        s.poll = mock.MagicMock(
            side_effect=[
                [packet.Packet(packet.MESSAGE, data=bar)],
                exceptions.QueueEmpty,
            ]
        )
        ws = mock.MagicMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE).encode(),
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            None,
        ]
        s._websocket_handler(ws)
        self._join_bg_tasks()
        assert s.upgraded
        assert mock_server._trigger_event.call_count == 2
        mock_server._trigger_event.assert_has_calls(
            [
                mock.call('message', 'sid', 'foo', run_async=True),
                mock.call('disconnect', 'sid',
                          mock_server.reason.TRANSPORT_CLOSE,
                          run_async=False)
            ]
        )
        ws.send.assert_called_with('4bar')

    def test_websocket_upgrade_with_payload(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.MagicMock(return_value=None)
        probe = 'probe'
        ws = mock.MagicMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE, data='2').encode(),
        ]
        s._websocket_handler(ws)
        self._join_bg_tasks()
        assert s.upgraded

    def test_websocket_upgrade_with_backlog(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.MagicMock(return_value=None)
        probe = 'probe'
        foo = 'foo'
        ws = mock.MagicMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE, data='2').encode(),
        ]
        s.upgrading = True
        s.send(packet.Packet(packet.MESSAGE, data=foo))
        environ = {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=sid'}
        start_response = mock.MagicMock()
        packets = s.handle_get_request(environ, start_response)
        assert len(packets) == 1
        assert packets[0].encode() == '6'
        packets = s.poll()
        assert len(packets) == 1
        assert packets[0].encode() == '4foo'

        s._websocket_handler(ws)
        self._join_bg_tasks()
        assert s.upgraded
        assert not s.upgrading
        packets = s.handle_get_request(environ, start_response)
        assert len(packets) == 1
        assert packets[0].encode() == '6'

    def test_websocket_read_write_wait_fail(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = False
        s.queue.join = mock.MagicMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        s.poll = mock.MagicMock(
            side_effect=[
                [packet.Packet(packet.MESSAGE, data=bar)],
                [packet.Packet(packet.MESSAGE, data=bar)],
                exceptions.QueueEmpty,
            ]
        )
        ws = mock.MagicMock()
        ws.wait.side_effect = [
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            RuntimeError,
        ]
        ws.send.side_effect = [None, RuntimeError]
        s._websocket_handler(ws)
        self._join_bg_tasks()
        assert s.closed

    def test_websocket_upgrade_with_large_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = True
        s.queue.join = mock.MagicMock(return_value=None)
        probe = 'probe'
        ws = mock.MagicMock()
        ws.wait.side_effect = [
            packet.Packet(packet.PING, data=probe).encode(),
            packet.Packet(packet.UPGRADE, data='2' * 128).encode(),
        ]
        with pytest.raises(ValueError):
            s._websocket_handler(ws)
        self._join_bg_tasks()
        assert not s.upgraded
        assert not s.upgrading

    def test_websocket_ignore_invalid_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.connected = False
        s.queue.join = mock.MagicMock(return_value=None)
        foo = 'foo'
        bar = 'bar'
        s.poll = mock.MagicMock(
            side_effect=[
                [packet.Packet(packet.MESSAGE, data=bar)],
                exceptions.QueueEmpty,
            ]
        )
        ws = mock.MagicMock()
        ws.wait.side_effect = [
            packet.Packet(packet.OPEN).encode(),
            packet.Packet(packet.MESSAGE, data=foo).encode(),
            None,
        ]
        s._websocket_handler(ws)
        self._join_bg_tasks()
        assert s.connected
        assert mock_server._trigger_event.call_count == 2
        mock_server._trigger_event.assert_has_calls(
            [
                mock.call('message', 'sid', foo, run_async=True),
                mock.call('disconnect', 'sid',
                          mock_server.reason.TRANSPORT_CLOSE,
                          run_async=False)
            ]
        )
        ws.send.assert_called_with('4bar')

    def _post_message(self, s, data):
        pkt = packet.Packet(packet.MESSAGE, data=data)
        p = payload.Payload(packets=[pkt]).encode().encode('utf-8')
        s.handle_post_request({
            'REQUEST_METHOD': 'POST',
            'QUERY_STRING': 'sid=foo',
            'CONTENT_LENGTH': len(p),
            'wsgi.input': io.BytesIO(p),
        })

    def _polling_get(self, s):
        return s.handle_get_request(
            {'REQUEST_METHOD': 'GET', 'QUERY_STRING': 'sid=foo'},
            mock.MagicMock())

    def test_upgrade_probe_wrong_ping_data(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([
            packet.Packet(packet.PING, data='not-a-probe').encode(),
        ])
        s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert ws.wait_calls == 1
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert mock_server._trigger_event.call_count == 0

        # the rejected candidate left the queue untouched; polling still works
        assert self._polling_get(s) == [business]
        assert self._queued(s) == []

    def test_upgrade_probe_first_packet_is_not_ping(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([
            packet.Packet(packet.MESSAGE, data='probe').encode(),
        ])
        s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert self._polling_get(s) == [business]

    def test_upgrade_websocket_closed_while_waiting_for_probe(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([None])
        s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert self._polling_get(s) == [business]

    def test_upgrade_failure_after_pong_does_not_strand_polling(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([probe_packet(), None])
        s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        # exactly one NOOP was queued to release the in-flight polling GET
        assert [p.packet_type for p in self._queued(s)
                if p.packet_type == packet.NOOP] == [packet.NOOP]

        # polling resumes: the business packet precedes the NOOP and is
        # delivered through the still-valid polling transport
        packets = self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.MESSAGE, packet.NOOP]
        assert packets[0] is business
        assert self._queued(s) == []
        assert mock_server._trigger_event.call_count == 0

    def test_upgrade_wait_error_during_probe_cleans_up(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([RuntimeError('boom')])
        with pytest.raises(RuntimeError):
            s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s) == [business]
        assert self._gets(s) == []
        assert self._polling_get(s) == [business]
        assert mock_server._trigger_event.call_count == 0

    def test_upgrade_wait_error_after_pong_cleans_up(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeWebSocket([probe_packet(), RuntimeError('boom')])
        with pytest.raises(RuntimeError):
            s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert [p.packet_type for p in self._polling_get(s)] == \
            [packet.NOOP]
        assert mock_server._trigger_event.call_count == 0

    def test_upgrade_pong_send_error_cleans_up(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([probe_packet()])
        ws.send = mock.MagicMock(side_effect=ProbeFailure('pong failed'))
        with pytest.raises(ProbeFailure):
            s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == []
        assert self._queued(s)[0] is business
        assert self._gets(s) == []
        assert self._polling_get(s)[0] is business
        assert mock_server._trigger_event.call_count == 0

    def test_upgrade_websocket_closed_before_upgrade_cleans_up(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([probe_packet(), OSError('closed')])
        s._websocket_handler(ws)

        # a transport-level close is handled gracefully, unlike the failure
        # before the probe which bubbles up
        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert self._queued(s)[0] is business
        packets = self._polling_get(s)
        assert packets[0] is business
        assert mock_server._trigger_event.call_count == 0

    def test_upgrade_illegal_second_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([
            probe_packet(),
            packet.Packet(packet.MESSAGE, data='stolen').encode(),
        ])
        s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        # the candidate's MESSAGE was never dispatched to the application
        assert mock_server._trigger_event.call_count == 0
        assert self._queued(s)[0] is business
        assert [p.packet_type for p in self._queued(s)[1:]] == \
            [packet.NOOP]
        assert self._gets(s) == []

        packets = self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.MESSAGE, packet.NOOP]
        assert packets[0] is business
        assert self._queued(s) == []

    def test_upgrade_malformed_second_packet_cleans_up(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        ws = FakeWebSocket([probe_packet(), ''])
        with pytest.raises(ValueError):
            s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert self._queued(s)[0] is business
        assert [p.packet_type for p in self._queued(s)[1:]] == \
            [packet.NOOP]
        packets = self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.MESSAGE, packet.NOOP]
        assert packets[0] is business
        assert mock_server._trigger_event.call_count == 0

    def test_upgrade_unknown_packet_type_second_packet(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeWebSocket([probe_packet(), '9junk'])
        s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]
        assert [p.packet_type for p in self._polling_get(s)] == \
            [packet.NOOP]

    def test_simultaneous_upgrade_candidates(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        winner = FakeWebSocket([probe_packet(), upgrade_packet()])
        loser = FakeWebSocket([probe_packet(), upgrade_packet()])
        # attempt the second upgrade while the first one waits for UPGRADE
        winner.hook_at_wait = 2
        winner.hook = lambda: s._websocket_handler(loser)
        s._websocket_handler(winner)
        self._join_bg_tasks()

        assert s.upgraded
        assert not s.upgrading
        # only the winning candidate became the receiver; the rejected
        # candidate was closed without consuming its probe
        assert loser.sent == []
        assert loser.wait_calls == 0
        assert loser.closed
        assert winner.sent.count(packet.Packet(packet.PONG,
                                               data='probe').encode()) == 1
        assert mock_server._trigger_event.call_count == 1
        mock_server._trigger_event.assert_called_once_with(
            'disconnect', 'foo', mock_server.reason.TRANSPORT_CLOSE,
            run_async=False)

    def test_failed_candidate_cannot_take_backlog_then_retry(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)

        bad = FakeWebSocket([probe_packet(),
                             packet.Packet(packet.PONG, data='x').encode()])
        bad.hook_at_wait = 2
        rejected = FakeWebSocket([probe_packet(), upgrade_packet()])
        bad.hook = lambda: s._websocket_handler(rejected)
        s._websocket_handler(bad)

        self._assert_still_polling(s)
        assert business in self._queued(s)
        assert rejected.sent == []
        assert rejected.wait_calls == 0
        assert rejected.closed
        packets = self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.MESSAGE, packet.NOOP]
        assert packets[0] is business
        assert mock_server._trigger_event.call_count == 0

        # a later, complete candidate succeeds and does not re-send the
        # already delivered business packet
        good = FakeWebSocket([probe_packet(), upgrade_packet()])
        s._websocket_handler(good)
        self._join_bg_tasks()
        assert s.upgraded
        assert good.sent.count('4pending') == 0
        assert good.sent.count(packet.Packet(packet.PONG,
                                             data='probe').encode()) == 1

    def _run_polling_get_during_probe(self, s, results):
        def get():
            try:
                results.append(self._polling_get(s))
            except Exception as e:  # pragma: no cover
                results.append(e)

        return get

    def test_polling_get_during_probe_only_returns_noop(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        business = packet.Packet(packet.MESSAGE, data='pending')
        s.send(business)
        results = []
        ws = FakeWebSocket([probe_packet(), upgrade_packet()])
        ws.hook_at_wait = 2
        ws.hook = self._run_polling_get_during_probe(s, results)
        s._websocket_handler(ws)
        self._join_bg_tasks()

        assert s.upgraded
        assert len(results[0]) == 1
        assert results[0][0].packet_type == packet.NOOP
        assert business not in results[0]
        # the in-flight polling GET did not steal the business packet; the
        # websocket writer delivered it exactly once
        assert ws.sent.count('4pending') == 1

    def test_polling_post_during_probe_is_dispatched_once(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeWebSocket([probe_packet(), upgrade_packet()])
        ws.hook_at_wait = 2
        ws.hook = lambda: self._post_message(s, 'during-upgrade')
        s._websocket_handler(ws)
        self._join_bg_tasks()

        assert s.upgraded
        # one dispatch for the POSTed message and one for the disconnect
        # that ends this scripted websocket session
        assert mock_server._trigger_event.call_count == 2
        assert mock_server._trigger_event.call_args_list[0] == \
            mock.call('message', 'foo', 'during-upgrade', run_async=True)
        assert mock_server._trigger_event.call_args_list[1] == \
            mock.call('disconnect', 'foo',
                      mock_server.reason.TRANSPORT_CLOSE, run_async=False)

    def test_failed_probe_does_not_consume_server_backlog(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        first = packet.Packet(packet.MESSAGE, data='first')
        second = packet.Packet(packet.MESSAGE, data='second')
        s.send(first)
        ws = FakeWebSocket([probe_packet(), None])
        ws.hook_at_wait = 2
        ws.hook = lambda: s.send(second)
        s._websocket_handler(ws)

        self._assert_still_polling(s)
        assert self._queued(s)[0] is first
        assert self._queued(s)[1].packet_type == packet.NOOP
        assert self._queued(s)[2] is second
        packets = self._polling_get(s)
        assert packets[0] is first
        assert packets[1].packet_type == packet.NOOP
        assert packets[2] is second
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]

    def test_polling_round_trip_after_failed_probe(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeWebSocket([
            probe_packet(),
            packet.Packet(packet.PONG, data='x').encode(),
        ])
        s._websocket_handler(ws)
        self._assert_still_polling(s)

        # receive through polling still dispatches exactly once
        self._post_message(s, 'from-polling')
        assert mock_server._trigger_event.call_count == 1
        mock_server._trigger_event.assert_called_once_with(
            'message', 'foo', 'from-polling', run_async=True)

        # send through polling still reaches the client exactly once
        outbound = packet.Packet(packet.MESSAGE, data='to-polling')
        s.send(outbound)
        packets = self._polling_get(s)
        assert [p.packet_type for p in packets] == \
            [packet.NOOP, packet.MESSAGE]
        assert packets[1] is outbound
        # the failed candidate's transport was never used for business data
        assert ws.sent == [packet.Packet(packet.PONG,
                                        data='probe').encode()]

    def test_upgrade_delivers_backlog_exactly_once_to_single_receiver(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        first = packet.Packet(packet.MESSAGE, data='first')
        second = packet.Packet(packet.MESSAGE, data='second')
        s.send(first)
        s.send(second)
        ws = FakeWebSocket([probe_packet(), upgrade_packet(), None])
        ws.hook_at_wait = 2
        ws.hook = lambda: self._post_message(s, 'from-client')
        s._websocket_handler(ws)
        self._join_bg_tasks()

        assert s.upgraded
        assert s.closed
        assert ws.sent.count('4first') == 1
        assert ws.sent.count('4second') == 1
        # the queue holds only the close sentinel; no business packet was
        # left behind or duplicated
        assert [p for p in self._queued(s) if p is not None] == []
        assert s.poll() == []
        assert mock_server._trigger_event.call_count == 2
        mock_server._trigger_event.assert_has_calls([
            mock.call('message', 'foo', 'from-client', run_async=True),
            mock.call('disconnect', 'foo',
                      mock_server.reason.TRANSPORT_CLOSE, run_async=False),
        ])

    def test_upgrade_without_connected_socket_starts_new_transport(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = False
        self._record_queue(s)
        s._websocket_handler(FakeWebSocket([None]))
        self._join_bg_tasks()

        assert s.connected
        assert s.upgraded
        assert not s.upgrading
        # this is a fresh websocket connection, not a failed upgrade probe
        assert s.closed

    def test_disconnect_after_successful_upgrade_is_transport_close(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'foo')
        s.connected = True
        self._record_queue(s)
        ws = FakeWebSocket([probe_packet(), upgrade_packet(), None])
        s._websocket_handler(ws)

        # a normal websocket disconnect after a committed upgrade
        assert s.upgraded
        assert s.closed
        assert not s.upgrading
        mock_server._trigger_event.assert_called_once_with(
            'disconnect', 'foo', mock_server.reason.TRANSPORT_CLOSE,
            run_async=False)

    def test_send_after_close(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.close(wait=False)
        with pytest.raises(exceptions.SocketIsClosedError):
            s.send(packet.Packet(packet.NOOP))

    def test_close_after_close(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.close(wait=False)
        assert s.closed
        assert mock_server._trigger_event.call_count == 1
        mock_server._trigger_event.assert_called_once_with(
            'disconnect', 'sid', mock_server.reason.SERVER_DISCONNECT,
            run_async=False
        )
        s.close()
        assert mock_server._trigger_event.call_count == 1

    def test_close_and_wait(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.queue = mock.MagicMock()
        s.close(wait=True)
        s.queue.join.assert_called_once_with()

    def test_close_without_wait(self):
        mock_server = self._get_mock_server()
        s = socket.Socket(mock_server, 'sid')
        s.queue = mock.MagicMock()
        s.close(wait=False)
        assert s.queue.join.call_count == 0
