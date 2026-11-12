"""Shared lifecycle regression scenarios for the Engine.IO server.

These tests pin down which application callbacks may still run once a
connection starts closing. Device control instructions are executed from the
``message`` callback, so the boundary after a CLOSE packet must be explicit.

Synchronization is done with admission/callback barriers (events), never with
timed sleeps, so the recorded event sequences are deterministic and
replayable.

The sync (threading) suite runs these scenarios through ``LifecycleTestMixin``.
The asyncio suite in ``tests/async/test_lifecycle.py`` mirrors the same
scenarios and assertions with async/await primitives; the encoded packet
constants below are shared by both.
"""
from engineio import packet
from engineio import payload as eio_payload


# Encoded packets reused across scenarios.
MSG_BEFORE = packet.Packet(packet.MESSAGE, data='before').encode()
CLOSE = packet.Packet(packet.CLOSE).encode()
MSG_AFTER = packet.Packet(packet.MESSAGE, data='after').encode()
MSG_LATE = packet.Packet(packet.MESSAGE, data='late').encode()


def join_packets(*encoded):
    return '\x1e'.join(encoded)


class LifecycleTestMixin:
    """Scenario definitions. Subclasses supply the primitives in Driver."""

    # Set by subclasses: engineio Server / AsyncServer class.
    server_class = None
    async_handlers = True

    # ---- primitives provided by the concrete driver ---------------------
    def make_server(self, **kwargs):
        raise NotImplementedError

    def connect(self, server):
        """Run a polling connect and return the new sid."""
        raise NotImplementedError

    def post(self, server, sid, raw_body):
        """Deliver a polling POST and return the HTTP status string."""
        raise NotImplementedError

    def start_post(self, server, sid, raw_body):
        """Start a POST concurrently; return a joinable handle."""
        raise NotImplementedError

    def join_handle(self, handle, timeout=5):
        raise NotImplementedError

    def handle_status(self, handle):
        """Status recorded by a handle created with start_post()."""
        raise NotImplementedError

    def start_get(self, server, sid):
        raise NotImplementedError

    def event(self):
        """Return a mode appropriate one-shot barrier."""
        raise NotImplementedError

    def wait(self, barrier, timeout=5):
        raise NotImplementedError

    def set(self, barrier):
        raise NotImplementedError

    def sleep_tick(self):
        """Yield execution so competing tasks can make progress.

        This is a scheduler yield, not an ordering guess: assertions always
        wait on explicit barriers before checking the recorded sequence.
        """
        raise NotImplementedError

    # ---- shared scenario configuration ----------------------------------
    def install_handlers(self, server, record, message_gate=None,
                         disconnect_gate=None):
        """Register message/disconnect handlers that record every boundary.

        ``record`` appends strings describing each callback start/end and the
        reason reported to disconnect.
        """
        raise NotImplementedError

    # =====================================================================
    # Scenario 1: MESSAGE, CLOSE, MESSAGE inside one polling payload
    # =====================================================================
    def test_message_after_close_in_same_payload_is_ignored(self):
        server = self.make_server(async_handlers=self.async_handlers,
                                  monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = self.connect(server)

        status = self.post(
            server, sid, join_packets(MSG_BEFORE, CLOSE, MSG_AFTER))

        assert status.startswith('200')
        assert sid not in server.sockets
        messages = [event for event in record if event.startswith('message:')]
        # The message admitted before CLOSE keeps its completion semantics;
        # the message after CLOSE must never reach the application.
        assert messages == ['message:before']
        assert record.count('disconnect:client disconnect') == 1

    # =====================================================================
    # Scenario 2: two concurrent POSTs, one admitted before closing starts
    # =====================================================================
    def test_concurrent_post_admitted_before_close_completes(self):
        server = self.make_server(async_handlers=True,
                                  monitor_clients=False)
        record = []
        first_message_started = self.event()
        release_first_message = self.event()
        disconnect_started = self.event()
        release_disconnect = self.event()
        self.install_handlers(
            server, record, message_gate=(first_message_started,
                                          release_first_message),
            disconnect_gate=(disconnect_started, release_disconnect))
        sid = self.connect(server)

        first = self.start_post(
            server, sid, join_packets(MSG_BEFORE, CLOSE))
        self.wait(first_message_started)
        self.wait(disconnect_started)

        # A second POST carrying a brand new command races the in-progress
        # close. It was not admitted before closing started, so its command
        # must be rejected instead of being delivered.
        second = self.start_post(server, sid, MSG_LATE)
        self.join_handle(second)

        assert self.handle_status(second).startswith('400')
        assert 'message:late' not in record

        self.set(release_first_message)
        self.set(release_disconnect)
        self.join_handle(first)
        assert self.handle_status(first).startswith('200')
        assert [event for event in record
                if event.startswith('message:')] == ['message:before']
        assert record.count('disconnect:client disconnect') == 1
        assert sid not in server.sockets

    # =====================================================================
    # Scenario 3: a GET already waiting when the socket starts closing
    # =====================================================================
    def test_waiting_get_is_released_by_close(self):
        server = self.make_server(monitor_clients=False)
        record = []
        disconnect_started = self.event()
        release_disconnect = self.event()
        self.install_handlers(
            server, record,
            disconnect_gate=(disconnect_started, release_disconnect))
        sid = self.connect(server)

        waiting_get = self.start_get(server, sid)
        self.sleep_tick()  # let the GET park on the queue (no ordering guess)
        assert not waiting_get['done']()

        close_post = self.start_post(server, sid, CLOSE)
        self.wait(disconnect_started)
        self.set(release_disconnect)
        self.join_handle(close_post)
        self.join_handle(waiting_get['handle'])

        assert waiting_get['status']().startswith('200')
        # The close sentinel drains the long poll as an empty payload.
        decoded = eio_payload.Payload(
            encoded_payload=waiting_get['body']().decode()).packets
        assert decoded == []
        assert record == ['disconnect:client disconnect']

    # =====================================================================
    # Scenario 4: disconnect callback still running when new work arrives
    # =====================================================================
    def test_new_post_blocked_while_disconnect_callback_runs(self):
        server = self.make_server(monitor_clients=False)
        record = []
        disconnect_started = self.event()
        release_disconnect = self.event()
        self.install_handlers(
            server, record,
            disconnect_gate=(disconnect_started, release_disconnect))
        sid = self.connect(server)

        close_post = self.start_post(server, sid, CLOSE)
        self.wait(disconnect_started)

        late_post = self.start_post(server, sid, MSG_LATE)
        self.join_handle(late_post)
        assert self.handle_status(late_post).startswith('400')
        assert 'message:late' not in record

        self.set(release_disconnect)
        self.join_handle(close_post)
        assert record == ['disconnect:client disconnect']
        assert sid not in server.sockets

    # =====================================================================
    # Scenario 5: server initiated disconnect
    # =====================================================================
    def test_server_disconnect_rejects_further_posts(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = self.connect(server)

        # A server initiated close enqueues a CLOSE packet, waits for the
        # outgoing queue to drain, and leaves a sentinel. Reproduce a real
        # client: a parked poll delivers the CLOSE and a second parked poll
        # consumes the empty sentinel, which releases the close wait.
        first_get = self.start_get(server, sid)
        second_get = self.start_get(server, sid)
        self.sleep_tick()
        disconnect_handle = self.start_disconnect(server, sid)
        self.join_handle(disconnect_handle)
        self.join_handle(first_get['handle'])
        self.join_handle(second_get['handle'])

        assert record == ['disconnect:server disconnect']
        assert sid not in server.sockets
        status = self.post(server, sid, MSG_LATE)
        assert status.startswith('400')
        assert 'message:late' not in record

    def start_disconnect(self, server, sid):
        raise NotImplementedError

    # =====================================================================
    # Scenario 6: malformed payload must not look like a clean disconnect
    # =====================================================================
    def test_malformed_payload_is_bad_request_without_disconnect(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = self.connect(server)

        # An empty packet segment cannot even be decoded. This is a protocol
        # error, not a client initiated logout: no disconnect event fires and
        # the session stays open for the client to retry.
        assert self.post(server, sid, '\x1e').startswith('400')
        assert record == []
        assert sid in server.sockets

    def test_unknown_packet_type_is_protocol_error_disconnect(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = self.connect(server)

        # A decodable but unknown packet type is a different failure mode:
        # the connection is torn down and reported as a transport error
        # instead of being silently accepted.
        assert self.post(server, sid, '9').startswith('400')
        assert record == ['disconnect:transport error']
        assert sid not in server.sockets

    def test_valid_close_is_distinct_from_parse_failure(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = self.connect(server)

        assert self.post(server, sid, CLOSE).startswith('200')
        assert record == ['disconnect:client disconnect']
        assert sid not in server.sockets

        # Any later request references an unknown session.
        assert self.post(server, sid, MSG_LATE).startswith('400')
        assert record == ['disconnect:client disconnect']

    # =====================================================================
    # Scenario 7: disconnect is reported exactly once under re-entry
    # =====================================================================
    def test_disconnect_event_fires_once(self):
        server = self.make_server(monitor_clients=False)
        record = []
        self.install_handlers(server, record)
        sid = self.connect(server)

        self.post(server, sid, CLOSE)
        # A second CLOSE races the already closed socket.
        second_close = self.start_post(server, sid, CLOSE)
        self.join_handle(second_close)

        assert record == ['disconnect:client disconnect']
        assert self.handle_status(second_close).startswith('400')
