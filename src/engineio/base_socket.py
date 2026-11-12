import threading
import time


class BaseSocket:
    upgrade_protocols = ['websocket']

    def __init__(self, server, sid):
        self.server = server
        self.sid = sid
        self.queue = self.server.create_queue()
        # ``last_ping`` stores a monotonic timestamp. It is ``None`` while a
        # ping wait cycle is in progress (i.e. the next ping has been
        # scheduled but not sent yet).
        self.last_ping = self._now()
        # Each scheduled ping wait cycle gets its own generation number. A
        # background task that wakes up after its cycle was superseded or
        # canceled must not send a ping nor refresh the liveness timestamp.
        self.ping_generation = 0
        # Protects the ping state machine against concurrent pong packets in
        # thread based async modes. Uncontended in asyncio based modes.
        self._ping_lock = threading.Lock()
        self.connected = False
        self.upgrading = False
        self.upgraded = False
        self.closing = False
        self.closed = False
        self.session = {}

    def _now(self):
        """Return a monotonic timestamp for connection liveness checks.

        The returned clock can never jump backwards and is unaffected by
        adjustments of the system wall clock (manual changes or NTP
        synchronization), so a clock rollback cannot make a dead client look
        alive. Use :func:`time.time` only for human readable log output.
        """
        return time.monotonic()
