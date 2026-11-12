import threading
import time


class BaseSocket:
    upgrade_protocols = ['websocket']

    def __init__(self, server, sid):
        self.server = server
        self.sid = sid
        self.queue = self.server.create_queue()
        self.last_ping = time.time()
        self.connected = False
        self.upgrading = False
        self.upgraded = False
        self.upgrade_lock = threading.Lock()
        self._upgrade_candidate = None
        self.closing = False
        self.closed = False
        self.session = {}

    def _begin_upgrade(self, candidate):
        """Reserve the socket for a single candidate transport.

        Returns ``True`` when the candidate owns the upgrade. A second
        candidate that arrives while an upgrade is in progress is rejected
        without disturbing the state of the first one.
        """
        with self.upgrade_lock:
            if self.upgraded:
                return False
            if self.upgrading:
                if self._upgrade_candidate is None:
                    # an upgrade was started without going through
                    # _upgrade_websocket; let this candidate own it
                    self._upgrade_candidate = candidate
                    return True
                return self._upgrade_candidate is candidate
            self.upgrading = True
            self._upgrade_candidate = candidate
            return True

    def _abort_upgrade(self, candidate):
        """Roll back an incomplete upgrade if it belongs to the candidate."""
        with self.upgrade_lock:
            if self._upgrade_candidate is candidate:
                self._upgrade_candidate = None
                self.upgrading = False

    def _commit_upgrade(self, candidate):
        """Atomically promote the candidate to the socket transport."""
        with self.upgrade_lock:
            if self._upgrade_candidate is candidate:
                self._upgrade_candidate = None
                self.upgrading = False
                self.upgraded = True
