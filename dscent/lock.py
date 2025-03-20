import threading
from contextlib import contextmanager


class ReaderWriterLock:
    """A reader-writer lock allowing multiple readers or one writer at a time."""

    def __init__(self):
        self._readers = 0
        self._writer = False
        self._condition = threading.Condition()

    @contextmanager
    def read_lock(self):
        """Context manager for acquiring a read lock."""
        with self._condition:
            while self._writer:  # Wait if a writer is active
                self._condition.wait()
            self._readers += 1  # Increment reader count

        try:
            yield  # Allow reading
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()  # Wake up waiting writers

    @contextmanager
    def write_lock(self):
        """Context manager for acquiring a write lock."""
        with self._condition:
            while self._writer or self._readers > 0:  # Wait if readers or a writer are active
                self._condition.wait()
            self._writer = True  # Set writer active

        try:
            yield  # Allow writing
        finally:
            with self._condition:
                self._writer = False  # Reset writer flag
                self._condition.notify_all()  # Wake up waiting readers or writers
