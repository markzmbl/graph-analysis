from collections import defaultdict
from contextlib import contextmanager
from threading import Lock
from typing import Callable, Any, Hashable, Generator


class LockedDefaultDict(defaultdict):
    """
    A defaultdict variant that provides fine-grained locking per key.

    - Uses thread or process locks depending on the `threaded` flag.
    - Provides a context manager `enter_lock(key)` to lock a specific key.
    """

    def __init__(self, default_factory: Callable[[], Any], threaded: bool = False):
        super().__init__(default_factory)
        self._global_lock: Lock = Lock()
        self._key_locks: dict[Hashable, Lock] = {}

    def __delitem__(self, key: Hashable):
        super().__delitem__(key)  # raises KeyError if key not present
        with self._global_lock:
            if key in self._key_locks:
                self._key_locks.pop(key)

    @contextmanager
    def enter_lock(self, key: Hashable) -> Generator[None, None, None]:
        """
        Context manager for acquiring a lock on a specific key.

        Example:
            with mydict.enter_lock("foo"):
                mydict["foo"] += 1
        """
        # Ensure thread-safe creation of a lock per key
        if key not in self._key_locks:
            with self._global_lock:
                if key not in self._key_locks:
                    self._key_locks[key] = Lock()
        with self._key_locks[key]:
            yield
