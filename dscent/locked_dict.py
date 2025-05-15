from collections import defaultdict
from contextlib import contextmanager
from threading import Lock
from typing import Callable, Any, Hashable, Generator


class LockedDefaultDict(defaultdict):
    def __init__(self, default_factory: Callable[[], Any], **kwargs):
        super().__init__(default_factory, **kwargs)
        self._global_lock: Lock = Lock()
        self._key_locks: dict[Hashable, Lock] = {}

    def __delitem__(self, key: Hashable):
        with self._global_lock:
            if key in self._key_locks:
                self._key_locks.pop(key)
            super().__delitem__(key)

    @contextmanager
    def access(self, key: Hashable) -> Generator[Any, None, None]:
        """
        Context manager for accessing a value under a per-key lock.

        Example:
            with mydict.access("foo") as val:
                val += 1
        """
        # Ensure lock for the key exists
        with self._global_lock:
            if key not in self._key_locks:
                self._key_locks[key] = Lock()
            key_lock = self._key_locks[key]  # Capture the lock object

        # Lock access to this specific key
        with key_lock:
            yield self[key]
