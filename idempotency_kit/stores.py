"""Result stores for the idempotency kit.

A store maps an idempotency key to a completed result entry::

    {"result": <the tool's return value>, "stored_at": <clock seconds>}

Only *completed* results are ever stored. A call that raises stores
nothing, so a retry with the same key can still execute -- a failure
never poisons the key.

Two stores ship with the kit:

* :class:`MemoryStore` -- fast, in-process, holds any Python object.
* :class:`FileStore` -- a JSON file on disk, so replays survive a
  process restart. Results must be JSON-serialisable.

Both stores are safe to use from multiple threads.
"""

import json
import os
import threading

__all__ = ["MemoryStore", "FileStore"]


class MemoryStore:
    """An in-memory store. Results can be any Python object."""

    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            entry = self._data.get(key)
            return dict(entry) if entry is not None else None

    def set(self, key, entry):
        with self._lock:
            self._data[key] = dict(entry)

    def delete(self, key):
        with self._lock:
            self._data.pop(key, None)

    def keys(self):
        with self._lock:
            return list(self._data)


class FileStore:
    """A JSON file-backed store. Survives process restarts.

    Results must be JSON-serialisable (dicts, lists, strings, numbers,
    booleans, None). Anything else raises :class:`TypeError` at store
    time, loudly, instead of failing silently on the next restart.

    Writes are atomic: the file is written to a temporary sibling and
    then renamed over the old one, so a crash mid-write cannot leave a
    half-written file behind.
    """

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self._lock = threading.Lock()
        self._data = {}
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                self._data = loaded

    def get(self, key):
        with self._lock:
            entry = self._data.get(key)
            return dict(entry) if entry is not None else None

    def set(self, key, entry):
        # Fail fast on results that cannot survive a restart.
        json.dumps(entry["result"])
        with self._lock:
            self._data[key] = dict(entry)
            self._save_locked()

    def delete(self, key):
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._save_locked()

    def keys(self):
        with self._lock:
            return list(self._data)

    def _save_locked(self):
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, sort_keys=True)
        os.replace(tmp_path, self.path)
