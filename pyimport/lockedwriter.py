import time
from queue import Queue, Empty
import threading
from threading import Lock, Event


class ThreadWriter:
    BATCH_SIZE = 1000

    def __init__(self, collection, timeout=None):
        self._collection = collection
        self._writer_thread = None
        self._count = 0
        self._lock = Lock()
        self._event = Event()
        self._items = []
        self._timeout = timeout

    def put(self, item):
        with self._lock:
            self._items.append(item)

    def start(self):
        if self._writer_thread is None:
            self._writer_thread = threading.Thread(target=self.writer)
            self._writer_thread.start()
            self._count = 0
            self._event.set()

    def stop(self):
        if self._writer_thread:
            self._event.clear()
            self._writer_thread.join()
            self._writer_thread = None
        return self._count

    @property
    def count(self):
        with self._lock:
            return self._count

    def writer(self):
        while self._event.is_set():
            with self._lock:
                current_items = len(self._items)
                if current_items >= self.BATCH_SIZE:
                    self._collection.insert_many(self._items)
                    self._count = self._count + current_items
                    self._items = []
            time.sleep(self._timeout or 0.001)

        with self._lock:
            if len(self._items) > 0:
                self._collection.insert_many(self._items)
                self._count = self._count + len(self._items)

