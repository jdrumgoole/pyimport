import time
from queue import Queue, Empty
import threading
from threading import Event


class ThreadWriter:
    BATCH_SIZE = 1000

    def __init__(self, collection, timeout=None):
        self._collection = collection
        self._queue = Queue()
        self._timeout = timeout
        self._writer_thread = None
        self._count = 0

    def put(self, item):
        self._queue.put(item=item)

    def start(self):
        if self._writer_thread is None:
            self._writer_thread = threading.Thread(target=self.writer)
            self._writer_thread.start()
            self._count = 0

    def stop(self):
        if self._writer_thread:
            self._queue.put({})
            self._queue.join()
            self._writer_thread.join()
            self._writer_thread = None
        return self._count


    @property
    def count(self):
        return self._count

    def writer(self):
        items = []
        while True:
            try:
                item = self._queue.get(block=False, timeout=0)
                print(f"queue length : {self._queue.qsize()}")
                if item == {}:
                    self._queue.task_done()
                    break
                else:
                    items.append(item)
                    self._queue.task_done()
                if len(items) >= self.BATCH_SIZE:
                    self._collection.insert_many(items)
                    self._count = self._count + len(items)
                    items = []
            except Empty:
                pass
                #time.sleep(self._timeout or 0.001)

        if len(items) > 0:
            self._collection.insert_many(items)
            self._count = self._count + len(items)
            #print("inserted at end")
