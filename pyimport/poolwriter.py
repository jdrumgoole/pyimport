

from queue import Queue, Empty
import threading

class PoolWriter:

    BATCH_SIZE = 1000

    def __init__(self, collection, timeout=0.01, worker_count=2):
        self._collection = collection
        self._queue = Queue()
        self._timeout = timeout
        self._threads = {}
        self._count = 0
        for i in range(worker_count):
            self._threads[i] = threading.Thread(target=self.writer, args=[i], daemon=True)

    def put(self, item):
        self._queue.put(item=item)
        print(f"queue length for put() : {self._queue.qsize()}")

    def start(self):
        for _,v in self._threads.items():
            v.start()

    def stop(self):
        for k,v in self._threads.items():
            print(f"stopping thread {k}")
            self._queue.put(None)
        self._queue.join()
        for k,v in self._threads.items():
            v.join()
        self._threads = {}
        return self._count

    @property
    def count(self):
        return self._count

    def writer(self, thread_id: int):
        items = []
        while True:
            try:
                item = self._queue.get(block=True, timeout=self._timeout)
                if item is None:
                    self._queue.task_done()
                    print(f"exiting writer: {thread_id}")
                    break
                else:
                    items.append(item)
                    self._queue.task_done()
                if len(items) >= self.BATCH_SIZE:
                    print(f"queue length for {thread_id} : {self._queue.qsize()}")
                    self._collection.insert_many(items)
                    self._count = self._count + len(items)
                    items = []
            except Empty:
                pass
                #time.sleep(self._timeout or 0.001)
            except KeyboardInterrupt:
                break

        if len(items) > 0:
            self._collection.insert_many(items)
            self._count = self._count + len(items)
            #print("inserted at end")
