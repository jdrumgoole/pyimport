import threading
import time


class MonotonicID:
    _lock = threading.Lock()
    _last_timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds
    _counter = 0

    def __init__(self, mid: int | None = None):
        with MonotonicID._lock:
            if mid is None:
                current_timestamp = int(time.time() * 1000)
                if current_timestamp == MonotonicID._last_timestamp:
                    MonotonicID._counter += 1
                else:
                    MonotonicID._last_timestamp = current_timestamp
                    MonotonicID._counter = 0

                self._id = (current_timestamp << 16) | (MonotonicID._counter & 0xFFFF)
            else:
                self._id = mid

    @property
    def id(self):
        return self._id

    def __eq__(self, other):
        if isinstance(other, MonotonicID):
            return self._id == other._id
        return False

    def __lt__(self, other):
        if isinstance(other, MonotonicID):
            return self._id < other._id
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, MonotonicID):
            return self._id > other._id
        return NotImplemented

    def __le__(self, other):
        return self == other or self < other

    def __ge__(self, other):
        return self == other or self > other

    def __int__(self):
        return self._id

    def __repr__(self):
        return f"MonotonicID({self._id})"

    def __hash__(self):
        return hash(self._id)

# # Example usage
# id1 = MonotonicID()
# time.sleep(0.001)  # Ensure the next ID is generated at a different timestamp
# id2 = MonotonicID()
# time.sleep(0.001)
# id3 = MonotonicID()
#
# print(id1)  # MonotonicID(some-increasing-id)
# print(id2)  # MonotonicID(some-increasing-id)
# print(id3)  # MonotonicID(some-increasing-id)
#
# print(id1 == id2)  # False
# print(id1 < id2)   # True
# print(id2 > id1)   # True
# print(id2 == id2)  # True
# print(id3 >= id1)  # True
# print(id1 <= id2)  # True
