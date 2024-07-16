import pickle
import io

import pymongo
from bson import CodecOptions

from pyimport.audit import Audit
from pyimport.monotonicid import MonotonicID


def can_pickle(obj):
    try:
        # Attempt to pickle the object
        pickle.dumps(obj)
        return True
    except (pickle.PicklingError, TypeError) as e:
        # Handle the exception if the object cannot be pickled
        print(f"Cannot pickle object: {e}")
        return False


# Example usage
class MyClass:
    def __init__(self, value):
        self.value = value


class TestPickle(object):
    name = "audit"

    def __init__(self, host, database_name: str, collection_name: str):

        client = pymongo.MongoClient(host)
        database = client[database_name]
        options = CodecOptions(tz_aware=True)
        self._col = database.get_collection(collection_name, options)
        #self._open_batch_count = 0
        #self._current_batch_id : MonotonicID = None
        #indexes = self._col.index_information()

# Create instances
obj1 = MyClass(10)
client = pymongo.MongoClient()
db = client["database"]
col = db["collection"]
a = TestPickle("mongodb://localhost:27017", "database", "collection")

# Check if the instances can be pickled

print(f"Can pickle TestPickle: {can_pickle(col)}")  # Should print: Cannot pickle object: This class cannot be pickled
