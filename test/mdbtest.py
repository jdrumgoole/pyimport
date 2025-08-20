import logging

from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

from pyimport.argmgr import ArgMgr


class MDBTestDB:

    TEST_DB_NAME = "TEST_DB"
    TEST_COLLECTION_NAME = "test"

    def __init__(self, client=None, uri="mongodb://localhost:27017", db_name=None, collection_name=None, drop_db=True):
        self._uri = uri
        if client:
            self._client = client
        else:
            self._client = MongoClient(self._uri)
        self._databases = {}
        self._collections = {}
        self._init_db_name = db_name
        self._init_collection_name = collection_name
        self._args = ArgMgr.default_args().add_arguments(database=self.TEST_DB_NAME,
                                                         collection=self.TEST_COLLECTION_NAME)
        self._drop_db = drop_db

    def __enter__(self):
        self.create_database(self.TEST_DB_NAME)
        self.create_collection(self.TEST_DB_NAME, self.TEST_COLLECTION_NAME)
        if self._init_db_name and self._init_collection_name:
            self.create_collection(self._init_db_name, self._init_collection_name)
        elif self._init_db_name:
            self.create_database(self._init_db_name)
        elif self._init_collection_name:
            self.create_collection(self.TEST_DB_NAME, self._init_collection_name)
        else:
            pass
        self.test_col.insert_one({"hello": "pyimport"})
        self.test_col.delete_one({"hello": "pyimport"})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._drop_db:
            for k in self._databases.keys():
                self._client.drop_database(k)

    @property
    def test_db_name(self):
        return self.TEST_DB_NAME

    @property
    def test_collection_name(self):
        return self.TEST_COLLECTION_NAME

    def count(self):
        return self.test_col.count_documents({})

    @property
    def args(self):
        return self._args

    @property
    def log(self):
        return logging.getLogger("test")

    @property
    def args_ns(self):
        return self._args.ns

    @property
    def test_db(self):
        return self._databases[self.TEST_DB_NAME]

    @property
    def test_col(self):
        return self._collections[self.TEST_COLLECTION_NAME]

    def create_database(self, db_name):
        if not self._client:
            raise Exception("MongoDB client is not initialized.")
        if db_name not in self._databases:
            self._databases[db_name] = self._client[db_name]
        return self._databases[db_name]

    def create_collection(self, db_name, collection_name):
        if db_name not in self._databases:
            raise Exception(f"Database '{db_name}' does not exist.")
        self._collections[collection_name] = self._databases[db_name][collection_name]
        return self._collections[collection_name]


class AsyncMDBTestDB(MDBTestDB):
    def __init__(self, uri="mongodb://localhost:27017", db_name=None, collection_name=None):
        self._client = AsyncIOMotorClient(uri)
        super().__init__(client=self._client, uri=uri, db_name=db_name, collection_name=collection_name)

    async def __aenter__(self):
        self.create_database(self.TEST_DB_NAME)
        await self.create_collection(self.TEST_DB_NAME, self.TEST_COLLECTION_NAME)
        if self._init_db_name and self._init_collection_name:
            await self.create_collection(self._init_db_name, self._init_collection_name)
        elif self._init_db_name:
            self.create_database(self._init_db_name)
        elif self._init_collection_name:
            await self.create_collection(self.TEST_DB_NAME, self._init_collection_name)
        else:
            pass
        await self.test_col.insert_one({"hello": "pyimport"})
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for k in self._databases.keys():
            await self._client.drop_database(k)
        if self._client:
            self._client.close()

    async def create_collection(self, db_name, collection_name):
        #options = CodecOptions(tz_aware=True)

        if db_name not in self._databases:
            raise Exception(f"Database '{db_name}' does not exist.")
        if collection_name not in await self._databases[db_name].list_collection_names():
            self._collections[collection_name] = await self._databases[db_name].create_collection(collection_name)
        self._collections[collection_name] =  self._databases[db_name].get_collection(collection_name)
        return self._collections[collection_name]


