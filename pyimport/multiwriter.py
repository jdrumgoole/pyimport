from pymongo import MongoClient
from sqlalchemy import create_engine, Table, Column, Integer, MetaData, select, and_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import json


class DatabaseWriter:
    def __init__(self, db_url):
        self.db_url = db_url
        if 'mongodb' in db_url:
            self.client = MongoClient(db_url)
            self.db = self.client.get_database()
        elif 'postgresql' in db_url:
            self.engine = create_engine(db_url)
            self.metadata = MetaData(bind=self.engine)
            self.session = sessionmaker(bind=self.engine)()
        else:
            raise ValueError("Unsupported database URL")

    def write(self, data, collection_name):
        if 'mongodb' in self.db_url:
            self._write_to_mongodb(data, collection_name)
        elif 'postgresql' in self.db_url:
            self._write_to_postgresql(data, collection_name)
        else:
            raise ValueError("Unsupported database URL")

    def _write_to_mongodb(self, data, collection_name):
        collection = self.db[collection_name]
        if isinstance(data, list):
            collection.insert_many(data)
        else:
            collection.insert_one(data)

    def _write_to_postgresql(self, data, table_name):
        if not self.engine.dialect.has_table(self.engine, table_name):
            Table(
                table_name, self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('data', JSONB),
                extend_existing=True
            ).create(self.engine)

        table = Table(table_name, self.metadata, autoload_with=self.engine)
        if isinstance(data, list):
            self.session.execute(table.insert(), [{'data': json.dumps(d)} for d in data])
        else:
            self.session.execute(table.insert().values(data=json.dumps(data)))
        self.session.commit()

    def find_one(self, query, collection_name):
        if 'mongodb' in self.db_url:
            return self._find_one_mongodb(query, collection_name)
        elif 'postgresql' in self.db_url:
            return self._find_one_postgresql(query, collection_name)
        else:
            raise ValueError("Unsupported database URL")

    def find_many(self, query, collection_name):
        if 'mongodb' in self.db_url:
            return self._find_many_mongodb(query, collection_name)
        elif 'postgresql' in self.db_url:
            return self._find_many_postgresql(query, collection_name)
        else:
            raise ValueError("Unsupported database URL")

    def _find_one_mongodb(self, query, collection_name):
        collection = self.db[collection_name]
        return collection.find_one(query)

    def _find_many_mongodb(self, query, collection_name):
        collection = self.db[collection_name]
        return collection.find(query)

    def _find_one_postgresql(self, query, table_name):
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        translated_query = self._translate_query(query)
        result = self.session.execute(
            select([table.c.data]).where(translated_query)
        ).fetchone()
        return json.loads(result[0]) if result else None

    def _find_many_postgresql(self, query, table_name):
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        translated_query = self._translate_query(query)
        results = self.session.execute(
            select([table.c.data]).where(translated_query)
        ).fetchall()
        return (json.loads(result[0]) for result in results)

    def _translate_query(self, query):
        conditions = []
        for key, value in query.items():
            if isinstance(value, dict):
                for operator, val in value.items():
                    if operator == '$eq':
                        conditions.append(text(f"data->>'{key}' = '{val}'"))
                    elif operator == '$ne':
                        conditions.append(text(f"data->>'{key}' != '{val}'"))
                    # Add more MongoDB to SQL translations as needed
            else:
                conditions.append(text(f"data->>'{key}' = '{value}'"))

        return and_(*conditions)


# Example Usage
db_writer_mongo = DatabaseWriter('mongodb://localhost:27017/mydatabase')
db_writer_postgres = DatabaseWriter('postgresql://user:password@localhost:5432/mydatabase')

# Writing to MongoDB
data = {'name': 'Alice', 'age': 30}
db_writer_mongo.write(data, 'mycollection')

# Writing to Postgres
data_list = [{'name': 'Bob', 'age': 25}, {'name': 'Charlie', 'age': 35}]
db_writer_postgres.write(data_list, 'mytable')

# Finding in MongoDB
query = {'name': 'Alice'}
result = db_writer_mongo.find_one(query, 'mycollection')
print(result)

# Finding in Postgres
result = db_writer_postgres.find_one(query, 'mytable')
print(result)
