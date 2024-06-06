import argparse
import random
import string
import sys
from pymongo import MongoClient
from pymongo.errors import OperationFailure


def drop_database(client, database_name):
    try:
        dbs = client.admin.command('listDatabases')
        if database_name in dbs['databases']:
            client.drop_database(database_name)
            print(f"Database '{database_name}' dropped successfully.")
        else:
            print(f"Error: Database '{database_name}' does not exist on {client.HOST}.")
            sys.exit(1)
    except OperationFailure as e:
        print(f"Error: {e}")
        sys.exit(1)


def generate_random_string(length=8):
    alphabet = string.ascii_letters + string.digits
    return ''.join(random.choice(alphabet) for _ in range(length))


def touch(client, database_name: str, collection_name: str):
    try:
        db = client[database_name]
        col = db[collection_name]
        key = generate_random_string()
        val = generate_random_string()
        col.insert_one({key: val})
        col.delete_one({key: val})
        print(f"Collection {database_name}.{collection_name} : touched successfully.")
        return col
    except OperationFailure as e:
        print(f"Error: {e}")
        sys.exit(1)


def parse_db_and_col(s: str) -> [str, str]:
    if '.' not in s:
        return s, None
    db_name, collection_name = s.split('.', 1)
    return db_name, collection_name


def drop_collection(client, db_name, collection_name):
    try:
        db = client[db_name]
        if collection_name in db.list_collection_names():
            db.drop_collection(collection_name)
            print(f"Collection {db_name}.{collection_name} dropped.")
        else:
            print(f"Error: Collection '{collection_name}' does not exist in database '{db_name}'.")
            sys.exit(1)
    except OperationFailure as e:
        print(f"Error: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Drop a MongoDB database or collection.")
    parser.add_argument('--touch', help="Touch the collection.")
    parser.add_argument('--drop', help="Drop the database or collection.")
    parser.add_argument('--host', type=str, default='mongodb://localhost:27017/',
                        help="MongoDB connection URL (default: 'mongodb://localhost:27017/').")

    args = parser.parse_args()

    client = MongoClient(args.host)

    if args.touch:
        db, col = parse_db_and_col(args.touch)
        if col is None:
            print("Error: You must specify a collection to touch in the format 'database_name.collection_name'.")
            sys.exit(1)
        else:
            touch(client, db, col)
    if args.drop:
        db, col = parse_db_and_col(args.drop)
        if col is None:
            drop_database(client, db)
        else:
            drop_collection(client, db, col)


if __name__ == '__main__':
    main()
