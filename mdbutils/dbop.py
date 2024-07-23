import argparse
import json
import os
import pprint
import random
import string
import sys
from pymongo import MongoClient
from pymongo.errors import OperationFailure


def drop_database(args, client, database_name):
    try:
        dbs = client.admin.command('listDatabases')
        if database_name in dbs['databases']:
            client.drop_database(database_name)
            print(f"Database '{database_name}' dropped successfully.")
        elif args.strict:
            print(f"Error: Database '{database_name}' does not exist on {client.HOST}.")
            sys.exit(1)
        else:
            print("No such database, nothing to do.")
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


def drop_collection(args, client, db_name, collection_name):
    try:
        db = client[db_name]
        if collection_name in db.list_collection_names():
            db.drop_collection(collection_name)
            print(f"Collection {db_name}.{collection_name} dropped.")
        elif  args.strict:
            print(f"Error: Collection '{collection_name}' does not exist in database '{db_name}'.")
            sys.exit(1)
        else:
            print("No such collection, nothing to do.")
    except OperationFailure as e:
        print(f"Error: {e}")
        sys.exit(1)


def save_resume_token(token, path):
    with open(path, 'w') as f:
        json.dump(token, f)


def load_resume_token(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return None


def watch_collection(host, restart_log, database_name, collection_name):
    client = MongoClient(host)
    db = client[database_name]  # replace with your database name
    collection = db[collection_name] # replace with your collection name

    resume_token = load_resume_token(restart_log)

    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'replace']}}}]

    if resume_token:
        change_stream = collection.watch(pipeline, resume_after=resume_token)
    else:
        change_stream = collection.watch(pipeline)

    try:
        for change in change_stream:
            print(change)
            save_resume_token(change['_id'], restart_log)
    except Exception as e:
        print(f"Error: {e}")
    except KeyboardInterrupt:
        print("Interrupted by user.")


def main():
    parser = argparse.ArgumentParser(description="MongoDB server, database and collection operations.")
    parser.add_argument("--ping", action="store_true", default=False, help="Ping the MongoDB server.")
    parser.add_argument("--serverinfo", action="store_true", default=False, help="Get server info.")
    parser.add_argument('--touch', help="Touch the collection.")
    parser.add_argument('--drop', help="Drop the database or collection.")
    parser.add_argument('--count', help="Drop the database or collection.")
    parser.add_argument('--host', type=str, default='mongodb://localhost:27017/',
                        help="MongoDB connection URL (default: 'mongodb://localhost:27017/').")
    parser.add_argument('--strict', '-s', action='store_true', default=False, help="Fail if db or collection does not exist.")
    parser.add_argument('--watch', help="Watch a collection for changes.")
    parser.add_argument('--timeout', type=int, default=1000, help="Timeout in milliseconds (default: 1000).")

    args = parser.parse_args()

    try:
        client = MongoClient(args.host, serverSelectionTimeoutMS=args.timeout)

        if args.ping:
            client.admin.command('ping')
            server_info = client.server_info()
            print(f"Connected to MongoDB server: '{args.mdburi}' (server version: {server_info['version']})")
        if args.serverinfo:
            server_info = client.server_info()
            pprint.pprint(server_info)
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
                drop_database(args, client, db)
            else:
                drop_collection(args, client, db, col)
        if args.count:
            db, col = parse_db_and_col(args.count)
            if col is None:
                print("Error: You must specify a collection to count in the format 'database_name.collection_name'.")
                sys.exit(1)
            else:
                count = client[db][col].count_documents({})
                print(f"count {db}.{col}:{count}")
        if args.watch:
            db, col = parse_db_and_col(args.watch)
            if col is None:
                print("Error: You must specify a collection to watch in the format 'database_name.collection_name'.")
                sys.exit(1)
            else:
                watch_collection(args.host, args.watch + '.log', db, col)

    except ConnectionError as e:
        print(f"Failed connect to MongoDB server: {e}")
        sys.exit(1)



if __name__ == '__main__':
    main()
