import argparse
import sys
from pymongo import MongoClient
from pymongo.errors import OperationFailure


def drop_database(client, database_name):
    try:
        client.drop_database(database_name)
        print(f"Database '{database_name}' dropped successfully.")
    except OperationFailure as e:
        print(f"Error: {e}")
        sys.exit(1)


def drop_collection(client, db_name, collection_name):
    try:
        db = client[db_name]
        if collection_name in db.list_collection_names():
            db.drop_collection(collection_name)
            print(f"Collection '{collection_name}' in database '{db_name}' dropped successfully.")
        else:
            print(f"Error: Collection '{collection_name}' does not exist in database '{db_name}'.")
            sys.exit(1)
    except OperationFailure as e:
        print(f"Error: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Drop a MongoDB database or collection.")
    parser.add_argument('--host', type=str, default='mongodb://localhost:27017/',
                        help="MongoDB connection URL (default: 'mongodb://localhost:27017/').")
    parser.add_argument('--database', type=str, help="Name of the database to drop.")
    parser.add_argument('--collection', type=str,
                        help="Name of the collection to drop in the format 'database_name.collection_name'.")

    args = parser.parse_args()

    client = MongoClient(args.host)

    if args.database:
        drop_database(client, args.database)
    elif args.collection:
        if '.' not in args.collection:
            print("Error: Collection name must be in the format 'database_name.collection_name'.")
            sys.exit(1)
        db_name, collection_name = args.collection.split('.', 1)
        drop_collection(client, db_name, collection_name)
    else:
        print("Error: You must specify either --database or --collection.")
        sys.exit(1)


if __name__ == '__main__':
    main()
