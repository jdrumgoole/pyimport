import argparse
from pymongo import MongoClient

def update_and_output_collection(host, database, collection_name, out_collection_name):
    # Connect to MongoDB
    client = MongoClient(host)
    db = client[database]
    collection = db[collection_name]

    # Define the aggregation pipeline

    pipeline = [
        {
            "$set": {
                "DatabaseWorkedWith": {
                    "$split": ["$DatabaseWorkedWith", ";"]
                }
            }
        },
        {
            "$set": {
                "year": collection_name
            }
        },
    ]

    if out_collection_name:
        pipeline[-1]["$out"] = out_collection_name
    # Execute the aggregation pipeline
    collection.aggregate(pipeline)

    # Close the MongoDB connection
    client.close()


def main(host, database, collections, out_collection_name):
    for collection_name in collections:
        update_and_output_collection(host, database, collection_name, out_collection_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update DatabaseWorkedWith field in MongoDB documents and "
                                                 "output to a new collection.")
    parser.add_argument('--host', type=str, default="mongodb://localhost:27017/", help='MongoDB host, e.g., mongodb://localhost:27017/')
    parser.add_argument('--database', type=str, required=True, help='Name of the database.')
    parser.add_argument('--collections', type=str, nargs='+', help='Names of the collections.')
    parser.add_argument('--out', type=str, help='Name of the output collection.')

    args = parser.parse_args()
    main(args.host, args.database, args.collections, args.out)
