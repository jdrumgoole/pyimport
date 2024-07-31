

import pymongo

from pymongo import MongoClient

# Connect to MongoDB

#
# split for aggregation
db_by_year = [
    {
        '$unwind': '$DatabaseHaveWorkedWith'
    }, {
        '$group': {
            '_id': {
                'year': '$year',
                'database': '$DatabaseHaveWorkedWith'
            },
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$project': {
            '_id': 0,
            'year': '$_id.year',
            'database': '$_id.database',
            'count': '$count'
        }
    }, {
        '$out': 'db_by_year'
    }
]

# Define the aggregation pipeline
field_to_list_pipeline = [
    {
        "$set": {
            "DatabaseHaveWorkedWith": {
                "$map": {
                    "input": { "$split": ["$DatabaseHaveWorkedWith", ";"] },
                    "as": "item",
                    "in": { "$trim": { "input": "$$item" } }
                }
            }
        }
    }
]

# Execute the aggregation pipeline
result = collection.aggregate(pipeline)

# Print the results
for doc in result:
    print(doc)


def rename_field(original_collection, new_collection, field_name, new_field_name, limit=0):
    aggregation = []
    if limit:
        aggregation.append({"$limit": limit})
    aggregation.append({"$addFields": {new_field_name: f"${field_name}"}})
    aggregation.append({"$unset": field_name})
    aggregation.append({"$out": new_collection})
    original_collection.aggregate(aggregation)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rename a field in a MongoDB collection.")
    parser.add_argument('--field', type=str, required=True, help='Name of the field to rename.')
    parser.add_argument('--new_field', type=str, required=True, help='New name of the field.')
    parser.add_argument('--limit', type=int, default=0, help='Limit the number of documents to process.')
    parser.add_argument('--host', type=str, default="mongodb://localhost:27017/",
                        help='MongoDB host, e.g., mongodb://localhost:27017/')
    parser.add_argument('--database', type=str, required=True, help='Name of the database.')
    args = parser.parse_args()
    client = pymongo.MongoClient()
    db = client["test"]
    original_collection = db["original"]
    new_collection = db["new"]
    rename_field(original_collection, new_collection, "old_field", "new_field", 1000)
    client.close()
