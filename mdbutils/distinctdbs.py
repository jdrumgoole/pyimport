from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['your_database_name']
collection = db['myCollection']

# Define the aggregation pipeline
pipeline = [{"$unwind": "$DatabaseHaveWorkedWith"},
            {"$group": {"_id": None, "allTags": {"$addToSet": "$DatabaseHaveWorkedWith"}}},
            {"$project": {"_id": 0, "allTags": 1}}]

# Execute the aggregation pipeline
result = collection.aggregate(pipeline)

# Print the distinct tags
for doc in result:
    print(doc['allTags'])
