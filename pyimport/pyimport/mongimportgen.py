import pandas as pd
import json
# Load the CSV file
url = "https://jdrumgoole.s3.eu-west-1.amazonaws.com/2018_Yellow_Taxi_Trip_Data_1000.csv"
df = pd.read_csv(url, delimiter=";")

# Generate the field names
fields = df.columns.tolist()

# Create the mongoimport field file content
mongoimport_fields = [{"name": field, "type": "auto"} for field in fields]

# Save the fields to a file

with open("mongoimport_fields.json", "w") as f:
    json.dump(mongoimport_fields, f, indent=4)

print("mongoimport fields file created: mongoimport_fields.json")
