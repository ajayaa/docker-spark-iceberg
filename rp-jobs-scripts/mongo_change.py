from pymongo import MongoClient
import pymongo
import time
from bson.objectid import ObjectId
print(pymongo.__version__)
print(pymongo.__file__)

replica_set_uri = "mongodb://localhost:27017/?replicaSet=dbrs"
replica_set_uri = "mongodb://localhost:27017/"
#replica_set_uri = "mongodb://172.18.0.4:27017/"
#replica_set_uri = "mongodb://mongodb:27017/?replicaSet=dbrs"

client = MongoClient(replica_set_uri)

db = client['ripplingdb']
collection = db['aj']

document = {
    "name": "John Doe",
    "age": 30,
    "email": "john.doe@example.com"
}

# Insert the document with a comment
try:
    result = collection.insert_one(document)
    #result = collection.update_one({"_id": ObjectId("66ab5e49f57760665ee60562")}, {"$inc": {"age": 1}})
    #result = collection.update_one({"_id": ObjectId("66ab5e49f57760665ee60562")}, {"$set": {"name": "foo"}})
    print(f"Document inserted with ID: {result.inserted_id}")
except pymongo.errors.PyMongoError as e:
    print(f"An error occurred while inserting the document: {e}")

