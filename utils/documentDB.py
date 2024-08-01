import pymongo
import os
from dotenv import load_dotenv
load_dotenv()

connection_string = os.getenv('CONNECTION_STRING')
def get_db_connection():
    try:
        client = pymongo.MongoClient('mongodb://gempolicyadmin:Gemini_12345@PolicyDB-851725235990.ap-south-1.docdb-elastic.amazonaws.com:27017/?ssl=true&retryWrites=false')
        db = client.PolicyDB
        return db
    except pymongo.errors.ConnectionError as e:
        print(f"Could not connect to MongoDB: {e}")
        return None
    
def insert_document(collection_name, document):
    try:
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db[collection_name]
        result = collection.insert_one(document)
        return result.inserted_id
    except Exception as e:
        print(f"An error occurred during insert: {e}")
        return None
    
def find_document(collection_name, filter_query):
    try:
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db[collection_name]
        document = collection.find_one(filter_query)
        return document
    except Exception as e:
        print(f"An error occurred during find: {e}")
        return None

def update_document(collection_name, filter_query, update_query):
    try:
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db[collection_name]
        result = collection.update_one(filter_query, {'$set': update_query})
        return result.modified_count
    except Exception as e:
        print(f"An error occurred during update: {e}")
        return None

def delete_document(collection_name, filter_query):
    try:
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db[collection_name]
        result = collection.delete_one(filter_query)
        return result.deleted_count
    except Exception as e:
        print(f"An error occurred during delete: {e}")
        return None