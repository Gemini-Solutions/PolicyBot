import pymongo
import os
from pymongo import MongoClient
from typing import List
from dotenv import load_dotenv
load_dotenv()

connection_string = os.getenv('CONNECTION_STRING')
def get_db_connection():
    try:
        client = pymongo.MongoClient(connection_string)
        db = client.PolicyDB
        return db
    except pymongo.errors.ConnectionError as e:
        print(f"Could not connect to MongoDB: {e}")
        return None


def insert_one_entry(collection_name, document):
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


def insert_many_entry(collection_name: str, documents: List):
    try:
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db[collection_name]
        result = collection.insert_many(documents)
        return result.inserted_id
    except Exception as e:
        print(f"An error occurred during insert: {e}")
        return None

def find_one_entry(collection_name, filter_query):
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


def find_all_entries(collection_name, projection=None):
    db = get_db_connection()
    if db is None:
        raise ValueError("Failed to connect to DocumentDB.")

    collection = db[collection_name]
    documents = list(collection.find({}, projection))
    return documents

    
def update_entry(collection_name, filter_query, update_query):
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
    

def delete_one_entry(collection_name, filter_query):
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


def delete_documents_from_db(collection, object_key):
    try:
        result = collection.delete_many({'name': object_key})
        print(f"Deleted {result.deleted_count} documents from DocumentDB for key {object_key}")
        return result.deleted_count
    except Exception as e:
        print(f"An error occurred while deleting from DocumentDB: {e}")
        return 0


def similarity_search(query_embedding, collection_name: str, top_k=5):
    client = MongoClient(connection_string)
    db = client.get_database()
    collection = db[collection_name]
    
    pipeline = [
        {
            "$search": {
                "knnBeta": {
                    "vector": query_embedding,
                    "path": "embedding",
                    "k": top_k
                }
            }
        }
    ]

    docs = collection.aggregate(pipeline)
    results = [(doc, doc['_id']) for doc in docs]
    
    return results[:top_k]


def vector_search(chunks, embeddings, collection_name: str):
    client = MongoClient(connection_string)
    db = client.get_database()
    collection = db[collection_name]
    
    documents = [
        {"textContent": chunk, "embedding": embedding}
        for chunk, embedding in zip(chunks, embeddings)
    ]
    
    collection.insert_many(documents)
    
    collection.create_index(
        [("embedding", "vector")],
        name="embedding_index",
        options={
            "vectorOptions": {
                "type": "hnsw",
                "similarity": "cosine",
                "dimensions": 1536,
                "m": 16,
                "efConstruction": 64
            }
        }
    )
#     db.collection.createIndex(
#   { "vectorEmbedding": "vector" },
#   { "name": "myIndex",
#     "vectorOptions": {
#       "type": "hnsw",
#       "dimensions": 3,
#       "similarity": "euclidean",
#       "m": 16,
#       "efConstruction": 64
#     }
#   }
# );

    for index in collection.list_indexes():
        print(index)

    return "Success"