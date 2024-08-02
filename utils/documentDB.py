import pymongo
import os
import numpy as np
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
    
def find_all_entries(collection_name, query=None):
    db = get_db_connection()
    if db is None:
        raise ValueError("Failed to connect to DocumentDB.")
    
    collection = db[collection_name]
    documents = list(collection.find({},query))
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
    
def cosine_similarity(vec1, vec2):
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    return dot_product / (norm_vec1 * norm_vec2)

def similarity_search(query_embedding, collection_name, top_k=5):
    documents = find_all_entries(collection_name)
    similarities = []
    
    for doc in documents:
        similarity = cosine_similarity(query_embedding, doc['embedding'])
        similarities.append((doc, similarity))
    
    similarities.sort(key=lambda x: x[1], reverse=True)
    
    return similarities[:top_k]