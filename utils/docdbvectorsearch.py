from pymongo import MongoClient
from bson.objectid import ObjectId
from enum import Enum
from typing import List, Dict, Any, Optional


class DocumentDBSimilarityType(str, Enum):
    """DocumentDB Similarity Type as enumerator."""
    COS = "cosine"
    DOT = "dotProduct"
    EUC = "euclidean"


def get_collection(connection_string: str, db_name: str, collection_name: str):
    client = MongoClient(connection_string)
    collection = client[db_name][collection_name]
    return collection


def create_index(collection, index_name: str, embedding_key: str, dimensions: int = 1536,
                 similarity: DocumentDBSimilarityType = DocumentDBSimilarityType.COS,
                 m: int = 16, ef_construction: int = 64) -> Dict[str, Any]:
    create_index_commands = {
        "createIndexes": collection.name,
        "indexes": [
            {
                "name": index_name,
                "key": {embedding_key: "vector"},
                "vectorOptions": {
                    "type": "hnsw",
                    "similarity": similarity,
                    "dimensions": dimensions,
                    "m": m,
                    "efConstruction": ef_construction,
                },
            }
        ],
    }

    current_database = collection.database
    create_index_responses = current_database.command(create_index_commands)
    return create_index_responses


def index_exists(collection, index_name: str) -> bool:
    cursor = collection.list_indexes()
    for res in cursor:
        if res.get("name") == index_name:
            return True
    return False


def delete_index(collection, index_name: str) -> None:
    if index_exists(collection, index_name):
        collection.drop_index(index_name)


def add_documents(collection, texts: List[str], embeddings: List[List[float]],
                  text_key: str, embedding_key: str, metadatas: Optional[List[Dict[str, Any]]] = None):
    if not texts:
        return []

    metadatas = metadatas or [{} for _ in texts]
    to_insert = [
        {text_key: t, embedding_key: e, **m}
        for t, e, m in zip(texts, embeddings, metadatas)
    ]

    insert_result = collection.insert_many(to_insert)
    return insert_result.inserted_ids


def delete_document_by_id(collection, document_id: str) -> None:
    collection.delete_one({"_id": ObjectId(document_id)})


def similarity_search(collection, embedding: List[float], embedding_key: str, text_key: str,
                      similarity: DocumentDBSimilarityType = DocumentDBSimilarityType.COS, k: int = 4,
                      ef_search: int = 40, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    if not filter:
        filter = {}

    pipeline = [
        {"$match": filter},
        {
            "$search": {
                "vectorSearch": {
                    "vector": embedding,
                    "path": embedding_key,
                    "similarity": similarity,
                    "k": k,
                    "efSearch": ef_search,
                }
            },
        },
    ]

    cursor = collection.aggregate(pipeline)
    docs = [{text_key: res.pop(text_key), **res} for res in cursor]
    return docs