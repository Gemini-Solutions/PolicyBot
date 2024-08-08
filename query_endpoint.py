import json
import os
from dotenv import load_dotenv
load_dotenv()
from utils.bedrock import create_embeddings, call_llm
from utils.docdbvectorsearch import get_collection, similarity_search


def lambda_handler(event, context):
    try:
        query = event.get('query')
        if not query:
            raise ValueError("Query parameter is missing or empty.")
        
        (f"Received query: {query}")

        # Generate embeddings for the query
        query_embedding = create_embeddings(query)
        if not query_embedding:
            raise ValueError("Failed to create embeddings for the query.")
        
        connection_string = os.getenv('CONNECTION_STRING')
        db_name = os.getenv('DB_NAME', 'PolicyDB')
        collection_name = os.getenv('COLLECTION_NAME', 'VectorTable')
        collection = get_collection(connection_string, db_name, collection_name)
        
        top_k_results = similarity_search(collection, query_embedding, embedding_key='embedding', text_key='text', k=5)
        
        prompt = f"""
The user asked the following query:
{query}

Here are the top relevant chunks of text from the database along with their metadata:

"""
        for idx, result in enumerate(top_k_results):
            prompt += f"\nChunk {idx+1}:\nText: {result.get('text')}\nMetadata: {result.get('metadata')}\nSimilarity: {result.get('similarity')}"
        
        prompt += "\nBased on the above information, generate a comprehensive answer to the user's query and provide references to the relevant chunks."
        
        response = call_llm(prompt)
        print(f"LLM response: {response}")

        return {
            'statusCode': 200,
            'body': json.dumps({'response': response})
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }