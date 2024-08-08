import os
import json
import time
from bson import json_util
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from utils.bedrock_call import create_embeddings, call_llm
from utils.text_extraction import extract_text_from_pdf
from utils.docDB import insert_one_entry
from utils.chunk_embed import create_chunks
from excel_uploader import read_and_clean_excel_or_csv_from_s3
from utils.docdbVS import get_collection, similarity_search
# Load environment variables
load_dotenv()

# Flask app setup
app = Flask(__name__)

@app.route('/', methods=['/POST'])
def upload_endpoint(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        if object_key.endswith('.pdf'):
            extracted_text = extract_text_from_pdf(bucket_name, object_key)

            if extracted_text is None:
                raise ValueError("Failed to extract text from the document.")

            chunks = create_chunks(extracted_text)

            for chunk in chunks:
                embedding = create_embeddings(chunk)
                if embedding is not None:
                    chunk_info = {
                        'name': object_key,
                        'unique_id': str(time.time()),  # Generate a unique ID based on the current time
                        'text': chunk,
                        'embedding': embedding
                    }
                    insert_one_entry('ExtractedTexts', chunk_info)

        elif object_key.endswith('.xlsx') or object_key.endswith('.csv'):
            read_and_clean_excel_or_csv_from_s3(bucket_name, object_key)

        return {
            'statusCode': 200,
            'body': json.dumps(f'Success!', default=json_util.default)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
# Flask route to handle query requests
@app.route('/query', methods=['POST'])
def query_endpoint():
    try:
        data = request.get_json()
        query = data['query']
        
        # Generate embeddings for the query
        query_embedding = create_embeddings(query)
        if query_embedding is None:
            return jsonify({"error": "Failed to generate embeddings"}), 500
        
        # Connect to DocumentDB
        conn_str = os.getenv('CONNECTION_STRING')
        collection = get_collection(conn_str, 'PolicyDB', 'Table')
        
        # Perform similarity search
        top_k_results = similarity_search(collection, query_embedding, 'embedding_key', 'ExtractedTexts', k=5)
        
        # Prepare the prompt for the LLM
        prompt = f"""
The user asked the following query:
{query}

Here are the top relevant chunks of text from the database along with their metadata:
"""
        for idx, result in enumerate(top_k_results):
            prompt += f"\nChunk {idx+1}:\nText: {result['ExtractedTexts']}\nMetadata: {result['metadata']}\nSimilarity: {result['similarity']}"
        
        prompt += "\nBased on the above information, generate a comprehensive answer to the user's query and provide references to the relevant chunks."
        
        # Call the LLM to generate a response
        response = call_llm(prompt)
        if response is None:
            return jsonify({"error": "Failed to generate response from LLM"}), 500
        
        return jsonify({"response": response})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
