import boto3
import pymongo
import json
import time
from bson import json_util
import os
from dotenv import load_dotenv
from utils.textract import extract_text_from_pdf
from utils.documentDB import get_db_connection, insert_one_entry, find_one_entry, update_entry, delete_one_entry

load_dotenv()

connection_string = os.getenv('CONNECTION_STRING')

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        extracted_text = extract_text_from_pdf(bucket_name, object_key)
        
        if extracted_text is None:
            raise ValueError("Failed to extract text from the document.")
        
        document = {
            'bucket_name': bucket_name,
            'object_key': object_key 
        }
        
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db['ExtractedTexts']
        insert_one_entry(collection, document)
        document = {
            'bucket_name': bucket_name,
            'object_key': object_key,
            'extracted_text': extracted_text,
            # 'embedding': embeddings
        }
        insert_one_entry('Table', document)
        insert_id = update_entry(collection, query = f"{'bucket_name':bucket_name},{'object_key': object_key}", update_query=document)
        find_one_entry(collection, None)
        delete_one_entry(collection, {{'bucket_name':bucket_name},{'object_key':object_key}})

        return {
            'statusCode': 200,
            'body': json.dumps('DocumentDB connection and insertion succeeded!', default=json_util.default)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }