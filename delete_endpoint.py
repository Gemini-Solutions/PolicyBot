import json
import os
from utils.s3 import delete_from_s3
from utils.documentDB import get_db_connection, delete_documents_from_db


def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        bucket_name = body['bucket_name']
        object_key = body['object_key']

        # Delete from S3
        if not delete_from_s3(bucket_name, object_key):
            raise ValueError("Failed to delete from S3.")

        # Delete from DocumentDB
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db['ExtractedTexts']
        deleted_count = delete_documents_from_db(collection, object_key)

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully deleted {deleted_count} documents related to {object_key} from DocumentDB.')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }