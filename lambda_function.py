import boto3
import pymongo
import json
import time
from bson import json_util

PASS = "Gemini_12345"

def get_db_connection():
    try:
        client = pymongo.MongoClient('mongodb://gempolicyadmin:Gemini_12345@PolicyDB-851725235990.ap-south-1.docdb-elastic.amazonaws.com:27017/?ssl=true&retryWrites=false') 
        db = client.PolicyDB
        return db
    except pymongo.errors.ConnectionError as e:
        print(f"Could not connect to MongoDB: {e}")
        return None

def extract_text_from_pdf(bucket, document_key):
    textract_client = boto3.client(service_name='textract')

    try:
        response = textract_client.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document_key}}
        )
        job_id = response['JobId']
        print(f"Started text detection job with JobId: {job_id}")

        while True:
            response = textract_client.get_document_text_detection(JobId=job_id)
            status = response['JobStatus']
            print(f"Current job status: {status}")
            if status in ['SUCCEEDED', 'FAILED']:
                break
            print("Extracting...")
            time.sleep(5)

        if status == 'SUCCEEDED':
            extracted_text = ''
            for block in response['Blocks']:
                if block['BlockType'] == 'LINE':
                    extracted_text += block['Text'] + '\n'
            return extracted_text
        else:
            print(f"Text detection failed with status: {status}")
            print("Detailed response:", response)
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        extracted_text = extract_text_from_pdf(bucket_name, object_key)
        
        if extracted_text is None:
            raise ValueError("Failed to extract text from the document.")
        
        document = {
            'bucket_name': bucket_name,
            'object_key': object_key,
            'extracted_text': extracted_text
        }
        
        db = get_db_connection()
        if db is None:
            raise ValueError("Failed to connect to DocumentDB.")
        
        collection = db['ExtractedTexts']
        collection.insert_one(document)
        
        return {
            'statusCode': 200,
            'body': json.dumps('DocumentDB connection and insertion succeeded!', default=json_util.default)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
