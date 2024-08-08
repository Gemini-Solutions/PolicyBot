import json
from bson import json_util
from utils.docDB import find_one_entry
from utils.docdbVS import get_collection
from utils.excel_uploader import read_and_clean_excel_or_csv_from_s3
import os
from dotenv import load_dotenv
load_dotenv()

connection_string = os.getenv('CONNECTION_STRING')

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        read_and_clean_excel_or_csv_from_s3(bucket_name, object_key)
        collection = get_collection(connection_string, 'PolicyDB', 'ExtractedTexts')
        document = find_one_entry(collection, 'Is it neccessary to take leave for LTA?')
        print(document)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Success!', default=json_util.default)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }