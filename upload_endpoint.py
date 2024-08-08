import json
import time
from bson import json_util
from utils.documentDB import insert_one_entry
from utils.textract import extract_text_from_pdf
from utils.bedrock import create_embeddings
from utils.excel_uploader import read_and_clean_excel_or_csv_from_s3


def create_chunks(text: str, chunk_size: int = 512, overlap: int = 32):
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)
        start += chunk_size - overlap
    return chunks


def lambda_handler(event, context):
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