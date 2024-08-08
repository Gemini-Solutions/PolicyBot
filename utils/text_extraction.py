import os
import boto3
import time
from utils.bucket_call import upload_to_s3, save_text_to_s3


def extract_text_from_pdf(bucket, document_key):
    textract_client = boto3.client(service_name='textract')

    try:
        response = textract_client.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document_key}}
        )
        job_id = response.get('JobId')
        if not job_id:
            print("Failed to retrieve JobId from response.")
            return None
        
        print(f"Started text detection job with JobId: {job_id}")

        while True:
            response = textract_client.get_document_text_detection(JobId=job_id)
            status = response.get('JobStatus')
            if not status:
                print("Failed to retrieve JobStatus from response.")
                return None
            
            print(f"Current job status: {status}")
            if status in ['SUCCEEDED', 'FAILED']:
                break
            print("Extracting...")
            time.sleep(5)

        if status == 'SUCCEEDED':
            extracted_text = {}
            next_token = None
            while True:
                if next_token:
                    response = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
                else:
                    response = textract_client.get_document_text_detection(JobId=job_id)
                blocks = response.get('Blocks', [])
                if not blocks:
                    break
                
                for block in blocks:
                    if block.get('BlockType') == 'LINE':
                        page_number = block.get('Page')
                        if page_number is None:
                            continue
                        if page_number not in extracted_text:
                            extracted_text[page_number] = ''
                        extracted_text[page_number] += block.get('Text', '') + '\n'
                
                next_token = response.get('NextToken')
                if not next_token:
                    break

            return extracted_text
        else:
            print(f"Text detection failed with status: {status}")
            print("Detailed response:", response)
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    
def pdf_upload_and_text_extraction(file_path, bucket):
    # Upload the PDF to S3
    document_key = upload_to_s3(file_path, bucket)
    if document_key is None:
        return

    # Extract text from the PDF using Textract
    extracted_text = extract_text_from_pdf(bucket, document_key)
    if extracted_text is None:
        return
    # Save the extracted text to a .txt file in S3
    save_text_to_s3(extracted_text, bucket, os.path.splitext(document_key)[0] + '.txt')