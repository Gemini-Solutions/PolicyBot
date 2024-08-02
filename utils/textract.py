import os
import boto3
import time
from s3 import upload_to_s3, save_text_to_s3

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