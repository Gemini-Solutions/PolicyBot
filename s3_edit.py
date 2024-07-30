import os
import boto3
import time
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

### STORAGE FUNCTIONS using S3
def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"File {file_name} uploaded to {bucket}/{object_name}")
        return object_name
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None
    except PartialCredentialsError:
        print("Incomplete credentials provided")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def upload_pdfs_in_folder(folder_path, bucket):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith('.pdf'):
                file_path = os.path.join(root, file)
                upload_to_s3(file_path, bucket)

def delete_from_s3(bucket, object_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.delete_object(Bucket=bucket, Key=object_name)
        print(f"File {object_name} deleted from {bucket}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except PartialCredentialsError:
        print("Incomplete credentials provided")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    

### TEXT EXTRACTION FUNCTIONS using textract
def extract_text_from_pdf(bucket, document_key):
    textract_client = boto3.client('textract')

    try:
        response = textract_client.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document_key}}
        )
        job_id = response['JobId']
        print(f"Started text detection job with JobId: {job_id}")

        # Wait for the job to complete
        while True:
            response = textract_client.get_document_text_detection(JobId=job_id)
            status = response['JobStatus']
            if status in ['SUCCEEDED', 'FAILED']:
                break
            print("Waiting for job to complete...")
            time.sleep(5)

        if status == 'SUCCEEDED':
            extracted_text = ''
            for block in response['Blocks']:
                if block['BlockType'] == 'LINE':
                    extracted_text += block['Text'] + '\n'
            return extracted_text
        else:
            print(f"Text detection failed with status: {status}")
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def save_text_to_s3(text, bucket, object_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=text, Bucket=bucket, Key=object_name)
        print(f"Text file saved to {bucket}/{object_name}")
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

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