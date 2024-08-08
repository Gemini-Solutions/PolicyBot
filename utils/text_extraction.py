import os
import boto3
import time
import logging
from utils.bucket_call import upload_to_s3, save_text_to_s3



def start_job(client, s3_bucket_name, object_name):
    response = None
    response = client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket_name,
                'Name': object_name
            }})

    return response["JobId"]


def is_job_complete(client, job_id):
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    status = response["JobStatus"]
    logging.info("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(1)
        response = client.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]
        logging.info("Job status: {}".format(status))

    return status


def get_job_results(client, job_id: str):
    pages = []
    time.sleep(1)
    response = client.get_document_text_detection(JobId=job_id)
    pages.append(response)
    logging.info("No. of pages received: {}".format(len(pages)))
    next_token = None
    if 'NextToken' in response:
        next_token = response['NextToken']

    while next_token:
        time.sleep(1)
        response = client.\
            get_document_text_detection(JobId=job_id, NextToken=next_token)
        pages.append(response)
        logging.info("No. of pages received: {}".format(len(pages)))
        next_token = None
        if 'NextToken' in response:
            next_token = response['NextToken']

    return pages



def extract_text_from_pdf(bucket: str, document_key: str):
    
    textract_client = boto3.client(service_name='textract', region_name='ap-south-1')
    try:
        job_id = start_job(textract_client, bucket, document_key)
        logging.info("Started job with id: {}".format(job_id))
        status = is_job_complete(textract_client, job_id)
        if status == 'SUCCEEDED':
            response = get_job_results(textract_client, job_id)
            extracted_text = {}
            for res in response:
                for block in res['Blocks']:
                    if block['BlockType'] == 'LINE':
                        page_number = block['Page']
                        if page_number not in extracted_text:
                            extracted_text[page_number] = ''
                        extracted_text[page_number] += f"{block['Text']}\n"
        else:
            logging.info(f"Text detection failed with status: {status}")
            logging.info("Detailed response:", response)
            return None
    except Exception as e:
        logging.info(f"An error occurred: {e}")
        return None
    return extracted_text
    
def pdf_upload_and_text_extraction(file_path: str, bucket: str):
    '''
    For running locally and Testing 
    '''
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