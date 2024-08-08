import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client(service_name='s3')

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

def delete_from_s3(bucket_name, object_key):
    s3_client = boto3.client(service_name='s3')
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"Deleted {object_key} from bucket {bucket_name}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except PartialCredentialsError:
        print("Incomplete credentials provided")
        return False
    except Exception as e:
        print(f"An error occurred while deleting from S3: {e}")
        return False
    
def save_text_to_s3(text, bucket, object_name):
    s3_client = boto3.client(service_name='s3')
    try:
        s3_client.put_object(Body=text, Bucket=bucket, Key=object_name)
        print(f"Text file saved to {bucket}/{object_name}")
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    
upload_to_s3( 'PDF\Medical Health Insurance Policy.pdf', 'policybot')