import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"File {file_name} uploaded to {bucket}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except PartialCredentialsError:
        print("Incomplete credentials provided")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

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