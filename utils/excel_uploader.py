import boto3
import pandas as pd
from io import BytesIO
import datetime
from bedrock import create_embeddings


# Function to read Excel or CSV file from S3
def read_and_clean_excel_or_csv_from_s3(bucket_name, object_key):
    try:
        s3_client = boto3.client('s3')
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = s3_response['Body'].read()
        
        if object_key.endswith('.xlsx'):
            excel_data = pd.read_excel(BytesIO(file_content), sheet_name=None, header=None)  # Read all sheets
        elif object_key.endswith('.csv'):
            # Try reading with different encodings
            try:
                excel_data = pd.read_csv(BytesIO(file_content), encoding='utf-8', header=None)
            except UnicodeDecodeError:
                try:
                    excel_data = pd.read_csv(BytesIO(file_content), encoding='latin1', header=None)
                except UnicodeDecodeError as e:
                    raise ValueError(f"Unable to read CSV file due to encoding issues: {e}")
        else:
            raise ValueError("Unsupported file format")

        excel_data.dropna(axis=1, how='all', inplace=True)
        excel_data = excel_data.iloc[1:, :].reset_index(drop=True)
        excel_data = {'Sheet1': excel_data}  # Create a dictionary with one sheet
        
        cleaned_excel_data = {}
        for sheet_name, sheet_data in excel_data.items():
            # Check if columns 'A' and 'B' contain data
            if sheet_data[0].isna().all() or sheet_data[1].isna().all():
                raise ValueError(f"Sheet {sheet_name} does not contain data in columns 'A' and 'B'. Check the data format.")
 
            initial_row_count = len(sheet_data)
            # Drop rows with null values in 'A' or 'B'
            sheet_data.dropna(subset=[0,1], inplace=True)
            cleaned_row_count = len(sheet_data)
            if cleaned_row_count < initial_row_count:
                dropped_row_percentage = ((initial_row_count - cleaned_row_count) / initial_row_count) * 100
                # print(f"Sheet {sheet_name}: Dropped {initial_row_count - cleaned_row_count} rows ({dropped_row_percentage:.2f}%) due to null values in columns 'A' or 'B'.")
            
            cleaned_excel_data[sheet_name] = sheet_data
        
        return cleaned_excel_data

    except Exception as e:
        print(f"An error occurred while reading the file from S3: {e}")
        return None
    


# Function to process Excel/CSV data and store embeddings in DocumentDB
def process_excel_data_and_store(excel_data, object_key, collection):
    try:
        for sheet_name, sheet_data in excel_data.items():
            for index, row in sheet_data.iterrows():
                question = row[0]
                answer = row[1]
                combined_text = f"Q: {question} A: {answer}"
                combined_embedding = create_embeddings(combined_text)
                # print(combined_embedding)
                if combined_embedding is None:
                    continue
                
                # Add metadata
                metadata = {
                    'source': 'S3 upload',
                    'upload_date': datetime.datetime.now().isoformat(),
                    'file_name': object_key,
                    'sheet_name': sheet_name
                }
                
                # Store data in DocumentDB
                doc = {
                    'question': question,
                    'answer': answer,
                    'embedding': combined_embedding,
                    'metadata': metadata
                }

                collection.insert_one(doc)
        
        print("Data processed and stored successfully!")
    except Exception as e:
        print(f"An error occurred while processing and storing the data: {e}")


