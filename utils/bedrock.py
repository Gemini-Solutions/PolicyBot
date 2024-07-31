import boto3

def call_llama_70b(prompt, max_tokens=100):
    client = boto3.client('bedrock')
    try:
        response = client.invoke_endpoint(
            # arn:aws:bedrock:ap-south-1::foundation-model/meta.llama3-70b-instruct-v1:0
            EndpointName='meta.llama3-70b-instruct-v1:0',
            Body={
                'input_text': prompt,
                'max_tokens': max_tokens
            }
        )
        result = response['Body'].read().decode('utf-8')
        return result
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def create_embeddings(text):
    client = boto3.client('bedrock')
    try:
        response = client.invoke_endpoint(
            EndpointName='amazon.titan-embed-text-v2:0',
            # arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0
            Body={
                'text': text
            }
        )
        embeddings = response['Body'].read().decode('utf-8')
        return embeddings
    except Exception as e:
        print(f"An error occurred: {e}")
        return None