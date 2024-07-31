import boto3
import json

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

def create_embeddings(text, region_name, model = 'amazon.titan-embed-text-v2:0'):
    bedrock_runtime = boto3.client(service_name = 'bedrock-runtime', region_name = region_name)
    
    try:
        response = bedrock_runtime.invoke_model(
            modelId= model,
            contentType= "application/json",
            accept= "application/json",
            body= json.dumps({'inputText': text})
        )
        embeddings = json.loads(response['body'].read()).get('embedding')
        print("Success")
        return embeddings
    except Exception as e:
        print(f"An error occurred: {e}")
        return None