import boto3
import json

def call_llm(prompt,model = 'meta.llama3-70b-instruct-v1:0', max_tokens=100):
    client = boto3.client(service_name='bedrock-runtime')
    try:
        response = client.invoke_model(
            modelId=model,
            contentType= "application/json",
            accept= "application/json",
            body={
                'input_text': prompt,
                'max_tokens': max_tokens
            }
        )
        result = response['body'].read().decode('utf-8')
        return result
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def create_embeddings(text, region_name='us-east-1', model='amazon.titan-embed-text-v2:0'):
    bedrock_runtime = boto3.client(service_name='bedrock-runtime', region_name=region_name)
    
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