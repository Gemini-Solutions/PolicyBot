import boto3
import json
from botocore.exceptions import ClientError


def call_llm(prompt, region_name='ap-south-1', model='meta.llama3-8b-instruct-v1:0', max_tokens=512):
    client = boto3.client("bedrock-runtime", region_name=region_name)
    formatted_prompt = f"""
<|begin_of_text|>
<|start_header_id|>user<|end_header_id|>
{prompt}
<|eot_id|>
<|start_header_id|>assistant<|end_header_id|>
"""
    native_request = {
        "prompt": formatted_prompt,
        "max_gen_len": max_tokens,
        "temperature": 0.5,
    }

    try:
        # Invoke the model with the request.
        response = client.invoke_model(
            modelId=model, 
            body=json.dumps(native_request)
        )
    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke '{model}'. Reason: {e}")
        exit(1)
    result = json.loads(response["body"].read())["generation"]
    return result


def create_embeddings(text, region_name='us-east-1', model='amazon.titan-embed-text-v2:0'):
    bedrock_runtime = boto3.client(service_name='bedrock-runtime', region_name=region_name)
    
    try:
        response = bedrock_runtime.invoke_model(
            modelId=model,
            contentType="application/json",
            accept="application/json",
            body=json.dumps({'inputText': text})
        )
        response_body = json.loads(response['body'].read())
        embeddings = response_body.get('embedding')
        print("Success")
        return embeddings
    except Exception as e:
        print(f"An error occurred: {e}")
        return None