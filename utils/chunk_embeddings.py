import boto3
from langchain.text_splitter import RecursiveCharacterTextSplitter # type: ignore
from bedrock import create_embeddings
from typing import List 

def create_chunks(text: str, chunkSize: int=512, overlap: int=32):
    textSplitter = RecursiveCharacterTextSplitter(
      chunk_size=chunkSize,
      chunk_overlap=overlap,
      length_function=len,
      is_separator_regex=False
    )
    chunks = textSplitter.split_text(text)
    return chunks

def create_embeddings_for_chunks(chunks: List[str], region_name: str = 'us-east-1', model: str = 'amazon.titan-embed-text-v2:0'):
    # given a list of chunks return the list of embeddings
    try:
        embeddings = [create_embeddings(chunk, region_name, model) for chunk in chunks]
        return embeddings
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
# print(chunk("Indian we are the bbbb kiiis ljdsfesf. njjsnfsdf lokaf adica akdianc kjnakncc cnjae", chunkSize=10, overlap =5))
#embeddings = create_embeddings_chunks(chunks, 'us-east-1')
# #print(len(embeddings[0]))