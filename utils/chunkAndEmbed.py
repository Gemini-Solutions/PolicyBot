from langchain.text_splitter import RecursiveCharacterTextSplitter # type: ignore
from bedrock import create_embeddings


def chunk(text, chunkSize=512, overlap=32):
    try:
        textSplitter = RecursiveCharacterTextSplitter(
        chunk_size=chunkSize,
        chunk_overlap=overlap,
        length_function=len,
        is_separator_regex=False
        )
        chunks = textSplitter.split_text(text)
        return chunks
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def create_chunks_embedding(chunks, region_name, model = 'amazon.titan-embed-text-v2:0'):
    # given a list of chunks return the list of embeddings
    try:
        embeddings = []
        for chunk in chunks:
            embedding = create_embeddings(chunk, region_name, model)
            embeddings.append(embedding)
        return embeddings
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
#chunks = chunk("Indian we are the bbbb kiiis ljdsfesf. njjsnfsdf lokaf adica akdianc kjnakncc cnjae", chunkSize=10, overlap =5)
#embeddings = create_chunks_embedding(chunks, 'us-east-1')
#print(len(embeddings[0]))