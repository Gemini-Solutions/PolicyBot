from utils.text_extraction import extract_text_from_pdf
from utils.chunk_embed import create_chunks, create_embeddings_for_chunks
import datetime
from utils.docDB import insert_many_entry
import logging
import uuid
logging.getLogger().setLevel(logging.INFO)


def pdf_handler(bucket_name: str, object_key: str):

    extracted_text = extract_text_from_pdf(bucket_name, object_key)

    if extracted_text is None:
        raise ValueError("Failed to extract text from the document.")
    
    for page, text in extracted_text.items():
        logging.info(f"Page {page}")
        chunks = create_chunks(text)
        embeddings = create_embeddings_for_chunks(chunks)
        chunk_infos = [{
                    'name': object_key,
                    'page_no': page,
                    'unique_id': str(uuid.uuid1()),  # Generate a unique ID 
                    'text': chunks[i],
                    'embedding': embedding
                } for i, embedding in enumerate(embeddings) if embeddings is not None]
        
    try:
        insert_many_entry('ExtractedTexts', chunk_infos)
    except:
        pass

    return chunk_infos

if __name__ == '__main__':
    chunk_infos = pdf_handler('policybot','Medical Health Insurance Policy.pdf')
    print(chunk_infos)