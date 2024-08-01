from utils.textract import extract_text_from_pdf
from utils.chunk_embeddings import create_chunks, create_embeddings_for_chunks


def pdf_handler(bucket_name: str, object_key: str):
    # Extract Text from PDF
    extracted_text = extract_text_from_pdf(bucket=bucket_name, document_key=object_key)
    chunks = create_chunks(extracted_text)
    chunk_embeddings = create_embeddings_for_chunks(chunks)
    document = {
        'bucket_name': bucket_name,
        'object_key': object_key,
        'extracted_text': extracted_text,
        'embedding': chunk_embeddings 
    }

    return document