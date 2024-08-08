from utils.text_extraction import extract_text_from_pdf
from utils.chunk_embed import create_chunks, create_embeddings_for_chunks
import datetime
from utils.docDB import insert_many_entry


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
    metadata = {
            'source': 'S3 upload',
            'upload_date': datetime.datetime.now().isoformat(),
            'file_name': object_key,
            
    }
    docs = [{
        'text': chunks[i],
        'embedding': embedding,
        'metadata': metadata
    } for i, embedding in enumerate(chunk_embeddings)]

    insert_many_entry('Table', docs)

    return document


if __name__ == '__main__':
    pdf_handler('policybot','Access Policy.pdf')