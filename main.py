from utils.bedrock import create_embeddings
from utils.documentDB import similarity_search

print("Welcome to Policy Bot. How may I help you?")

# Example usage
query = "Can you tell me about the Sick leaves policy?"

query_embedding = create_embeddings(query)
top_k_results = similarity_search(query_embedding, 'ExtractedTexts', top_k=5)

for result in top_k_results:
    doc, similarity = result
    print(f"Document Name: {doc['name']}, Unique ID: {doc['unique_id']}, Similarity: {similarity}")