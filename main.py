# main.py
from src.pdf_processor import PDFProcessor
from src.search_engine import SearchEngine
import logging

def main():
    # Initialize PDF processor and process documents
    # processor = PDFProcessor(batch_size=500)
    
    # processor.process_directory()
    
    # Initialize search engine
    search_engine = SearchEngine()
    
    # Get total documents indexed
    doc_count = search_engine.get_document_count()
    print(f"\nTotal documents indexed: {doc_count}")
    
    # Interactive search loop
    while True:
        query = input("\nEnter your search query (or 'quit' to exit): ")
        if query.lower() == 'quit':
            break
            
        results = search_engine.search(query)
        
        if not results:
            print("No matching documents found.")
            continue
            
        print("\nSearch Results:")
        print("-" * 80)
        
        for idx, result in enumerate(results, 1):
            print(f"\nResult {idx} (Relevance: {result['relevance_score']}%):")
            print(f"File: {result['file_name']}")
            print(f"Page: {result['page_number']}, Chunk: {result['chunk_number']}")
            print(f"Content: {result['content'][:200]}...")
            print("-" * 80)

if __name__ == "__main__":
    main()