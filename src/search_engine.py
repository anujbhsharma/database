# src/search_engine.py
from typing import List, Dict, Any
import weaviate
from . import config
import logging

logging.basicConfig(level=logging.INFO)

class SearchEngine:
    def __init__(self):
        self.client = weaviate.Client(config.WEAVIATE_HOST)
    
    def search(self, 
              query: str, 
              limit: int = 5, 
              min_score: float = 0.7) -> List[Dict[str, Any]]:
        """
        Perform semantic search on the document collection.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            min_score: Minimum similarity score (0-1) for results
            
        Returns:
            List of matching documents with metadata
        """
        try:
            response = (
                self.client.query
                .get("Document", ["content", "page_number", "file_name", "chunk_number", "_additional {certainty}"])
                .with_near_text({"concepts": [query]})
                .with_limit(limit)
                .do()
            )
            
            results = response['data']['Get']['Document']
            
            # Filter and format results
            formatted_results = []
            for r in results:
                certainty = r['_additional']['certainty']
                if certainty >= min_score:
                    formatted_results.append({
                        'content': r['content'],
                        'page_number': r['page_number'],
                        'file_name': r['file_name'],
                        'chunk_number': r['chunk_number'],
                        'relevance_score': round(certainty * 100, 2)
                    })
            
            return formatted_results
            
        except Exception as e:
            logging.error(f"Search error: {str(e)}")
            return []

    def get_document_count(self) -> int:
        """Get the total number of documents in the database."""
        try:
            response = (
                self.client.query
                .aggregate("Document")
                .with_meta_count()
                .do()
            )
            return response['data']['Aggregate']['Document'][0]['meta']['count']
        except Exception as e:
            logging.error(f"Error getting document count: {str(e)}")
            return 0