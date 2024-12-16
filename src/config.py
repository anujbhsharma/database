# src/config.py
import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# PDF directory
PDF_DIR = PROJECT_ROOT / "pdfs"

# Weaviate configuration
WEAVIATE_HOST = "http://localhost:8080"
BATCH_SIZE = 50  # Number of documents to process in batch

# Schema configuration
SCHEMA_CONFIG = {
    "class": "Document",
    "vectorizer": "text2vec-transformers",
    "properties": [
        {
            "name": "content",
            "dataType": ["text"],
            "description": "The text content from the PDF"
        },
        {
            "name": "page_number",
            "dataType": ["int"],
            "description": "Page number in the PDF"
        },
        {
            "name": "file_name",
            "dataType": ["string"],
            "description": "Name of the PDF file"
        },
        {
            "name": "chunk_number",
            "dataType": ["int"],
            "description": "Chunk number for large pages"
        }
    ]
}