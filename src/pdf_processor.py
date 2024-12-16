import PyPDF2
import weaviate
from tqdm import tqdm
from pathlib import Path
import logging
import traceback
from . import config
import json
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pdf_processing.log'),
        logging.StreamHandler()
    ]
)

class PDFProcessor:
    def __init__(self, batch_size=50):
        self.client = weaviate.Client(
            url=config.WEAVIATE_HOST,
            startup_period=30
        )
        self.batch_size = batch_size
        self.backup_dir = Path('weaviate_backups')
        self.backup_dir.mkdir(exist_ok=True)
        self._setup_schema()
        
    def _setup_schema(self):
        """Initialize the Weaviate schema if it doesn't exist."""
        try:
            try:
                self.client.schema.get("Document")
                logging.info("Schema already exists")
            except:
                self.client.schema.create_class(config.SCHEMA_CONFIG)
                logging.info("Schema created successfully")
        except Exception as e:
            logging.error(f"Schema setup error: {str(e)}")
            raise
    
    def _chunk_text(self, text: str, chunk_size: int = 5000) -> list:
        """Split text into larger chunks for better context."""
        words = text.split()
        chunks = []
        current_chunk = []
        current_size = 0
        
        for word in words:
            current_size += len(word) + 1
            if current_size > chunk_size:
                if current_chunk:
                    chunks.append(' '.join(current_chunk))
                current_chunk = [word]
                current_size = len(word)
            else:
                current_chunk.append(word)
                
        if current_chunk:
            chunks.append(' '.join(current_chunk))
            
        return chunks

    def _create_backup(self, filename: str):
        """Create a backup of processed documents for a specific file."""
        try:
            # Query all chunks for this file with all properties
            result = (
                self.client.query
                .get("Document", ["content", "page_number", "file_name", "chunk_number"])  # Correct syntax for fields
                .with_additional(["id"])
                .with_where({
                    "path": ["file_name"],
                    "operator": "Equal",
                    "valueString": filename
                })
                .with_limit(10000)  # Adjust if needed
                .do()
            )
            
            if result and 'data' in result and 'Get' in result['data'] and 'Document' in result['data']['Get']:
                # Create backup filename with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_file = self.backup_dir / f"backup_{filename}_{timestamp}.json"
                
                # Format the backup data
                backup_data = {
                    "metadata": {
                        "filename": filename,
                        "backup_time": timestamp,
                        "total_chunks": len(result['data']['Get']['Document'])
                    },
                    "chunks": result['data']['Get']['Document']
                }
                
                # Save backup with pretty printing for readability
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, indent=2, ensure_ascii=False)
                
                logging.info(f"Backup created for {filename} at {backup_file}")
                return True
        except Exception as e:
            logging.error(f"Backup creation failed for {filename}: {str(e)}")
            return False
        
    def process_pdf(self, pdf_path: Path) -> None:
        """Process a single PDF file with batch processing."""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                batch = []
                
                for page_num in range(len(pdf_reader.pages)):
                    try:
                        page = pdf_reader.pages[page_num]
                        text = page.extract_text()
                        
                        if not text or not text.strip():
                            logging.warning(f"Empty page {page_num + 1} in {pdf_path.name}")
                            continue
                        
                        chunks = self._chunk_text(text)
                        
                        for chunk_num, chunk in enumerate(chunks):
                            if not chunk.strip():
                                continue
                                
                            properties = {
                                "content": chunk,
                                "page_number": page_num + 1,
                                "file_name": str(pdf_path.name),
                                "chunk_number": chunk_num + 1
                            }
                            
                            batch.append(properties)
                            
                            # Process batch when it reaches the size limit
                            if len(batch) >= self.batch_size:
                                self._process_batch(batch)
                                batch = []
                        
                    except Exception as e:
                        logging.error(f"Error processing page {page_num + 1} in {pdf_path.name}: {str(e)}")
                        continue
                
                # Process remaining batch
                if batch:
                    self._process_batch(batch)
                
                # Create backup after processing the file
                self._create_backup(pdf_path.name)
                logging.info(f"Successfully processed {pdf_path.name}")
                
        except Exception as e:
            logging.error(f"Error processing {pdf_path.name}: {str(e)}")
            logging.error(traceback.format_exc())

    def _process_batch(self, batch: list):
        """Process a batch of documents."""
        try:
            with self.client.batch as batch_processor:
                batch_processor.batch_size = self.batch_size
                
                for properties in batch:
                    batch_processor.add_data_object(
                        class_name="Document",
                        data_object=properties
                    )
            
            # Small delay to prevent overwhelming the server
            time.sleep(0.1)
            
        except Exception as e:
            logging.error(f"Batch processing error: {str(e)}")
            raise

    def process_directory(self, directory: Path = config.PDF_DIR) -> None:
        """Process all PDF files in the specified directory."""
        try:
            pdf_files = list(directory.glob("*.pdf"))
            
            if not pdf_files:
                logging.warning(f"No PDF files found in {directory}")
                return
                
            logging.info(f"Found {len(pdf_files)} PDF files to process")
            
            with tqdm(pdf_files, desc="Processing PDFs") as pbar:
                for pdf_file in pbar:
                    try:
                        self.process_pdf(pdf_file)
                    except KeyboardInterrupt:
                        logging.info("Processing interrupted by user")
                        raise
                    except Exception as e:
                        logging.error(f"Failed to process {pdf_file.name}: {str(e)}")
                        continue
                    finally:
                        pbar.update(1)
                        
        except KeyboardInterrupt:
            logging.info("Processing interrupted by user")
            raise
        except Exception as e:
            logging.error(f"Directory processing error: {str(e)}")
            raise

    # [Previous methods remain unchanged: get_database_stats, check_file_status, list_processed_files]

    def get_database_stats(self):
        """Get statistics about the database"""
        try:
            # Get total count of documents
            result = (
                self.client.query
                .aggregate("Document")
                .with_meta_count()
                .do()
            )
            
            total_docs = result['data']['Aggregate']['Document'][0]['meta']['count']
            
            # Get unique file names
            result = (
                self.client.query
                .get("Document")
                .with_additional(["id"])
                .with_group_by(["file_name"])
                .with_limit(10000)
                .do()
            )
            
            unique_files = len(result['data']['Get']['Document'])
            
            return {
                "total_documents": total_docs,
                "unique_files": unique_files
            }
        except Exception as e:
            logging.error(f"Error getting database stats: {str(e)}")
            return None

    def check_file_status(self, filename: str):
        """Check if a file exists in the database"""
        try:
            result = (
                self.client.query
                .get("Document")
                .with_additional(["id"])
                .with_where({
                    "path": ["file_name"],
                    "operator": "Equal",
                    "valueString": filename
                })
                .with_limit(1)
                .do()
            )
            
            return len(result['data']['Get']['Document']) > 0
        except Exception as e:
            logging.error(f"Error checking file status: {str(e)}")
            return False

    def list_processed_files(self, limit=100):
        """List all processed files in the database"""
        try:
            result = (
                self.client.query
                .get("Document")
                .with_additional(["id"])
                .with_group_by(["file_name"])
                .with_limit(limit)
                .do()
            )
            
            return [doc['file_name'] for doc in result['data']['Get']['Document']]
        except Exception as e:
            logging.error(f"Error listing processed files: {str(e)}")
            return []