import singlestoredb as s2
from langchain.embeddings import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from aryn.sycamore import DocParser, DocPrep
from aryn.sycamore.types import Document, ProcessingConfig
import os
import json
from typing import List, Dict, Union
from datetime import datetime

class SingleStoreRAGSystem:
    def __init__(self, host: str, user: str, password: str, database: str):
        """Initialize the SingleStore RAG system with connection details and Aryn components."""
        self.connection_string = f"singlestore://{user}:{password}@{host}/{database}"
        self.embeddings = OpenAIEmbeddings()
        self.doc_parser = DocParser()
        self.doc_prep = DocPrep()
        
        # Configure default processing settings
        self.processing_config = ProcessingConfig(
            chunk_size=1000,
            chunk_overlap=200,
            remove_headers=True,
            clean_whitespace=True,
            extract_tables=True,
            ocr_enabled=True
        )

    def setup_database(self):
        """Set up the necessary tables in SingleStore."""
        conn = s2.connect(self.connection_string)
        cursor = conn.cursor()
        
        # Create vector table with additional fields for Aryn metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS document_vectors (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                content TEXT,
                metadata JSON,
                embedding BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                document_id VARCHAR(255),
                chunk_index INT,
                content_type VARCHAR(50),
                confidence_score FLOAT,
                page_number INT,
                section_type VARCHAR(50)
            );
        """)
        
        # Enhanced source documents table with Aryn-specific fields
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS source_documents (
                id VARCHAR(255) PRIMARY KEY,
                title TEXT,
                source_type VARCHAR(50),
                original_content TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSON,
                document_structure JSON,
                processing_metadata JSON,
                extraction_confidence FLOAT
            );
        """)
        
        conn.commit()
        conn.close()

    async def process_document(self, file_path: str) -> Document:
        """Process a document using Aryn's DocParser and DocPrep."""
        # Parse the document
        parsed_doc = await self.doc_parser.parse(
            file_path=file_path,
            config=self.processing_config
        )
        
        # Prepare the document
        processed_doc = await self.doc_prep.prepare(
            document=parsed_doc,
            config=self.processing_config
        )
        
        return processed_doc

    def chunk_and_embed(self, doc: Document) -> List[Dict]:
        """Create embeddings for document chunks using Aryn's structure."""
        embedded_chunks = []
        
        for section in doc.sections:
            # Use the section's processed content
            embedding = self.embeddings.embed_query(section.content)
            
            embedded_chunks.append({
                "content": section.content,
                "embedding": embedding,
                "chunk_index": section.index,
                "content_type": section.content_type,
                "confidence_score": section.confidence,
                "page_number": section.page_number,
                "section_type": section.section_type,
                "metadata": section.metadata
            })
            
        return embedded_chunks

    async def store_document(self, file_path: str, doc_id: str = None):
        """Process and store a document using Aryn's pipeline."""
        # Generate doc_id if not provided
        if not doc_id:
            doc_id = f"doc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
        # Process the document
        processed_doc = await self.process_document(file_path)
        
        conn = s2.connect(self.connection_string)
        cursor = conn.cursor()
        
        # Store source document with Aryn metadata
        cursor.execute("""
            INSERT INTO source_documents (
                id, title, source_type, original_content, metadata,
                document_structure, processing_metadata, extraction_confidence
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            doc_id,
            processed_doc.title,
            processed_doc.document_type,
            processed_doc.raw_content,
            json.dumps(processed_doc.metadata),
            json.dumps(processed_doc.structure),
            json.dumps(processed_doc.processing_metadata),
            processed_doc.confidence
        ))
        
        # Process and store vectors
        embedded_chunks = self.chunk_and_embed(processed_doc)
        
        for chunk in embedded_chunks:
            cursor.execute("""
                INSERT INTO document_vectors (
                    content, embedding, document_id, chunk_index,
                    content_type, confidence_score, page_number,
                    section_type, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                chunk["content"],
                bytes(chunk["embedding"]),
                doc_id,
                chunk["chunk_index"],
                chunk["content_type"],
                chunk["confidence_score"],
                chunk["page_number"],
                chunk["section_type"],
                json.dumps(chunk["metadata"])
            ))
        
        conn.commit()
        conn.close()
        
        return doc_id

    def similarity_search(
        self, 
        query: str, 
        k: int = 5,
        min_confidence: float = 0.7,
        content_types: List[str] = None
    ) -> List[Dict]:
        """Enhanced similarity search with Aryn metadata filtering."""
        query_embedding = self.embeddings.embed_query(query)
        
        conn = s2.connect(self.connection_string)
        cursor = conn.cursor()
        
        # Build query with filters
        base_query = """
            SELECT 
                dv.content,
                dv.metadata,
                dv.content_type,
                dv.confidence_score,
                dv.page_number,
                dv.section_type,
                DOT_PRODUCT(dv.embedding, %s) as similarity
            FROM document_vectors dv
            WHERE dv.confidence_score >= %s
        """
        
        params = [bytes(query_embedding), min_confidence]
        
        if content_types:
            content_type_clause = " AND dv.content_type IN (" + ",".join(["%s"] * len(content_types)) + ")"
            base_query += content_type_clause
            params.extend(content_types)
            
        base_query += " ORDER BY similarity DESC LIMIT %s"
        params.append(k)
        
        cursor.execute(base_query, tuple(params))
        results = cursor.fetchall()
        conn.close()
        
        return [
            {
                "content": row[0],
                "metadata": row[1],
                "content_type": row[2],
                "confidence_score": row[3],
                "page_number": row[4],
                "section_type": row[5],
                "similarity": row[6]
            }
            for row in results
        ]

# Example usage
async def main():
    # Initialize the system
    rag_system = SingleStoreRAGSystem(
        host="your-host",
        user="your-user",
        password="your-password",
        database="your-database"
    )
    
    # Set up the database tables
    rag_system.setup_database()
    
    # Process and store a document
    doc_id = await rag_system.store_document(
        file_path="path/to/your/document.pdf"
    )
    
    # Perform a similarity search with filters
    results = rag_system.similarity_search(
        query="What are the key findings?",
        k=3,
        min_confidence=0.8,
        content_types=["text", "table"]
    )
    
    for result in results:
        print(f"Similarity: {result['similarity']}")
        print(f"Content Type: {result['content_type']}")
        print(f"Confidence: {result['confidence_score']}")
        print(f"Content: {result['content'][:200]}...")
        print("---")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
