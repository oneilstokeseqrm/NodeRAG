"""
Pinecone Storage Adapter for NodeRAG with EQ Metadata Support
"""
import os
import logging
from typing import Dict, List, Any, Optional, Tuple

from pinecone import Pinecone, ServerlessSpec

from ..standards.eq_metadata import EQMetadata

logger = logging.getLogger(__name__)


class PineconeAdapter:
    """Pinecone adapter for NodeRAG vector storage with multi-tenant support"""
    
    def __init__(self, api_key: Optional[str] = None, index_name: str = "noderag"):
        self.api_key = api_key or os.getenv("PINECONE_API_KEY")
        self.index_name = index_name
        self.pc = None
        self.index = None
        self.dimension = 3072
        
    def connect(self) -> bool:
        """Initialize Pinecone connection"""
        try:
            self.pc = Pinecone(api_key=self.api_key)
            
            existing_indexes = [idx.name for idx in self.pc.list_indexes()]
            
            if self.index_name not in existing_indexes:
                logger.info(f"Creating new Pinecone index: {self.index_name} with dimension {self.dimension}")
                self.pc.create_index(
                    name=self.index_name,
                    dimension=self.dimension,
                    metric="cosine",
                    spec=ServerlessSpec(
                        cloud="aws",
                        region="us-east-1"
                    )
                )
                logger.info(f"Created Pinecone index: {self.index_name}")
            else:
                index_info = self.pc.describe_index(self.index_name)
                existing_dimension = index_info.dimension
                logger.info(f"Found existing index {self.index_name} with dimension {existing_dimension}")
                
                if existing_dimension != self.dimension:
                    logger.warning(f"Dimension mismatch! Index has {existing_dimension}, adapter expects {self.dimension}")
                    logger.info("Deleting and recreating index with correct dimension")
                    self.pc.delete_index(self.index_name)
                    
                    import time
                    time.sleep(10)
                    
                    self.pc.create_index(
                        name=self.index_name,
                        dimension=self.dimension,
                        metric="cosine",
                        spec=ServerlessSpec(
                            cloud="aws",
                            region="us-east-1"
                        )
                    )
                    logger.info(f"Recreated index with dimension {self.dimension}")
            
            self.index = self.pc.Index(self.index_name)
            logger.info(f"Connected to Pinecone index: {self.index_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Pinecone: {e}")
            return False
    
    def prepare_metadata(self, metadata: EQMetadata, additional_fields: Optional[Dict] = None) -> Dict[str, Any]:
        """Prepare metadata for Pinecone (must be flat dict with string/number values)"""
        errors = metadata.validate()
        if errors:
            raise ValueError(f"Invalid metadata: {errors}")
        
        flat_metadata = {
            "tenant_id": metadata.tenant_id,
            "interaction_id": metadata.interaction_id,
            "interaction_type": metadata.interaction_type,
            "account_id": metadata.account_id,
            "timestamp": metadata.timestamp,
            "user_id": metadata.user_id,
            "source_system": metadata.source_system,
        }
        
        if additional_fields:
            flat_metadata.update(additional_fields)
        
        return flat_metadata
    
    async def upsert_vector(self, vector_id: str, embedding: List[float], 
                          metadata: EQMetadata, namespace: Optional[str] = None,
                          additional_metadata: Optional[Dict] = None) -> bool:
        """Upsert a single vector with metadata"""
        try:
            namespace = namespace or metadata.tenant_id
            
            vector_metadata = self.prepare_metadata(metadata, additional_metadata)
            
            logger.info(f"Upserting vector {vector_id} to namespace {namespace}")
            logger.info(f"Vector metadata: {vector_metadata}")
            logger.info(f"Embedding dimension: {len(embedding)}")
            
            response = self.index.upsert(
                vectors=[(vector_id, embedding, vector_metadata)],
                namespace=namespace
            )
            
            logger.info(f"Upsert response: {response}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert vector {vector_id}: {e}")
            return False
    
    async def upsert_vectors_batch(self, vectors: List[Tuple[str, List[float], EQMetadata, Optional[Dict]]],
                                 namespace: Optional[str] = None) -> Tuple[int, List[str]]:
        """Batch upsert vectors for performance"""
        successful_count = 0
        errors = []
        
        namespace_groups = {}
        for vector_id, embedding, metadata, additional in vectors:
            ns = namespace or metadata.tenant_id
            if ns not in namespace_groups:
                namespace_groups[ns] = []
            
            try:
                vector_metadata = self.prepare_metadata(metadata, additional)
                namespace_groups[ns].append((vector_id, embedding, vector_metadata))
            except Exception as e:
                errors.append(f"Vector {vector_id}: {str(e)}")
        
        batch_size = 100
        
        for ns, ns_vectors in namespace_groups.items():
            for i in range(0, len(ns_vectors), batch_size):
                batch = ns_vectors[i:i + batch_size]
                try:
                    self.index.upsert(vectors=batch, namespace=ns)
                    successful_count += len(batch)
                except Exception as e:
                    errors.append(f"Batch in namespace {ns}: {str(e)}")
        
        return successful_count, errors
    
    async def search(self, query_embedding: List[float], filters: Dict[str, Any],
                    top_k: int = 10, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for similar vectors with metadata filtering"""
        try:
            namespace = namespace or filters.get("tenant_id")
            
            pinecone_filter = {}
            for key, value in filters.items():
                if key in ["tenant_id", "account_id", "interaction_id", 
                          "interaction_type", "user_id", "source_system"]:
                    pinecone_filter[key] = {"$eq": value}
            
            results = self.index.query(
                vector=query_embedding,
                filter=pinecone_filter if pinecone_filter else None,
                top_k=top_k,
                include_metadata=True,
                namespace=namespace
            )
            
            formatted_results = []
            for match in results.matches:
                formatted_results.append({
                    "id": match.id,
                    "score": match.score,
                    "metadata": match.metadata
                })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    async def get_vector(self, vector_id: str, namespace: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific vector by ID"""
        try:
            results = self.index.fetch(ids=[vector_id], namespace=namespace)
            
            if vector_id in results.vectors:
                vector = results.vectors[vector_id]
                return {
                    "id": vector_id,
                    "values": vector.values,
                    "metadata": vector.metadata
                }
            return None
            
        except Exception as e:
            logger.error(f"Failed to get vector {vector_id}: {e}")
            return None
    
    async def delete_vectors(self, vector_ids: List[str], namespace: str) -> bool:
        """Delete vectors by IDs"""
        try:
            self.index.delete(ids=vector_ids, namespace=namespace)
            return True
        except Exception as e:
            logger.error(f"Failed to delete vectors: {e}")
            return False
    
    async def delete_namespace(self, namespace: str) -> bool:
        """Delete all vectors in a namespace (for testing/cleanup)"""
        try:
            self.index.delete(delete_all=True, namespace=namespace)
            return True
        except Exception as e:
            logger.error(f"Failed to delete namespace {namespace}: {e}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get index statistics"""
        try:
            stats = self.index.describe_index_stats()
            return {
                "total_vectors": stats.total_vector_count,
                "dimension": stats.dimension,
                "namespaces": stats.namespaces
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {"error": str(e)}
    
    def close(self):
        """Close Pinecone connection (no-op for stateless client)"""
        self.index = None
        self.pc = None
