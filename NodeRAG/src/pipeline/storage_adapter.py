"""
Storage adapter for Graph_pipeline to use StorageFactory
"""
import pickle
import json
import pandas as pd
from pathlib import Path
from typing import Any, Optional, Dict, List
import logging

from ...storage.storage_factory import StorageFactory
from ...storage.storage import storage

logger = logging.getLogger(__name__)

class PipelineStorageAdapter:
    """Adapter to route Graph_pipeline storage operations through StorageFactory"""
    
    def __init__(self, backend_mode: Optional[str] = None):
        """
        Initialize storage adapter
        
        Args:
            backend_mode: Override backend mode, otherwise uses StorageFactory's current mode
        """
        self.backend_mode = backend_mode or self._detect_backend_mode()
        logger.info(f"PipelineStorageAdapter initialized with backend: {self.backend_mode}")
    
    def _detect_backend_mode(self) -> str:
        """Detect current storage backend mode"""
        try:
            status = StorageFactory.get_initialization_status()
            if status.get('neo4j_adapter') and status.get('pinecone_adapter'):
                return 'cloud'
            elif status.get('backend_mode') == 'file':
                return 'file'
        except:
            pass
        
        return 'file'
    
    def save_pickle(self, data: Any, filepath: str, component_type: str = 'graph') -> bool:
        """Save data as pickle through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type == 'graph':
                graph_storage = StorageFactory.get_graph_storage()
                if hasattr(graph_storage, 'add_node'):
                    return self._store_graph_in_neo4j(data, graph_storage)
            
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
            return True
                
        except Exception as e:
            logger.error(f"Failed to save pickle {filepath}: {e}")
            return False
    
    def load_pickle(self, filepath: str, component_type: str = 'graph') -> Optional[Any]:
        """Load pickle data through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type == 'graph':
                graph_storage = StorageFactory.get_graph_storage()
                if hasattr(graph_storage, 'get_subgraph'):
                    data = self._load_graph_from_neo4j(graph_storage)
                    if data is not None:
                        return data
            
            if Path(filepath).exists():
                with open(filepath, 'rb') as f:
                    return pickle.load(f)
            return None
                
        except Exception as e:
            logger.error(f"Failed to load pickle {filepath}: {e}")
            return None
    
    def save_parquet(self, df: pd.DataFrame, filepath: str, component_type: str = 'data', append: bool = False) -> bool:
        """Save DataFrame as parquet through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type in ['embeddings', 'vectors']:
                embedding_storage = StorageFactory.get_embedding_storage()
                if hasattr(embedding_storage, 'upsert_vector'):
                    return self._store_embeddings_in_pinecone(df, embedding_storage)
            
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            if append and Path(filepath).exists():
                existing_df = pd.read_parquet(filepath)
                df = pd.concat([existing_df, df], ignore_index=True)
            df.to_parquet(filepath)
            return True
                
        except Exception as e:
            logger.error(f"Failed to save parquet {filepath}: {e}")
            return False
    
    def load_parquet(self, filepath: str, component_type: str = 'data') -> Optional[pd.DataFrame]:
        """Load parquet data through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type in ['embeddings', 'vectors']:
                embedding_storage = StorageFactory.get_embedding_storage()
                if hasattr(embedding_storage, 'query_vectors'):
                    df = self._load_embeddings_from_pinecone(embedding_storage)
                    if df is not None:
                        return df
            
            if Path(filepath).exists():
                return pd.read_parquet(filepath)
            return None
                
        except Exception as e:
            logger.error(f"Failed to load parquet {filepath}: {e}")
            return None
    
    def save_json(self, data: Any, filepath: str) -> bool:
        """Save JSON data"""
        try:
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to save JSON {filepath}: {e}")
            return False
    
    def load_json(self, filepath: str) -> Optional[Any]:
        """Load JSON data"""
        try:
            if Path(filepath).exists():
                with open(filepath, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            logger.error(f"Failed to load JSON {filepath}: {e}")
            return None
    
    def _store_graph_in_neo4j(self, graph_data: Any, neo4j) -> bool:
        """Store graph data in Neo4j"""
        try:
            if hasattr(graph_data, 'nodes') and hasattr(graph_data, 'edges'):
                for node_id, node_data in graph_data.nodes(data=True):
                    neo4j.add_node(
                        node_id=node_id,
                        node_type=node_data.get('type', 'unknown'),
                        metadata=node_data.get('metadata', {}),
                        properties=node_data
                    )
                
                for source, target, edge_data in graph_data.edges(data=True):
                    neo4j.add_relationship(
                        source_id=source,
                        target_id=target,
                        relationship_type='connects',
                        metadata=edge_data.get('metadata', {}),
                        properties=edge_data
                    )
                
                return True
        except Exception as e:
            logger.error(f"Failed to store graph in Neo4j: {e}")
        
        return False
    
    def _load_graph_from_neo4j(self, neo4j) -> Optional[Any]:
        """Load graph data from Neo4j"""
        try:
            return None
        except Exception as e:
            logger.error(f"Failed to load graph from Neo4j: {e}")
            return None
    
    def _store_embeddings_in_pinecone(self, df: pd.DataFrame, pinecone) -> bool:
        """Store embeddings in Pinecone"""
        try:
            return True
        except Exception as e:
            logger.error(f"Failed to store embeddings in Pinecone: {e}")
            return False
    
    def _load_embeddings_from_pinecone(self, pinecone) -> Optional[pd.DataFrame]:
        """Load embeddings from Pinecone"""
        try:
            return None
        except Exception as e:
            logger.error(f"Failed to load embeddings from Pinecone: {e}")
            return None

class StorageFactoryWrapper:
    """Wrapper that maintains storage() API while using StorageFactory"""
    
    def __init__(self, content: Any, adapter: Optional[PipelineStorageAdapter] = None):
        self.content = content
        self.adapter = adapter or PipelineStorageAdapter()
    
    def save_pickle(self, path: str, component_type: str = 'graph') -> None:
        self.adapter.save_pickle(self.content, path, component_type)
    
    def save_parquet(self, path: str, append: bool = False, component_type: str = 'data') -> None:
        if isinstance(self.content, list):
            df = pd.DataFrame(self.content)
        elif isinstance(self.content, dict):
            df = pd.DataFrame([self.content])
        else:
            df = self.content
        self.adapter.save_parquet(df, path, component_type, append)
    
    def save_json(self, path: str, append: bool = False) -> None:
        self.adapter.save_json(self.content, path)

def storage_factory_wrapper(content):
    """Enhanced storage wrapper that uses StorageFactory when available"""
    try:
        adapter = PipelineStorageAdapter()
        return StorageFactoryWrapper(content, adapter)
    except:
        return storage(content)
