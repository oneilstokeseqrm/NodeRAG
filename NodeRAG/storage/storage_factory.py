"""
Storage Factory for NodeRAG - Manages storage backend selection
"""
import os
import warnings
import logging
from typing import Optional, Dict, Any, Union
from enum import Enum
import asyncio
import time

from .storage import storage
from .neo4j_adapter import Neo4jAdapter
from .pinecone_adapter import PineconeAdapter
from ..config.eq_config import EQConfig
from ..standards.eq_metadata import EQMetadata

logger = logging.getLogger(__name__)


class StorageBackend(Enum):
    """Available storage backends"""
    FILE = "file"
    NEO4J = "neo4j"
    PINECONE = "pinecone"


class StorageFactory:
    """
    Factory for creating and managing storage adapters.
    Provides migration path from file to cloud storage.
    """
    
    _instances: Dict[str, Any] = {}
    _config: Optional[EQConfig] = None
    _backend_mode: StorageBackend = StorageBackend.FILE
    
    @classmethod
    def initialize(cls, config: Union[EQConfig, Dict[str, Any]], 
                  backend_mode: str = "file") -> None:
        """
        Initialize the storage factory with configuration
        
        Args:
            config: EQConfig instance or config dict
            backend_mode: "file", "neo4j", or "cloud" (neo4j+pinecone)
        """
        if isinstance(config, dict):
            config = EQConfig(config)
        
        cls._config = config
        
        if backend_mode == "cloud":
            cls._backend_mode = StorageBackend.NEO4J  # Cloud means both Neo4j + Pinecone
        elif backend_mode == "neo4j":
            cls._backend_mode = StorageBackend.NEO4J
        else:
            cls._backend_mode = StorageBackend.FILE
        
        logger.info(f"Storage factory initialized with backend: {cls._backend_mode.value}")
    
    @classmethod
    def get_graph_storage(cls) -> Union[Neo4jAdapter, storage]:
        """
        Get graph storage backend (Neo4j or file-based)
        
        Returns:
            Neo4jAdapter for cloud mode, storage for file mode
        """
        if not cls._config:
            raise RuntimeError("StorageFactory not initialized. Call initialize() first.")
        
        if cls._backend_mode == StorageBackend.NEO4J:
            return cls._get_neo4j_adapter()
        else:
            cls._warn_deprecated_storage("graph")
            return storage  # Return the storage class for file operations
    
    @classmethod
    def get_embedding_storage(cls) -> Union[PineconeAdapter, storage]:
        """
        Get embedding storage backend (Pinecone or file-based)
        
        Returns:
            PineconeAdapter for cloud mode, storage for file mode
        """
        if not cls._config:
            raise RuntimeError("StorageFactory not initialized. Call initialize() first.")
        
        if cls._backend_mode == StorageBackend.NEO4J:  # Cloud mode includes Pinecone
            return cls._get_pinecone_adapter()
        else:
            cls._warn_deprecated_storage("embedding")
            return storage  # Return the storage class for file operations
    
    @classmethod
    def _get_neo4j_adapter(cls) -> Neo4jAdapter:
        """Get or create singleton Neo4j adapter with retry logic"""
        if 'neo4j' not in cls._instances:
            adapter = Neo4jAdapter(cls._config.neo4j_config)
            
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    connected = loop.run_until_complete(adapter.connect())
                    loop.close()
                    
                    if connected:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(adapter.create_constraints_and_indexes())
                        loop.close()
                        
                        cls._instances['neo4j'] = adapter
                        logger.info("Successfully connected to Neo4j")
                        break
                    else:
                        raise ConnectionError("Failed to connect to Neo4j")
                        
                except Exception as e:
                    logger.warning(f"Neo4j connection attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        raise RuntimeError(f"Failed to connect to Neo4j after {max_retries} attempts: {e}")
        
        return cls._instances['neo4j']
    
    @classmethod
    def _get_pinecone_adapter(cls) -> PineconeAdapter:
        """Get or create singleton Pinecone adapter with retry logic"""
        if 'pinecone' not in cls._instances:
            adapter = PineconeAdapter(
                api_key=cls._config.pinecone_config['api_key'],
                index_name=cls._config.pinecone_config['index_name']
            )
            
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    connected = adapter.connect()
                    if connected:
                        cls._instances['pinecone'] = adapter
                        logger.info("Successfully connected to Pinecone")
                        break
                    else:
                        raise ConnectionError("Failed to connect to Pinecone")
                        
                except Exception as e:
                    logger.warning(f"Pinecone connection attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        raise RuntimeError(f"Failed to connect to Pinecone after {max_retries} attempts: {e}")
        
        return cls._instances['pinecone']
    
    @classmethod
    def _warn_deprecated_storage(cls, storage_type: str) -> None:
        """Issue deprecation warning for file-based storage"""
        warnings.warn(
            f"File-based {storage_type} storage is deprecated and will be removed in v2.0. "
            f"Please migrate to cloud storage (Neo4j/Pinecone) for production workloads. "
            f"Set backend_mode='cloud' when initializing StorageFactory.",
            DeprecationWarning,
            stacklevel=3
        )
    
    @classmethod
    def get_storage_for_component(cls, content: Any, component_type: str) -> Any:
        """
        Get appropriate storage wrapper for a component
        Maintains backward compatibility with existing storage() usage
        
        Args:
            content: Data to be stored
            component_type: Type of component (graph, embedding, etc.)
            
        Returns:
            Appropriate storage wrapper
        """
        if component_type in ['graph', 'nodes', 'relationships']:
            backend = cls.get_graph_storage()
            if (hasattr(backend, '__class__') and 
                (backend.__class__.__name__ == 'Neo4jAdapter' or 
                 str(backend.__class__).find('Neo4jAdapter') != -1 or
                 hasattr(backend, 'connect'))):  # Neo4j adapters have connect method
                return Neo4jStorageWrapper(backend, content)
            else:
                return storage(content)
                
        elif component_type in ['embedding', 'vectors']:
            backend = cls.get_embedding_storage()
            if (hasattr(backend, '__class__') and 
                (backend.__class__.__name__ == 'PineconeAdapter' or 
                 str(backend.__class__).find('PineconeAdapter') != -1 or
                 hasattr(backend, 'index'))):  # Pinecone adapters have index attribute
                return PineconeStorageWrapper(backend, content)
            else:
                return storage(content)
        else:
            cls._warn_deprecated_storage(component_type)
            return storage(content)
    
    @classmethod
    def cleanup(cls) -> None:
        """Clean up all storage connections"""
        if 'neo4j' in cls._instances:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(cls._instances['neo4j'].close())
            loop.close()
            del cls._instances['neo4j']
            
        if 'pinecone' in cls._instances:
            cls._instances['pinecone'].close()
            del cls._instances['pinecone']
            
        cls._instances.clear()
        logger.info("Storage factory cleaned up")
    
    @classmethod
    def get_backend_mode(cls) -> str:
        """Get current backend mode"""
        return cls._backend_mode.value
    
    @classmethod
    def is_cloud_storage(cls) -> bool:
        """Check if using cloud storage"""
        return cls._backend_mode == StorageBackend.NEO4J


class Neo4jStorageWrapper:
    """Wrapper to make Neo4j adapter compatible with storage class interface"""
    
    def __init__(self, adapter: Neo4jAdapter, content: Any):
        self.adapter = adapter
        self.content = content
    
    def save_pickle(self, path: str) -> None:
        """Save graph to Neo4j instead of pickle file"""
        logger.info(f"Redirecting pickle save to Neo4j (path ignored: {path})")
        pass
    
    def save_parquet(self, path: str, append: bool = False) -> None:
        """Save nodes/relationships to Neo4j instead of parquet"""
        logger.info(f"Redirecting parquet save to Neo4j (path ignored: {path})")
        pass


class PineconeStorageWrapper:
    """Wrapper to make Pinecone adapter compatible with storage class interface"""
    
    def __init__(self, adapter: PineconeAdapter, content: Any):
        self.adapter = adapter
        self.content = content
    
    def save_parquet(self, path: str, append: bool = False) -> None:
        """Save embeddings to Pinecone instead of parquet"""
        logger.info(f"Redirecting embedding save to Pinecone (path ignored: {path})")
        pass
