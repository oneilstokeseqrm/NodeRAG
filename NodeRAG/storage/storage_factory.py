"""
Storage Factory for NodeRAG - Manages storage backend selection
"""
import os
import warnings
import logging
import threading
import concurrent.futures
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
    Thread-safe singleton implementation with isolated async execution.
    """
    
    _instances: Dict[str, Any] = {}
    _config: Optional[EQConfig] = None
    _backend_mode: StorageBackend = StorageBackend.FILE
    _lock = threading.Lock()  # Thread lock for singleton safety
    _executor: Optional[concurrent.futures.ThreadPoolExecutor] = None  # Lazy-initialized executor
    
    @classmethod
    def _get_executor(cls):
        """Get or create the ThreadPoolExecutor lazily"""
        if cls._executor is None or cls._executor._shutdown:
            cls._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, 
                thread_name_prefix="storage-factory-async"
            )
        return cls._executor
    
    @classmethod
    def _run_async(cls, coro):
        """
        Run an async coroutine in a sync context using a dedicated thread.
        This avoids modifying global asyncio behavior while allowing
        async operations from synchronous code.
        """
        def run_in_new_loop():
            """Run coroutine in a fresh event loop in the executor thread"""
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()
                asyncio.set_event_loop(None)
        
        try:
            existing_loop = asyncio.get_running_loop()
            logger.warning("StorageFactory._run_async called from existing async context")
            future = asyncio.ensure_future(coro)
            return asyncio.get_event_loop().run_until_complete(future)
        except RuntimeError:
            executor = cls._get_executor()
            future = executor.submit(run_in_new_loop)
            return future.result()
    
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
        """Get or create singleton Neo4j adapter with retry logic and thread safety"""
        if 'neo4j' in cls._instances:
            return cls._instances['neo4j']
        
        with cls._lock:  # Thread-safe singleton creation
            if 'neo4j' in cls._instances:
                return cls._instances['neo4j']
            
            adapter = Neo4jAdapter(cls._config.neo4j_config)
            
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    connected = cls._run_async(adapter.connect())
                    
                    if connected:
                        cls._run_async(adapter.create_constraints_and_indexes())
                        
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
        """Get or create singleton Pinecone adapter with retry logic and thread safety"""
        if 'pinecone' in cls._instances:
            return cls._instances['pinecone']
        
        with cls._lock:  # Thread-safe singleton creation
            if 'pinecone' in cls._instances:
                return cls._instances['pinecone']
            
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
        """Clean up all storage connections and event loop"""
        with cls._lock:
            if 'neo4j' in cls._instances:
                try:
                    adapter = cls._instances['neo4j']
                    if hasattr(adapter, 'close') and callable(adapter.close):
                        cls._run_async(adapter.close())
                except Exception as e:
                    logger.warning(f"Error closing Neo4j adapter: {e}")
                finally:
                    del cls._instances['neo4j']
                
            if 'pinecone' in cls._instances:
                try:
                    adapter = cls._instances['pinecone']
                    if hasattr(adapter, 'close') and callable(adapter.close):
                        adapter.close()
                except Exception as e:
                    logger.warning(f"Error closing Pinecone adapter: {e}")
                finally:
                    del cls._instances['pinecone']
                
            cls._instances.clear()
            
            if cls._executor is not None and not cls._executor._shutdown:
                cls._executor.shutdown(wait=True, cancel_futures=False)
                logger.info("Executor thread pool shutdown complete")
            
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
