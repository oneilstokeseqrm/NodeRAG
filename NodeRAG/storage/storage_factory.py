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
import json
from pathlib import Path
from datetime import datetime, timedelta

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
    
    _lazy_init: bool = False
    _adapters_initialized: Dict[str, bool] = {}
    _cache: Dict[str, Any] = {}
    _cache_ttl: Dict[str, datetime] = {}
    _warmup_complete: bool = False
    
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
                  backend_mode: str = "file",
                  lazy_init: bool = False,
                  warmup_connections: bool = False) -> None:
        """
        Initialize the storage factory with configuration
        
        Args:
            config: EQConfig instance or config dict
            backend_mode: "file", "neo4j", or "cloud" (neo4j+pinecone)
            lazy_init: If True, defer adapter initialization until first use
            warmup_connections: If True, pre-warm connection pools after init
        """
        if isinstance(config, dict):
            config = EQConfig(config)
        
        cls._config = config
        cls._lazy_init = lazy_init
        
        if backend_mode == "cloud":
            cls._backend_mode = StorageBackend.NEO4J  # Cloud means both Neo4j + Pinecone
        elif backend_mode == "neo4j":
            cls._backend_mode = StorageBackend.NEO4J
        else:
            cls._backend_mode = StorageBackend.FILE
        
        cls._adapters_initialized = {'neo4j': False, 'pinecone': False}
        cls._warmup_complete = False
        
        cls._ensure_directories(config)
        
        logger.info(f"Storage factory initialized with backend: {cls._backend_mode.value}, lazy_init: {lazy_init}")
        
        if warmup_connections and not lazy_init:
            cls._warmup_connections()
    
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
        """Get or create singleton Neo4j adapter with lazy initialization support"""
        if 'neo4j' in cls._instances:
            return cls._instances['neo4j']
        
        with cls._lock:  # Thread-safe singleton creation
            if 'neo4j' in cls._instances:
                return cls._instances['neo4j']
            
            if cls._lazy_init and not cls._adapters_initialized.get('neo4j', False):
                logger.info("Lazy initializing Neo4j adapter...")
            
            adapter = Neo4jAdapter(cls._config.neo4j_config)
            
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    connected = adapter.connect()  # CHANGED: Synchronous call (was cls._run_async(adapter.connect()))
                    
                    if connected:
                        adapter.create_constraints_and_indexes()  # CHANGED: Synchronous call (was cls._run_async(...))
                        
                        cls._instances['neo4j'] = adapter
                        cls._adapters_initialized['neo4j'] = True
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
        """Get or create singleton Pinecone adapter with lazy initialization support"""
        if 'pinecone' in cls._instances:
            return cls._instances['pinecone']
        
        with cls._lock:  # Thread-safe singleton creation
            if 'pinecone' in cls._instances:
                return cls._instances['pinecone']
            
            if cls._lazy_init and not cls._adapters_initialized.get('pinecone', False):
                logger.info("Lazy initializing Pinecone adapter...")
            
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
                        cls._adapters_initialized['pinecone'] = True
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
    def _ensure_directories(cls, config: Union[EQConfig, Dict[str, Any]]) -> None:
        """Ensure all required directories and files exist"""
        main_folder = None
        if hasattr(config, 'config'):
            main_folder = config.config.get('main_folder')
        elif isinstance(config, dict) and 'config' in config:
            main_folder = config['config'].get('main_folder')
        
        if main_folder:
            directories = [
                main_folder,
                f"{main_folder}/cache",
                f"{main_folder}/input",
                f"{main_folder}/output",
                f"{main_folder}/logs"
            ]
            
            for directory in directories:
                Path(directory).mkdir(parents=True, exist_ok=True)
                logger.debug(f"Ensured directory exists: {directory}")
            
            cache_files = [
                f"{main_folder}/cache/text_decomposition.jsonl",
                f"{main_folder}/cache/entities.json",
                f"{main_folder}/cache/relationships.json"
            ]
            
            for cache_file in cache_files:
                cache_path = Path(cache_file)
                if not cache_path.exists():
                    if cache_file.endswith('.jsonl'):
                        cache_path.write_text('')
                    elif cache_file.endswith('.json'):
                        cache_path.write_text('{}')
                    logger.debug(f"Created empty cache file: {cache_file}")
    
    @classmethod
    def _warmup_connections(cls) -> None:
        """Pre-warm connection pools for better performance"""
        if cls._warmup_complete:
            return
        
        logger.info("Warming up connection pools...")
        start_time = time.time()
        
        if cls._backend_mode == StorageBackend.NEO4J:
            try:
                neo4j = cls.get_graph_storage()
                for _ in range(3):
                    neo4j.health_check()
                logger.info("Neo4j connection pool warmed up")
            except Exception as e:
                logger.warning(f"Failed to warm up Neo4j: {e}")
            
            try:
                pinecone = cls.get_embedding_storage()
                if hasattr(pinecone, 'index'):
                    pinecone.index.describe_index_stats()
                logger.info("Pinecone connection warmed up")
            except Exception as e:
                logger.warning(f"Failed to warm up Pinecone: {e}")
        
        elapsed = time.time() - start_time
        logger.info(f"Connection warmup completed in {elapsed:.2f}s")
        cls._warmup_complete = True
    
    @classmethod
    def get_pipeline_config(cls) -> Dict[str, Any]:
        """
        Get configuration in format expected by Graph_pipeline/NodeConfig
        
        Returns:
            Dict with proper structure for NodeConfig initialization
        """
        if not cls._config:
            raise RuntimeError("StorageFactory not initialized. Call initialize() first.")
        
        pipeline_config = {}
        
        if hasattr(cls._config, 'config'):
            pipeline_config['config'] = cls._config.config
        elif hasattr(cls._config, '__dict__'):
            pipeline_config['config'] = {
                'main_folder': getattr(cls._config, 'main_folder', '/tmp/noderag'),
                'language': getattr(cls._config, 'language', 'en'),
                'chunk_size': getattr(cls._config, 'chunk_size', 512),
                'chunk_overlap': getattr(cls._config, 'chunk_overlap', 50),
                'batch_size': getattr(cls._config, 'batch_size', 100)
            }
        
        if hasattr(cls._config, 'model_config'):
            pipeline_config['model_config'] = cls._config.model_config
        elif hasattr(cls._config, '_model_config'):
            pipeline_config['model_config'] = cls._config._model_config
        else:
            pipeline_config['model_config'] = {'model_name': 'gpt-4o'}
        
        if hasattr(cls._config, 'embedding_config'):
            pipeline_config['embedding_config'] = cls._config.embedding_config
        elif hasattr(cls._config, '_embedding_config'):
            pipeline_config['embedding_config'] = cls._config._embedding_config
        else:
            pipeline_config['embedding_config'] = {'model_name': 'gpt-4o'}
        
        if hasattr(cls._config, 'eq_config'):
            pipeline_config['eq_config'] = cls._config.eq_config
        elif hasattr(cls._config, '_eq_config'):
            pipeline_config['eq_config'] = cls._config._eq_config
        
        main_folder = pipeline_config['config'].get('main_folder')
        if main_folder:
            cls._ensure_directories({'config': pipeline_config['config']})
        
        logger.debug(f"Generated pipeline config: {json.dumps(pipeline_config, indent=2)}")
        
        return pipeline_config
    
    @classmethod
    def get_cached_health_check(cls, cache_ttl: int = 30) -> Dict[str, Any]:
        """
        Get cached health check result to avoid repeated calls
        
        Args:
            cache_ttl: Cache time-to-live in seconds
            
        Returns:
            Cached or fresh health check result
        """
        cache_key = 'neo4j_health_check'
        now = datetime.now()
        
        if (cache_key in cls._cache and 
            cache_key in cls._cache_ttl and
            (now - cls._cache_ttl[cache_key]).total_seconds() < cache_ttl):
            logger.debug("Returning cached health check")
            return cls._cache[cache_key]
        
        if cls._backend_mode == StorageBackend.NEO4J:
            neo4j = cls.get_graph_storage()
            health = neo4j.health_check()
            
            cls._cache[cache_key] = health
            cls._cache_ttl[cache_key] = now
            
            return health
        else:
            return {'status': 'not_applicable', 'backend': 'file'}
    
    @classmethod
    def preload_adapters(cls) -> None:
        """
        Explicitly initialize all adapters (useful for pre-warming)
        Call this during application startup to avoid initialization delay on first request
        """
        if cls._backend_mode == StorageBackend.NEO4J:
            logger.info("Preloading adapters...")
            start_time = time.time()
            
            # Initialize Neo4j
            neo4j = cls.get_graph_storage()
            neo4j_time = time.time() - start_time
            logger.info(f"Neo4j adapter loaded in {neo4j_time:.2f}s")
            
            # Initialize Pinecone
            pinecone_start = time.time()
            pinecone = cls.get_embedding_storage()
            pinecone_time = time.time() - pinecone_start
            logger.info(f"Pinecone adapter loaded in {pinecone_time:.2f}s")
            
            total_time = time.time() - start_time
            logger.info(f"All adapters preloaded in {total_time:.2f}s")
    
    @classmethod
    def get_initialization_status(cls) -> Dict[str, Any]:
        """
        Get current initialization status of adapters
        
        Returns:
            Dict with initialization status for each adapter
        """
        return {
            'backend_mode': cls._backend_mode.value if cls._backend_mode else None,
            'lazy_init_enabled': cls._lazy_init,
            'adapters_initialized': cls._adapters_initialized.copy(),
            'warmup_complete': cls._warmup_complete,
            'cached_items': list(cls._cache.keys()),
            'instances': list(cls._instances.keys())
        }

    @classmethod
    def cleanup(cls) -> None:
        """Clean up all storage connections and executor thread"""
        with cls._lock:
            if 'neo4j' in cls._instances:
                try:
                    adapter = cls._instances['neo4j']
                    if hasattr(adapter, 'close') and callable(adapter.close):
                        adapter.close()  # CHANGED: Synchronous call (was cls._run_async(adapter.close()))
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
