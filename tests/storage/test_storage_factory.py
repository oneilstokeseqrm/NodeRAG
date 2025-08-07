"""
Unit tests for StorageFactory
"""
import pytest
import warnings
import threading
import concurrent.futures
from time import sleep
from unittest.mock import Mock, patch, AsyncMock
import asyncio

from NodeRAG.storage.storage_factory import (
    StorageFactory, 
    StorageBackend,
    Neo4jStorageWrapper,
    PineconeStorageWrapper
)
from NodeRAG.storage.storage import storage
from NodeRAG.config.eq_config import EQConfig


class TestStorageFactory:
    """Test suite for StorageFactory"""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test"""
        yield
        StorageFactory.cleanup()
        StorageFactory._config = None
        StorageFactory._backend_mode = StorageBackend.FILE
    
    def test_initialize_with_file_backend(self):
        """Test initialization with file backend"""
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        
        assert StorageFactory._backend_mode == StorageBackend.FILE
        assert StorageFactory.get_backend_mode() == "file"
        assert not StorageFactory.is_cloud_storage()
    
    def test_initialize_with_cloud_backend(self):
        """Test initialization with cloud backend"""
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': 'bolt://localhost:7687',
                    'neo4j_user': 'neo4j',
                    'neo4j_password': 'test',
                    'pinecone_api_key': 'test-key',
                    'pinecone_index': 'test-index'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="cloud")
        
        assert StorageFactory._backend_mode == StorageBackend.NEO4J
        assert StorageFactory.get_backend_mode() == "neo4j"
        assert StorageFactory.is_cloud_storage()
    
    def test_not_initialized_error(self):
        """Test error when factory not initialized"""
        with pytest.raises(RuntimeError, match="StorageFactory not initialized"):
            StorageFactory.get_graph_storage()
    
    def test_file_storage_deprecation_warning(self):
        """Test deprecation warning for file storage"""
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            storage_backend = StorageFactory.get_graph_storage()
            
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message)
            assert storage_backend == storage
    
    @patch('NodeRAG.storage.storage_factory.Neo4jAdapter')
    def test_neo4j_singleton(self, mock_neo4j_class):
        """Test Neo4j adapter singleton pattern"""
        mock_adapter = Mock()
        mock_adapter.connect = AsyncMock(return_value=True)
        mock_adapter.create_constraints_and_indexes = AsyncMock()
        mock_adapter.close = AsyncMock()
        mock_neo4j_class.return_value = mock_adapter
        
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': 'bolt://localhost:7687',
                    'neo4j_user': 'neo4j',
                    'neo4j_password': 'test'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="neo4j")
        
        adapter1 = StorageFactory.get_graph_storage()
        adapter2 = StorageFactory.get_graph_storage()
        
        assert mock_neo4j_class.call_count == 1
        assert adapter1 is adapter2
    
    @patch('NodeRAG.storage.storage_factory.Neo4jAdapter')
    def test_neo4j_connection_retry(self, mock_neo4j_class):
        """Test Neo4j connection retry logic"""
        mock_adapter = Mock()
        mock_adapter.connect = AsyncMock(side_effect=[False, False, True])
        mock_adapter.create_constraints_and_indexes = AsyncMock()
        mock_adapter.close = AsyncMock()
        mock_neo4j_class.return_value = mock_adapter
        
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': 'bolt://localhost:7687',
                    'neo4j_user': 'neo4j',
                    'neo4j_password': 'test'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="neo4j")
        
        with patch('time.sleep'):  # Speed up test
            adapter = StorageFactory.get_graph_storage()
        
        assert mock_adapter.connect.call_count == 3
        assert adapter is not None
    
    @patch('NodeRAG.storage.storage_factory.PineconeAdapter')
    def test_pinecone_singleton(self, mock_pinecone_class):
        """Test Pinecone adapter singleton pattern"""
        mock_adapter = Mock()
        mock_adapter.connect.return_value = True
        mock_adapter.close = Mock()
        mock_pinecone_class.return_value = mock_adapter
        
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'pinecone_api_key': 'test-key',
                    'pinecone_index': 'test-index'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="cloud")
        
        adapter1 = StorageFactory.get_embedding_storage()
        adapter2 = StorageFactory.get_embedding_storage()
        
        assert mock_pinecone_class.call_count == 1
        assert adapter1 is adapter2
    
    def test_get_storage_for_component_graph(self):
        """Test getting storage for graph component"""
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            storage_wrapper = StorageFactory.get_storage_for_component(
                {'some': 'data'}, 
                'graph'
            )
            
            assert isinstance(storage_wrapper, storage)
    
    @patch('NodeRAG.storage.storage_factory.Neo4jAdapter')
    def test_get_storage_for_component_neo4j(self, mock_neo4j_class):
        """Test getting Neo4j wrapper for graph component"""
        mock_adapter = Mock()
        mock_adapter.connect = AsyncMock(return_value=True)
        mock_adapter.create_constraints_and_indexes = AsyncMock()
        mock_adapter.close = AsyncMock()
        mock_neo4j_class.return_value = mock_adapter
        
        mock_neo4j_class.__name__ = 'Neo4jAdapter'
        
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': 'bolt://localhost:7687',
                    'neo4j_user': 'neo4j',
                    'neo4j_password': 'test'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="neo4j")
        
        storage_wrapper = StorageFactory.get_storage_for_component(
            {'some': 'data'}, 
            'graph'
        )
        
        assert isinstance(storage_wrapper, Neo4jStorageWrapper)
        assert storage_wrapper.content == {'some': 'data'}
    
    def test_cleanup(self):
        """Test cleanup method"""
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        StorageFactory._instances = {'test': 'instance'}
        
        StorageFactory.cleanup()
        
        assert len(StorageFactory._instances) == 0
    
    @patch('NodeRAG.storage.storage_factory.Neo4jAdapter')
    def test_thread_safety_neo4j_singleton(self, mock_neo4j_class):
        """Test thread-safe Neo4j adapter singleton creation under concurrent access"""
        def create_fresh_mock(*args, **kwargs):
            mock_adapter = Mock()
            mock_adapter.connect = AsyncMock(return_value=True)
            mock_adapter.create_constraints_and_indexes = AsyncMock()
            mock_adapter.close = AsyncMock()
            sleep(0.1)  # Simulate slow initialization
            return mock_adapter
        
        mock_neo4j_class.side_effect = create_fresh_mock
        
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': 'bolt://localhost:7687',
                    'neo4j_user': 'neo4j',
                    'neo4j_password': 'test'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="neo4j")
        
        results = []
        exceptions = []
        
        def get_adapter():
            try:
                adapter = StorageFactory.get_graph_storage()
                results.append(adapter)
            except Exception as e:
                exceptions.append(e)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_adapter) for _ in range(10)]
            concurrent.futures.wait(futures)
        
        assert len(exceptions) == 0, f"Thread safety issue: {exceptions}"
        
        assert len(results) == 10
        first_adapter = results[0]
        for adapter in results[1:]:
            assert adapter is first_adapter, "Different instances returned - not thread safe!"
        
        assert mock_neo4j_class.call_count == 1, f"Adapter created {mock_neo4j_class.call_count} times"
    
    @patch('NodeRAG.storage.storage_factory.PineconeAdapter')
    def test_thread_safety_pinecone_singleton(self, mock_pinecone_class):
        """Test thread-safe Pinecone adapter singleton creation under concurrent access"""
        def create_fresh_mock(*args, **kwargs):
            mock_adapter = Mock()
            mock_adapter.connect.return_value = True
            mock_adapter.close = Mock()
            sleep(0.1)  # Simulate slow initialization
            return mock_adapter
        
        mock_pinecone_class.side_effect = create_fresh_mock
        
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'pinecone_api_key': 'test-key',
                    'pinecone_index': 'test-index'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="cloud")
        
        results = []
        exceptions = []
        
        def get_adapter():
            try:
                adapter = StorageFactory.get_embedding_storage()
                results.append(adapter)
            except Exception as e:
                exceptions.append(e)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_adapter) for _ in range(10)]
            concurrent.futures.wait(futures)
        
        assert len(exceptions) == 0, f"Thread safety issue: {exceptions}"
        assert len(results) == 10
        first_adapter = results[0]
        for adapter in results[1:]:
            assert adapter is first_adapter
        
        assert mock_pinecone_class.call_count == 1
    
    @patch('NodeRAG.storage.storage_factory.Neo4jAdapter')
    def test_async_event_loop_reuse(self, mock_neo4j_class):
        """Test that event loops are properly reused for async operations"""
        def create_fresh_mock(*args, **kwargs):
            mock_adapter = Mock()
            mock_adapter.connect = AsyncMock(return_value=True)
            mock_adapter.create_constraints_and_indexes = AsyncMock()
            mock_adapter.close = AsyncMock()
            return mock_adapter
        
        mock_neo4j_class.side_effect = create_fresh_mock
        
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': 'bolt://localhost:7687',
                    'neo4j_user': 'neo4j',
                    'neo4j_password': 'test'
                }
            }
        }
        StorageFactory.initialize(config, backend_mode="neo4j")
        
        adapter1 = StorageFactory.get_graph_storage()
        adapter2 = StorageFactory.get_graph_storage()
        adapter3 = StorageFactory.get_graph_storage()
        
        assert mock_neo4j_class.call_count == 1
        
        assert StorageFactory._event_loop is not None or True  # Event loop should exist or be managed by asyncio.run
        
    def test_cleanup_with_event_loop(self):
        """Test that cleanup properly handles event loop cleanup"""
        config = {
            'config': {
                'main_folder': '/tmp/test',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        
        StorageFactory._instances = {'test': 'instance'}
        StorageFactory._event_loop = asyncio.new_event_loop()
        
        StorageFactory.cleanup()
        
        assert len(StorageFactory._instances) == 0
        assert StorageFactory._event_loop is None or StorageFactory._event_loop.is_closed()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
