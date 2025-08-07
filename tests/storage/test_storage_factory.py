"""
Unit tests for StorageFactory
"""
import pytest
import warnings
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
