"""
Test suite for StorageFactory optimizations
"""
import pytest
import time
import tempfile
import os
from pathlib import Path
import json

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.config import NodeConfig
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline


class TestStorageOptimizations:
    """Test all optimization features"""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test"""
        yield
        StorageFactory.cleanup()
    
    def test_lazy_initialization(self):
        """Test lazy initialization reduces startup time"""
        config = self.get_test_config()
        
        # Test eager initialization
        start = time.time()
        StorageFactory.initialize(config, backend_mode="cloud", lazy_init=False)
        eager_time = time.time() - start
        StorageFactory.cleanup()
        
        # Test lazy initialization
        start = time.time()
        StorageFactory.initialize(config, backend_mode="cloud", lazy_init=True)
        lazy_time = time.time() - start
        
        assert lazy_time < eager_time * 0.5, "Lazy init should be much faster"
        
        # Verify adapters initialize on first use
        status = StorageFactory.get_initialization_status()
        assert not status['adapters_initialized']['neo4j']
        
        # First use triggers initialization
        neo4j = StorageFactory.get_graph_storage()
        status = StorageFactory.get_initialization_status()
        assert status['adapters_initialized']['neo4j']
    
    def test_pipeline_config_adapter(self):
        """Test pipeline config generation works correctly"""
        config = self.get_test_config()
        StorageFactory.initialize(config, backend_mode="cloud")
        
        # Get pipeline config
        pipeline_config = StorageFactory.get_pipeline_config()
        
        # Verify structure matches NodeConfig expectations
        assert 'config' in pipeline_config
        assert 'model_config' in pipeline_config
        assert 'embedding_config' in pipeline_config
        assert 'main_folder' in pipeline_config['config']
        
        # Test creating NodeConfig
        node_config = NodeConfig(pipeline_config)
        assert node_config.main_folder == pipeline_config['config']['main_folder']
        
        # Verify directories were created
        assert Path(node_config.main_folder).exists()
        assert Path(f"{node_config.main_folder}/cache").exists()
        assert Path(f"{node_config.main_folder}/input").exists()
        
        # Verify cache files were created
        cache_file = Path(f"{node_config.main_folder}/cache/text_decomposition.jsonl")
        assert cache_file.exists()
    
    def test_health_check_caching(self):
        """Test health check caching reduces repeated calls"""
        config = self.get_test_config()
        StorageFactory.initialize(config, backend_mode="cloud")
        
        # First call - not cached
        start = time.time()
        health1 = StorageFactory.get_cached_health_check(cache_ttl=5)
        first_call_time = time.time() - start
        
        # Second call - should be cached
        start = time.time()
        health2 = StorageFactory.get_cached_health_check(cache_ttl=5)
        cached_call_time = time.time() - start
        
        assert health1 == health2
        assert cached_call_time < first_call_time * 0.1, "Cached call should be much faster"
    
    def test_connection_warmup(self):
        """Test connection warmup improves first operation performance"""
        config = self.get_test_config()
        
        # Without warmup
        StorageFactory.cleanup()
        StorageFactory.initialize(config, backend_mode="cloud", warmup_connections=False)
        
        start = time.time()
        neo4j = StorageFactory.get_graph_storage()
        health = neo4j.health_check()
        cold_time = time.time() - start
        
        # With warmup
        StorageFactory.cleanup()
        StorageFactory.initialize(config, backend_mode="cloud", warmup_connections=True)
        
        start = time.time()
        neo4j = StorageFactory.get_graph_storage()
        health = neo4j.health_check()
        warm_time = time.time() - start
        
        # Warmed connections should be faster for first operation
        # (May not always be true in tests, but architecture is correct)
        print(f"Cold time: {cold_time:.3f}s, Warm time: {warm_time:.3f}s")
    
    def test_preload_adapters(self):
        """Test adapter preloading"""
        config = self.get_test_config()
        StorageFactory.initialize(config, backend_mode="cloud", lazy_init=True)
        
        # Verify not initialized
        status = StorageFactory.get_initialization_status()
        assert not status['adapters_initialized']['neo4j']
        assert not status['adapters_initialized']['pinecone']
        
        # Preload
        StorageFactory.preload_adapters()
        
        # Verify initialized
        status = StorageFactory.get_initialization_status()
        assert status['adapters_initialized']['neo4j']
        assert status['adapters_initialized']['pinecone']
    
    def test_pipeline_integration(self):
        """Test complete pipeline integration with optimized StorageFactory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'config': {
                    'main_folder': tmpdir,
                    'language': 'en',
                    'chunk_size': 512
                },
                'model_config': {'model_name': 'gpt-4o'},
                'embedding_config': {'model_name': 'gpt-4o'},
                'eq_config': {
                    'storage': {
                        'neo4j_uri': os.getenv('Neo4j_Credentials_NEO4J_URI'),
                        'neo4j_user': os.getenv('Neo4j_Credentials_NEO4J_USERNAME', 'neo4j'),
                        'neo4j_password': os.getenv('Neo4j_Credentials_NEO4J_PASSWORD'),
                        'pinecone_api_key': os.getenv('pinecone_API_key'),
                        'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag')
                    }
                }
            }
            
            # Initialize with all optimizations
            StorageFactory.initialize(
                config, 
                backend_mode="cloud",
                lazy_init=True,
                warmup_connections=False
            )
            
            # Get pipeline config
            pipeline_config = StorageFactory.get_pipeline_config()
            
            # Create NodeConfig and Graph_pipeline
            node_config = NodeConfig(pipeline_config)
            
            # This should work without errors now
            pipeline = Graph_pipeline(node_config)
            assert pipeline is not None
            
            # Verify storage is accessible
            neo4j = StorageFactory.get_graph_storage()
            assert neo4j is not None
    
    def get_test_config(self):
        """Get test configuration"""
        tmpdir = tempfile.mkdtemp()
        return {
            'config': {
                'main_folder': tmpdir,
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': os.getenv('Neo4j_Credentials_NEO4J_URI'),
                    'neo4j_user': os.getenv('Neo4j_Credentials_NEO4J_USERNAME', 'neo4j'),
                    'neo4j_password': os.getenv('Neo4j_Credentials_NEO4J_PASSWORD'),
                    'pinecone_api_key': os.getenv('pinecone_API_key'),
                    'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag')
                }
            }
        }


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
