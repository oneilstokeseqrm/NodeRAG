"""Tests for Task 4.0.4: Embedding Pipeline Storage Operations"""

import pytest
import pandas as pd
import numpy as np
import os
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.src.pipeline.embedding import Embedding_pipeline

class TestEmbeddingPipelineStorage:
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment"""
        os.environ['NODERAG_STORAGE_BACKEND'] = 'cloud'
        self.tenant_id = f"test_404_{os.getpid()}"
        TenantContext.set_current_tenant(self.tenant_id)
        yield
        TenantContext.clear_current_tenant()
    
    def test_namespace_generation(self):
        """Test namespace format is correct"""
        namespace = TenantContext.get_tenant_namespace('embeddings')
        assert namespace == f"{self.tenant_id}_embeddings"
        
        namespace_entities = TenantContext.get_tenant_namespace('entities')
        assert namespace_entities == f"{self.tenant_id}_entities"
        
        namespace_semantic = TenantContext.get_tenant_namespace('semantic_units')
        assert namespace_semantic == f"{self.tenant_id}_semantic_units"
    
    def test_no_local_files_created_cloud_mode(self):
        """Verify no parquet files are created in cloud mode"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = MagicMock()
            config.embedding_cache = f"{temp_dir}/embedding_cache.jsonl"
            config.embedding = f"{temp_dir}/embeddings.parquet"
            config.console = MagicMock()
            config.account_id = 'test_account'
            config.interaction_id = 'test_interaction'
            config.user_id = 'test_user'
            config.interaction_type = 'test_interaction_type'
            config.source_system = 'test_source_system'
            
            cache_data = [
                {'hash_id': 'test_1', 'embedding': np.random.rand(3072).tolist()},
                {'hash_id': 'test_2', 'embedding': np.random.rand(3072).tolist()}
            ]
            
            with open(config.embedding_cache, 'w') as f:
                for item in cache_data:
                    f.write(json.dumps(item) + '\n')
            
            with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
                 patch.object(StorageFactory, 'get_embedding_storage') as mock_pinecone, \
                 patch.object(Embedding_pipeline, 'load_mapper') as mock_load_mapper:
                
                mock_adapter = MagicMock()
                mock_adapter.index = MagicMock()
                mock_adapter.index.upsert = MagicMock(return_value={'upserted_count': 2})
                mock_pinecone.return_value = mock_adapter
                
                mock_mapper = MagicMock()
                mock_load_mapper.return_value = mock_mapper
                
                pipeline = Embedding_pipeline(config)
                pipeline.mapper = MagicMock()
                pipeline.mapper.add_attribute = MagicMock()
                pipeline.mapper.update_save = MagicMock()
                
                pipeline.insert_embeddings()
                
                assert not Path(config.embedding).exists()
                
                assert mock_adapter.index.upsert.called
    
    def test_fallback_to_file_storage(self):
        """Test fallback to file storage when not in cloud mode"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = MagicMock()
            config.embedding_cache = f"{temp_dir}/embedding_cache.jsonl"
            config.embedding = f"{temp_dir}/embeddings.parquet"
            config.console = MagicMock()
            
            cache_data = [
                {'hash_id': 'test_1', 'embedding': np.random.rand(3072).tolist()}
            ]
            
            with open(config.embedding_cache, 'w') as f:
                for item in cache_data:
                    f.write(json.dumps(item) + '\n')
            
            with patch.object(StorageFactory, 'is_cloud_storage', return_value=False), \
                 patch('NodeRAG.src.pipeline.storage_adapter.storage_factory_wrapper') as mock_wrapper, \
                 patch.object(Embedding_pipeline, 'load_mapper') as mock_load_mapper:
                
                mock_storage = MagicMock()
                mock_wrapper.return_value = mock_storage
                
                mock_mapper = MagicMock()
                mock_load_mapper.return_value = mock_mapper
                
                pipeline = Embedding_pipeline(config)
                pipeline.mapper = MagicMock()
                pipeline.mapper.add_attribute = MagicMock()
                pipeline.mapper.update_save = MagicMock()
                
                pipeline.insert_embeddings()
                
                assert mock_wrapper.called
                assert mock_storage.save_parquet.called
    
    def test_metadata_fields(self):
        """Test that metadata structure is correct"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = MagicMock()
            config.embedding_cache = f"{temp_dir}/embedding_cache.jsonl"
            config.embedding = f"{temp_dir}/embeddings.parquet"
            config.console = MagicMock()
            config.account_id = 'test_account'
            config.interaction_id = 'test_interaction'
            config.user_id = 'test_user'
            config.interaction_type = 'test_interaction_type'
            config.source_system = 'test_source_system'
            
            cache_data = [
                {'hash_id': 'test_1', 'embedding': np.random.rand(3072).tolist()}
            ]
            
            with open(config.embedding_cache, 'w') as f:
                for item in cache_data:
                    f.write(json.dumps(item) + '\n')
            
            with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
                 patch.object(StorageFactory, 'get_embedding_storage') as mock_pinecone, \
                 patch.object(Embedding_pipeline, 'load_mapper') as mock_load_mapper:
                
                mock_adapter = MagicMock()
                mock_adapter.index = MagicMock()
                mock_adapter.index.upsert = MagicMock(return_value={'upserted_count': 1})
                mock_pinecone.return_value = mock_adapter
                
                mock_mapper = MagicMock()
                mock_load_mapper.return_value = mock_mapper
                
                pipeline = Embedding_pipeline(config)
                pipeline.mapper = MagicMock()
                pipeline.mapper.add_attribute = MagicMock()
                pipeline.mapper.update_save = MagicMock()
                
                pipeline.insert_embeddings()
                
                assert mock_adapter.index.upsert.called
                call_args = mock_adapter.index.upsert.call_args
                vectors = call_args[1]['vectors']
                
                vector_data = vectors[0]
                assert vector_data['id'].startswith(f"{self.tenant_id}_embedding_")
                assert len(vector_data['values']) == 3072
                metadata = vector_data['metadata']
                assert metadata['tenant_id'] == self.tenant_id
                assert metadata['account_id'] == 'test_account'
                assert metadata['interaction_id'] == 'test_interaction'
                assert metadata['user_id'] == 'test_user'
                assert metadata['source_system'] == 'test_source_system'
                assert metadata['interaction_type'] == 'test_interaction_type'
    
    def test_batch_size_limit(self):
        """Test that batches respect 100 vector limit"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = MagicMock()
            config.embedding_cache = f"{temp_dir}/embedding_cache.jsonl"
            config.embedding = f"{temp_dir}/embeddings.parquet"
            config.console = MagicMock()
            config.account_id = 'test_account'
            config.interaction_id = 'test_interaction'
            config.user_id = 'test_user'
            config.interaction_type = 'test_interaction_type'
            config.source_system = 'test_source_system'
            
            cache_data = []
            for i in range(250):
                cache_data.append({
                    'hash_id': f'test_{i}', 
                    'embedding': np.random.rand(3072).tolist()
                })
            
            with open(config.embedding_cache, 'w') as f:
                for item in cache_data:
                    f.write(json.dumps(item) + '\n')
            
            with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
                 patch.object(StorageFactory, 'get_embedding_storage') as mock_pinecone, \
                 patch.object(Embedding_pipeline, 'load_mapper') as mock_load_mapper:
                
                mock_adapter = MagicMock()
                mock_adapter.index = MagicMock()
                mock_adapter.index.upsert = MagicMock(return_value={'upserted_count': 100})
                mock_pinecone.return_value = mock_adapter
                
                mock_mapper = MagicMock()
                mock_load_mapper.return_value = mock_mapper
                
                pipeline = Embedding_pipeline(config)
                pipeline.mapper = MagicMock()
                pipeline.mapper.add_attribute = MagicMock()
                pipeline.mapper.update_save = MagicMock()
                
                pipeline.insert_embeddings()
                
                assert mock_adapter.index.upsert.call_count == 3
                
                call_args_list = mock_adapter.index.upsert.call_args_list
                assert len(call_args_list[0][1]['vectors']) == 100  # First batch
                assert len(call_args_list[1][1]['vectors']) == 100  # Second batch
                assert len(call_args_list[2][1]['vectors']) == 50   # Third batch

    def test_no_asyncio_run(self):
        """Verify that asyncio.run is NOT used (causes production crashes)"""
        import inspect
        from NodeRAG.src.pipeline.embedding import Embedding_pipeline
        
        source = inspect.getsource(Embedding_pipeline._store_embeddings_in_pinecone)
        
        assert 'asyncio.run' not in source, "asyncio.run causes event loop conflicts - use synchronous operations"
        
        assert 'index.upsert' in source, "Should use synchronous Pinecone operations"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
