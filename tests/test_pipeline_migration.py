"""
Test suite for Graph_pipeline storage migration
"""
import pytest
import tempfile
import os
from pathlib import Path
import pandas as pd
import networkx as nx
import uuid
from datetime import datetime, timezone

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.src.pipeline.graph_pipeline_v2 import Graph_pipeline
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
from NodeRAG.config.Node_config import NodeConfig

class TestPipelineMigration:
    """Test Graph_pipeline with StorageFactory integration"""
    
    @pytest.fixture
    def setup_environment(self):
        """Setup test environment with proper credentials"""
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
                        'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag-test')
                    }
                }
            }
            
            yield config, tmpdir
    
    def test_file_storage_compatibility(self, setup_environment):
        """Test backward compatibility with file storage"""
        config, tmpdir = setup_environment
        
        StorageFactory.initialize(config, backend_mode="file")
        
        adapter = PipelineStorageAdapter()
        assert adapter.backend_mode == 'file'
        
        test_graph = nx.Graph()
        test_graph.add_node("A", weight=1)
        test_graph.add_node("B", weight=2)
        test_graph.add_edge("A", "B", weight=0.5)
        
        graph_path = f"{tmpdir}/test_graph.pkl"
        assert adapter.save_pickle(test_graph, graph_path, component_type='graph')
        loaded_graph = adapter.load_pickle(graph_path, component_type='graph')
        
        assert loaded_graph is not None
        assert len(loaded_graph.nodes()) == 2
        assert len(loaded_graph.edges()) == 1
    
    def test_cloud_storage_mode(self, setup_environment):
        """Test cloud storage mode with real credentials"""
        config, tmpdir = setup_environment
        
        if not all([
            os.getenv('Neo4j_Credentials_NEO4J_URI'),
            os.getenv('Neo4j_Credentials_NEO4J_PASSWORD'),
            os.getenv('pinecone_API_key')
        ]):
            pytest.skip("Cloud storage credentials not available")
        
        StorageFactory.initialize(config, backend_mode="cloud")
        
        adapter = PipelineStorageAdapter()
        assert adapter.backend_mode == 'cloud'
        
        test_graph = nx.Graph()
        test_graph.add_node("test_node_1", type="entity", weight=1)
        test_graph.add_node("test_node_2", type="semantic_unit", weight=1)
        test_graph.add_edge("test_node_1", "test_node_2", weight=0.8)
        
        graph_path = f"{tmpdir}/test_graph.pkl"
        success = adapter.save_pickle(test_graph, graph_path, component_type='graph')
        assert success
    
    def test_parquet_operations(self, setup_environment):
        """Test parquet save/load operations"""
        config, tmpdir = setup_environment
        
        StorageFactory.initialize(config, backend_mode="file")
        adapter = PipelineStorageAdapter()
        
        test_data = pd.DataFrame({
            'hash_id': [str(uuid.uuid4()) for _ in range(3)],
            'type': ['entity', 'relationship', 'semantic_unit'],
            'context': ['Test entity', 'Test relationship', 'Test semantic unit'],
            'weight': [1, 2, 1]
        })
        
        parquet_path = f"{tmpdir}/test_data.parquet"
        assert adapter.save_parquet(test_data, parquet_path, component_type='data')
        
        loaded_data = adapter.load_parquet(parquet_path, component_type='data')
        assert loaded_data is not None
        assert len(loaded_data) == 3
        assert list(loaded_data.columns) == ['hash_id', 'type', 'context', 'weight']
    
    def test_graph_pipeline_v2_initialization(self, setup_environment):
        """Test Graph_pipeline_v2 initialization"""
        config, tmpdir = setup_environment
        
        StorageFactory.initialize(config, backend_mode="file")
        node_config = NodeConfig(config)
        
        pipeline = Graph_pipeline(node_config)
        assert hasattr(pipeline, 'storage_adapter')
        assert isinstance(pipeline.storage_adapter, PipelineStorageAdapter)
    
    def test_mixed_storage_operations(self, setup_environment):
        """Test mixed storage operations with different component types"""
        config, tmpdir = setup_environment
        
        StorageFactory.initialize(config, backend_mode="file")
        adapter = PipelineStorageAdapter()
        
        graph = nx.Graph()
        graph.add_node("entity_1", type="entity", weight=2)
        
        entities_data = pd.DataFrame({
            'hash_id': ['entity_1'],
            'type': ['entity'],
            'context': ['Test entity context'],
            'weight': [2]
        })
        
        embeddings_data = pd.DataFrame({
            'hash_id': ['entity_1'],
            'embedding': [[0.1, 0.2, 0.3]]
        })
        
        graph_path = f"{tmpdir}/graph.pkl"
        entities_path = f"{tmpdir}/entities.parquet"
        embeddings_path = f"{tmpdir}/embeddings.parquet"
        
        assert adapter.save_pickle(graph, graph_path, component_type='graph')
        assert adapter.save_parquet(entities_data, entities_path, component_type='data')
        assert adapter.save_parquet(embeddings_data, embeddings_path, component_type='embeddings')
        
        loaded_graph = adapter.load_pickle(graph_path, component_type='graph')
        loaded_entities = adapter.load_parquet(entities_path, component_type='data')
        loaded_embeddings = adapter.load_parquet(embeddings_path, component_type='embeddings')
        
        assert loaded_graph is not None
        assert loaded_entities is not None
        assert loaded_embeddings is not None
        assert len(loaded_graph.nodes()) == 1
        assert len(loaded_entities) == 1
        assert len(loaded_embeddings) == 1
    
    def test_append_functionality(self, setup_environment):
        """Test append functionality for parquet files"""
        config, tmpdir = setup_environment
        
        StorageFactory.initialize(config, backend_mode="file")
        adapter = PipelineStorageAdapter()
        
        initial_data = pd.DataFrame({
            'hash_id': ['id_1', 'id_2'],
            'type': ['entity', 'relationship'],
            'context': ['Entity 1', 'Relationship 1']
        })
        
        additional_data = pd.DataFrame({
            'hash_id': ['id_3'],
            'type': ['semantic_unit'],
            'context': ['Semantic unit 1']
        })
        
        parquet_path = f"{tmpdir}/test_append.parquet"
        
        assert adapter.save_parquet(initial_data, parquet_path, component_type='data')
        assert adapter.save_parquet(additional_data, parquet_path, component_type='data', append=True)
        
        final_data = adapter.load_parquet(parquet_path, component_type='data')
        assert len(final_data) == 3
        assert set(final_data['hash_id']) == {'id_1', 'id_2', 'id_3'}

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
