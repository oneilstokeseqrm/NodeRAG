"""
Test actual data persistence in cloud storage with round-trip verification
"""
import pytest
import networkx as nx
import pandas as pd
import numpy as np
import uuid
import os
import time
import sys
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter


class TestCloudStorageRoundTrip:
    """Test actual cloud storage persistence"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup for each test"""
        self.test_tenant = f"test_{uuid.uuid4()}"
        self.test_namespace = f"test_{uuid.uuid4()}"
        
        config = {
            'config': {'main_folder': '/tmp/test', 'language': 'en', 'chunk_size': 512},
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
        
        neo4j_uri = os.getenv('Neo4j_Credentials_NEO4J_URI')
        neo4j_password = os.getenv('Neo4j_Credentials_NEO4J_PASSWORD')
        pinecone_key = os.getenv('pinecone_API_key')
        
        if not all([neo4j_uri, neo4j_password, pinecone_key]):
            pytest.skip("Cloud credentials not available")
        
        StorageFactory.initialize(config, backend_mode="cloud")
        self.adapter = PipelineStorageAdapter()
        
        yield
        
        try:
            neo4j = StorageFactory.get_graph_storage()
            neo4j.clear_tenant_data(self.test_tenant)
            
            pinecone = StorageFactory.get_embedding_storage()
            import asyncio
            asyncio.run(pinecone.delete_namespace(self.test_namespace))
        except Exception as e:
            print(f"Cleanup failed: {e}")
    
    def test_neo4j_graph_round_trip(self):
        """Test that graphs can be saved and loaded from Neo4j"""
        original_graph = nx.Graph()
        original_graph.add_node("node1", type="entity", weight=1.0, data="test1")
        original_graph.add_node("node2", type="semantic_unit", weight=2.0, data="test2")
        original_graph.add_node("node3", type="entity", weight=1.5, data="test3")
        original_graph.add_edge("node1", "node2", type="relates", weight=0.8)
        original_graph.add_edge("node2", "node3", type="contains", weight=0.5)
        
        save_success = self.adapter.save_pickle(
            original_graph, 
            "/tmp/test_graph.pkl",
            component_type="graph",
            tenant_id=self.test_tenant
        )
        assert save_success, "Failed to save graph to Neo4j"
        
        loaded_graph = self.adapter.load_pickle(
            "/tmp/test_graph.pkl",
            component_type="graph",
            tenant_id=self.test_tenant
        )
        
        assert loaded_graph is not None, "Failed to load graph from Neo4j"
        assert len(loaded_graph.nodes()) == 3, f"Expected 3 nodes, got {len(loaded_graph.nodes())}"
        assert len(loaded_graph.edges()) == 2, f"Expected 2 edges, got {len(loaded_graph.edges())}"
        
        assert "node1" in loaded_graph.nodes()
        assert loaded_graph.nodes["node1"].get("type") == "entity"
        assert loaded_graph.nodes["node1"].get("weight") == 1.0
        assert loaded_graph.nodes["node1"].get("data") == "test1"
        
        assert loaded_graph.has_edge("node1", "node2")
        edge_data = loaded_graph.edges["node1", "node2"]
        assert edge_data.get("type") == "relates"
        assert edge_data.get("weight") == 0.8
    
    def test_pinecone_embeddings_round_trip(self):
        """Test that embeddings can be saved and loaded from Pinecone"""
        original_df = pd.DataFrame({
            'id': [f'vec_{i}' for i in range(5)],
            'embedding': [np.random.randn(3072).tolist() for _ in range(5)],
            'type': ['entity', 'relationship', 'entity', 'semantic_unit', 'entity'],
            'context': [f'Test context {i}' for i in range(5)],
            'weight': [1.0, 0.8, 1.2, 0.9, 1.1]
        })
        
        save_success = self.adapter.save_parquet(
            original_df,
            "/tmp/test_embeddings.parquet",
            component_type="embeddings",
            namespace=self.test_namespace
        )
        assert save_success, "Failed to save embeddings to Pinecone"
        
        time.sleep(2)
        
        loaded_df = self.adapter.load_parquet(
            "/tmp/test_embeddings.parquet",
            component_type="embeddings",
            namespace=self.test_namespace
        )
        
        assert loaded_df is not None, "Failed to load embeddings from Pinecone"
        assert len(loaded_df) > 0, "No embeddings retrieved from Pinecone"
        
        loaded_ids = set(loaded_df['id'].tolist())
        original_ids = set(original_df['id'].tolist())
        
        common_ids = loaded_ids.intersection(original_ids)
        assert len(common_ids) > 0, f"No matching IDs found. Original: {original_ids}, Loaded: {loaded_ids}"
        
        for embedding in loaded_df['embedding']:
            assert len(embedding) == 3072, f"Expected 3072 dimensions, got {len(embedding)}"
    
    def test_empty_data_handling(self):
        """Test handling of empty graphs and DataFrames"""
        empty_graph = nx.Graph()
        save_success = self.adapter.save_pickle(
            empty_graph,
            "/tmp/empty_graph.pkl",
            component_type="graph",
            tenant_id=self.test_tenant
        )
        assert save_success  # Should succeed even with empty graph
        
        loaded_graph = self.adapter.load_pickle(
            "/tmp/empty_graph.pkl",
            component_type="graph",
            tenant_id=self.test_tenant
        )
        assert loaded_graph is not None
        assert len(loaded_graph.nodes()) == 0
        
        empty_df = pd.DataFrame()
        save_success = self.adapter.save_parquet(
            empty_df,
            "/tmp/empty_embeddings.parquet",
            component_type="embeddings",
            namespace=self.test_namespace
        )
        assert save_success  # Should succeed with empty DataFrame
    
    def test_tenant_isolation(self):
        """Test that tenant isolation works correctly"""
        tenant1 = f"tenant1_{uuid.uuid4()}"
        tenant2 = f"tenant2_{uuid.uuid4()}"
        
        graph1 = nx.Graph()
        graph1.add_node("tenant1_node", type="entity", data="tenant1_data")
        
        graph2 = nx.Graph()
        graph2.add_node("tenant2_node", type="entity", data="tenant2_data")
        
        success1 = self.adapter.save_pickle(graph1, "/tmp/graph1.pkl", "graph", tenant1)
        success2 = self.adapter.save_pickle(graph2, "/tmp/graph2.pkl", "graph", tenant2)
        
        assert success1 and success2, "Failed to save tenant graphs"
        
        loaded1 = self.adapter.load_pickle("/tmp/graph1.pkl", "graph", tenant1)
        loaded2 = self.adapter.load_pickle("/tmp/graph2.pkl", "graph", tenant2)
        
        assert loaded1 is not None and loaded2 is not None
        assert "tenant1_node" in loaded1.nodes()
        assert "tenant2_node" in loaded2.nodes()
        assert "tenant1_node" not in loaded2.nodes()
        assert "tenant2_node" not in loaded1.nodes()
        
        try:
            neo4j = StorageFactory.get_graph_storage()
            neo4j.clear_tenant_data(tenant1)
            neo4j.clear_tenant_data(tenant2)
        except:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
