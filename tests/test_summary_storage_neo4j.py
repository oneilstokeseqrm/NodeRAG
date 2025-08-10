"""Tests for Task 4.0.6: Summary Generation Storage Operations"""

import pytest
import networkx as nx
import os
import tempfile
from unittest.mock import patch, MagicMock, call
from pathlib import Path

from NodeRAG.src.pipeline.summary_generation import SummaryGeneration
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.standards.eq_metadata import EQMetadata

class TestSummaryStorageNeo4j:
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment"""
        self.tenant_id = "test_tenant_406"
        TenantContext.set_current_tenant(self.tenant_id)
        
        self.config = MagicMock()
        self.config.graph_path = "/tmp/test_graph.pkl"
        self.config.summary_path = "/tmp/test_summary.jsonl"
        self.config.high_level_elements_path = "/tmp/test_he.parquet"
        self.config.high_level_elements_titles_path = "/tmp/test_he_titles.parquet"
        self.config.embedding = "/tmp/test_embedding.parquet"
        self.config.indices_path = "/tmp/test_indices.json"
        self.config.console = MagicMock()
        
        yield
        TenantContext.clear_current_tenant()
    
    def test_graph_loading_from_neo4j(self):
        """Test that graph is loaded from Neo4j in cloud mode"""
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
             patch.object(StorageFactory, 'get_graph_storage') as mock_neo4j:
            
            mock_adapter = MagicMock()
            mock_neo4j.return_value = mock_adapter
            
            mock_adapter.get_subgraph.return_value = {
                'nodes': [
                    {'node_id': 'node1', 'type': 'entity', 'weight': 1},
                    {'node_id': 'node2', 'type': 'semantic_unit', 'weight': 2}
                ],
                'relationships': [
                    {'source_id': 'node1', 'target_id': 'node2', 'weight': 1}
                ]
            }
            
            pipeline = SummaryGeneration(self.config)
            
            assert isinstance(pipeline.G, nx.Graph)
            assert pipeline.G.has_node('node1')
            assert pipeline.G.has_node('node2')
            assert pipeline.G.has_edge('node1', 'node2')
            
            mock_adapter.get_subgraph.assert_called_with(self.tenant_id)
    
    def test_graph_storage_to_neo4j(self):
        """Test that graph is stored to Neo4j with proper metadata"""
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
             patch.object(StorageFactory, 'get_graph_storage') as mock_neo4j:
            
            mock_adapter = MagicMock()
            mock_adapter.add_node.return_value = True
            mock_adapter.add_relationship.return_value = True
            mock_neo4j.return_value = mock_adapter
            
            test_graph = nx.Graph()
            test_graph.add_node('node1', type='entity', weight=1, 
                              tenant_id='test_tenant', account_id='acc_123')
            test_graph.add_node('node2', type='semantic_unit', weight=2,
                              tenant_id='test_tenant', account_id='acc_123')
            test_graph.add_edge('node1', 'node2', weight=1)
            
            pipeline = SummaryGeneration(self.config)
            pipeline.G = test_graph
            
            pipeline.store_graph()
            
            assert mock_adapter.add_node.call_count == 2
            
            assert mock_adapter.add_relationship.call_count == 1
    
    def test_aggregated_metadata_for_cross_tenant(self):
        """Test AGGREGATED metadata is used for cross-tenant summaries"""
        test_graph = nx.Graph()
        test_graph.add_node('node1', type='entity', tenant_id='tenant_a',
                          account_id='acc_a', interaction_id='int_a',
                          interaction_type='chat', timestamp='2025-01-01T00:00:00Z',
                          user_id='user_a', source_system='slack')
        test_graph.add_node('node2', type='entity', tenant_id='tenant_b',
                          account_id='acc_b', interaction_id='int_b',
                          interaction_type='email', timestamp='2025-01-01T00:00:00Z',
                          user_id='user_b', source_system='gmail')
        
        pipeline = SummaryGeneration(self.config)
        pipeline.G = test_graph
        
        metadata = pipeline._extract_metadata_from_community(['node1', 'node2'])
        
        assert metadata.tenant_id == 'AGGREGATED'
        assert metadata.account_id == 'AGGREGATED'
        assert metadata.interaction_id == 'AGGREGATED'
        assert metadata.interaction_type == 'summary'
        assert metadata.source_system == 'internal'
    
    def test_single_tenant_metadata_preserved(self):
        """Test that single-tenant metadata is preserved"""
        test_graph = nx.Graph()
        test_graph.add_node('node1', type='entity', tenant_id='tenant_a',
                          account_id='acc_123', interaction_id='int_456',
                          interaction_type='chat', timestamp='2025-01-01T00:00:00Z',
                          user_id='user_123', source_system='slack')
        test_graph.add_node('node2', type='entity', tenant_id='tenant_a',
                          account_id='acc_123', interaction_id='int_789',
                          interaction_type='chat', timestamp='2025-01-01T00:00:00Z',
                          user_id='user_456', source_system='slack')
        
        pipeline = SummaryGeneration(self.config)
        pipeline.G = test_graph
        
        metadata = pipeline._extract_metadata_from_community(['node1', 'node2'])
        
        assert metadata.tenant_id == 'tenant_a'
        assert metadata.account_id == 'acc_123'
        assert metadata.source_system == 'slack'
    
    def test_high_level_elements_storage(self):
        """Test high-level elements are stored to Neo4j"""
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
             patch.object(StorageFactory, 'get_graph_storage') as mock_neo4j:
            
            mock_adapter = MagicMock()
            mock_adapter.add_node.return_value = True
            mock_neo4j.return_value = mock_adapter
            
            test_graph = nx.Graph()
            test_graph.add_node('he_1', type='high_level_element',
                              tenant_id='AGGREGATED', account_id='AGGREGATED')
            test_graph.add_node('he_1_title', type='high_level_element_title',
                              tenant_id='AGGREGATED', account_id='AGGREGATED')
            
            from NodeRAG.src.component import High_level_elements
            he = MagicMock(spec=High_level_elements)
            he.hash_id = 'he_1'
            he.title_hash_id = 'he_1_title'
            he.context = 'Test high level element'
            he.title = 'Test Title'
            he.human_readable_id = 'HLE_001'
            he.embedding = [0.1] * 3072
            
            pipeline = SummaryGeneration(self.config)
            pipeline.G = test_graph
            pipeline.high_level_elements = [he]
            
            pipeline.store_high_level_elements()
            
            calls = mock_adapter.add_node.call_args_list
            assert len(calls) >= 1
            
            for call in calls:
                metadata = call[1]['metadata']
                if hasattr(metadata, 'tenant_id'):
                    assert metadata.tenant_id == 'AGGREGATED'
    
    def test_fallback_to_file_storage(self):
        """Test fallback to file storage when not in cloud mode"""
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=False), \
             patch('NodeRAG.src.pipeline.summary_generation.storage') as mock_storage, \
             patch('os.path.exists', return_value=True), \
             patch('NodeRAG.storage.graph_mapping.Mapper') as mock_mapper:
            
            mock_storage.load_pickle.return_value = nx.Graph()
            mock_mapper_instance = MagicMock()
            mock_mapper.return_value = mock_mapper_instance
            
            pipeline = SummaryGeneration(self.config)
            
            mock_storage.load_pickle.assert_called_with(self.config.graph_path)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
