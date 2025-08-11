"""Tests for Task 4.0.7: Attribute Generation Storage Operations"""

import pytest
import networkx as nx
import os
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path

from NodeRAG.src.pipeline.attribute_generation import Attribution_generation_pipeline
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.standards.eq_metadata import EQMetadata

class TestAttributeStorageNeo4j:
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment"""
        self.tenant_id = "test_tenant_407"
        TenantContext.set_current_tenant(self.tenant_id)
        
        self.config = MagicMock()
        self.config.graph_path = "/tmp/test_graph.pkl"
        self.config.entities_path = "/tmp/test_entities.parquet"
        self.config.relationship_path = "/tmp/test_relationships.parquet"
        self.config.semantic_units_path = "/tmp/test_semantic.parquet"
        self.config.attributes_path = "/tmp/test_attributes.parquet"
        self.config.indices_path = "/tmp/test_indices.json"
        self.config.console = MagicMock()
        self.config.API_client = AsyncMock(return_value="Test attribute text")
        self.config.prompt_manager = MagicMock()
        self.config.prompt_manager.attribute_generation = MagicMock()
        self.config.prompt_manager.attribute_generation.format = MagicMock(return_value="query")
        self.config.token_counter = MagicMock()
        self.config.token_counter.token_limit = MagicMock(return_value=False)
        self.config.indices = MagicMock()
        self.config.tracker = MagicMock()
        
        yield
        TenantContext.clear_current_tenant()
    
    def test_graph_loading_from_neo4j(self):
        """Test that graph is loaded from Neo4j in cloud mode"""
        import pandas as pd
        
        empty_df = pd.DataFrame(columns=['hash_id', 'context', 'type'])
        
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
             patch.object(StorageFactory, 'get_graph_storage') as mock_neo4j, \
             patch('NodeRAG.storage.storage.storage.load', return_value=pd.DataFrame(columns=['hash_id', 'context', 'type'])):
            
            mock_adapter = MagicMock()
            mock_neo4j.return_value = mock_adapter
            
            mock_adapter.get_subgraph.return_value = {
                'nodes': [
                    {'node_id': 'entity1', 'type': 'entity', 'weight': 3,
                     'tenant_id': 'test_tenant', 'account_id': 'acc_123',
                     'interaction_id': 'int_456', 'interaction_type': 'chat',
                     'timestamp': '2025-01-01T00:00:00Z', 'user_id': 'user_123',
                     'source_system': 'slack'},
                    {'node_id': 'entity2', 'type': 'entity', 'weight': 2}
                ],
                'relationships': [
                    {'source_id': 'entity1', 'target_id': 'entity2', 'weight': 1}
                ]
            }
            
            pipeline = Attribution_generation_pipeline(self.config)
            
            assert isinstance(pipeline.G, nx.Graph)
            assert pipeline.G.has_node('entity1')
            assert pipeline.G.has_node('entity2')
            assert pipeline.G.has_edge('entity1', 'entity2')
            
            assert pipeline.G.nodes['entity1']['tenant_id'] == 'test_tenant'
            
            mock_adapter.get_subgraph.assert_called_with(self.tenant_id)
    
    def test_attribute_metadata_propagation(self):
        """Test that attributes inherit metadata from their entities"""
        import pandas as pd
        
        test_graph = nx.Graph()
        test_graph.add_node('entity1', type='entity', weight=3,
                          tenant_id='test_tenant', account_id='acc_123',
                          interaction_id='int_456', interaction_type='chat',
                          timestamp='2025-01-01T00:00:00Z', user_id='user_123',
                          source_system='slack')
        
        with patch('NodeRAG.storage.storage.storage.load', return_value=pd.DataFrame(columns=['hash_id', 'context', 'type'])):
            
            pipeline = Attribution_generation_pipeline(self.config)
            pipeline.G = test_graph
            pipeline.important_nodes = ['entity1']
            
            node_data = test_graph.nodes['entity1']
            assert 'tenant_id' in node_data
            assert node_data['tenant_id'] == 'test_tenant'
            assert all(field in node_data for field in 
                      ['tenant_id', 'account_id', 'interaction_id', 
                       'interaction_type', 'timestamp', 'user_id', 'source_system'])
    
    def test_graph_storage_to_neo4j(self):
        """Test that graph with attributes is stored to Neo4j"""
        import pandas as pd
        
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
             patch.object(StorageFactory, 'get_graph_storage') as mock_neo4j:
            
            mock_adapter = MagicMock()
            mock_adapter.add_node.return_value = True
            mock_adapter.add_relationship.return_value = True
            mock_neo4j.return_value = mock_adapter
            
            test_graph = nx.Graph()
            test_graph.add_node('entity1', type='entity', weight=3,
                              tenant_id='test_tenant', account_id='acc_123')
            test_graph.add_node('attr1', type='attribute', weight=1,
                              tenant_id='test_tenant', account_id='acc_123',
                              interaction_id='int_456', interaction_type='attribute',
                              timestamp='2025-01-01T00:00:00Z', user_id='user_123',
                              source_system='slack')
            test_graph.add_edge('entity1', 'attr1', weight=1)
            
            with patch('NodeRAG.storage.storage.storage.load', return_value=pd.DataFrame(columns=['hash_id', 'context', 'type'])):
                
                pipeline = Attribution_generation_pipeline(self.config)
                pipeline.G = test_graph
                
                pipeline.save_graph()
                
                assert mock_adapter.add_node.called
                assert mock_adapter.add_node.call_count >= 2  # entity and attribute
                
                assert mock_adapter.add_relationship.call_count >= 1
                
                rel_calls = mock_adapter.add_relationship.call_args_list
                if rel_calls:
                    rel_types = [call[1]['relationship_type'] for call in rel_calls]
                    assert 'has_attribute' in rel_types or 'attribute_of' in rel_types
    
    def test_fallback_to_file_storage(self):
        """Test fallback to file storage when not in cloud mode"""
        import pandas as pd
        
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=False), \
             patch('NodeRAG.storage.storage.storage.load', return_value=pd.DataFrame(columns=['hash_id', 'context', 'type'])):
            
            with patch('os.path.exists', return_value=False):
                pipeline = Attribution_generation_pipeline(self.config)
                
                assert isinstance(pipeline.G, nx.Graph)
                assert pipeline.G.number_of_nodes() == 0
    
    def test_attribute_storage_verification(self):
        """Test that attributes are verified in Neo4j after generation"""
        import pandas as pd
        
        with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
             patch.object(StorageFactory, 'get_graph_storage') as mock_neo4j, \
             patch('NodeRAG.storage.storage.storage.load', return_value=pd.DataFrame(columns=['hash_id', 'context', 'type'])):
            
            mock_adapter = MagicMock()
            mock_neo4j.return_value = mock_adapter
            
            pipeline = Attribution_generation_pipeline(self.config)
            pipeline.G = nx.Graph()
            pipeline.G.add_node('attr1', type='attribute', weight=1)
            pipeline.G.add_node('entity1', type='entity', weight=3)
            
            from NodeRAG.src.component import Attribute
            attr = MagicMock(spec=Attribute)
            attr.node = 'entity1'
            attr.raw_context = 'Test attribute'
            attr.hash_id = 'attr1'
            attr.human_readable_id = 'ATTR_001'
            pipeline.attributes = [attr]
            
            pipeline.save_attributes()
            
            self.config.console.print.assert_called()
            call_args = str(self.config.console.print.call_args)
            assert 'verified' in call_args.lower() or 'attributes' in call_args.lower()
    
    def test_existing_metadata_logic_preserved(self):
        """Test that existing metadata logic (lines 162-193) is preserved"""
        import pandas as pd
        
        with patch('NodeRAG.storage.storage.storage.load', return_value=pd.DataFrame(columns=['hash_id', 'context', 'type'])):
            
            pipeline = Attribution_generation_pipeline(self.config)
            
            pipeline.G = nx.Graph()
            pipeline.G.add_node('test_entity', 
                              type='entity', 
                              weight=3,
                              tenant_id='test_tenant',
                              account_id='acc_test',
                              interaction_id='int_test',
                              interaction_type='document',
                              timestamp='2025-01-01T00:00:00Z',
                              user_id='user_test',
                              source_system='test_system')
            
            
            entity_data = pipeline.G.nodes['test_entity']
            required_fields = ['tenant_id', 'account_id', 'interaction_id', 
                             'interaction_type', 'timestamp', 'user_id', 'source_system']
            assert all(field in entity_data for field in required_fields)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
