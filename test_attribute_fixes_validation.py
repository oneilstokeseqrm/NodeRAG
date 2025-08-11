#!/usr/bin/env python3
"""Validation test for Task 4.0.7 fixes"""

import sys
import networkx as nx
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))

def test_attribute_storage_actually_stores():
    """Test that save_attributes() actually calls Neo4j storage"""
    
    print("=" * 60)
    print("Testing Attribute Storage Fixes")
    print("=" * 60)
    
    from NodeRAG.src.pipeline.attribute_generation import Attribution_generation_pipeline
    from NodeRAG.storage.storage_factory import StorageFactory
    from NodeRAG.tenant.tenant_context import TenantContext
    
    TenantContext.set_current_tenant("test_tenant")
    
    with patch.object(StorageFactory, 'is_cloud_storage', return_value=True), \
         patch.object(StorageFactory, 'get_graph_storage') as mock_neo4j, \
         patch('NodeRAG.storage.storage.storage.load'):
        
        mock_adapter = MagicMock()
        mock_adapter.add_node = MagicMock(return_value=True)
        mock_neo4j.return_value = mock_adapter
        
        config = MagicMock()
        config.console = MagicMock()
        
        pipeline = Attribution_generation_pipeline(config)
        pipeline.G = nx.Graph()
        
        pipeline.G.add_node('attr_001', 
                          type='attribute',
                          tenant_id='test_tenant',
                          account_id='acc_123',
                          interaction_id='int_456',
                          interaction_type='attribute',
                          timestamp='2025-01-01T00:00:00Z',
                          user_id='user_123',
                          source_system='internal')
        
        from NodeRAG.src.component import Attribute
        attr = MagicMock(spec=Attribute)
        attr.node = 'entity1'
        attr.raw_context = 'Test attribute text'
        attr.hash_id = 'attr_001'
        attr.human_readable_id = 'ATTR_001'
        
        pipeline.attributes = [attr]
        
        pipeline.save_attributes()
        
        assert mock_adapter.add_node.called, "❌ FAILED: add_node never called!"
        
        call_args = mock_adapter.add_node.call_args
        assert call_args[1]['node_id'] == 'attr_001', "❌ FAILED: Wrong node_id"
        assert call_args[1]['node_type'] == 'attribute', "❌ FAILED: Wrong node_type"
        assert 'metadata' in call_args[1], "❌ FAILED: No metadata provided"
        
        metadata = call_args[1]['metadata']
        assert metadata.tenant_id == 'test_tenant', "❌ FAILED: Wrong tenant_id"
        
        print("✅ save_attributes() now actually stores to Neo4j")
        print(f"✅ add_node called with correct parameters")
        print(f"✅ Metadata propagated correctly: tenant_id={metadata.tenant_id}")
    
    print("\n" + "=" * 60)
    print("✅ VALIDATION PASSED - Fixes are working!")
    print("=" * 60)
    
    TenantContext.clear_current_tenant()
    return True

if __name__ == "__main__":
    success = test_attribute_storage_actually_stores()
    sys.exit(0 if success else 1)
