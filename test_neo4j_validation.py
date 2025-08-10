#!/usr/bin/env python3
"""Focused Neo4j validation for summary generation storage"""

import os
import sys
import networkx as nx
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))

def test_neo4j_validation():
    """Test core Neo4j storage operations without full pipeline dependencies"""
    
    print("=" * 60)
    print("Testing Neo4j Storage Operations")
    print("=" * 60)
    
    from NodeRAG.tenant.tenant_context import TenantContext
    from NodeRAG.storage.storage_factory import StorageFactory
    from NodeRAG.standards.eq_metadata import EQMetadata
    
    tenant_id = "test_tenant_406"
    TenantContext.set_current_tenant(tenant_id)
    
    print("\n1. Testing StorageFactory cloud detection...")
    try:
        factory = StorageFactory()
        is_cloud = factory.is_cloud_storage()
        print(f"✅ StorageFactory.is_cloud_storage() = {is_cloud}")
    except Exception as e:
        print(f"❌ StorageFactory failed: {e}")
        return False
    
    print("\n2. Testing Neo4j adapter access...")
    try:
        if is_cloud:
            neo4j_adapter = factory.get_graph_storage()
            print(f"✅ Neo4j adapter obtained: {type(neo4j_adapter)}")
        else:
            print("⚠️  Not in cloud mode, skipping Neo4j adapter test")
    except Exception as e:
        print(f"❌ Neo4j adapter failed: {e}")
        return False
    
    print("\n3. Testing cross-tenant metadata logic...")
    try:
        test_graph = nx.Graph()
        test_graph.add_node('node1', type='entity', tenant_id='tenant_a',
                          account_id='acc_a', interaction_id='int_a',
                          interaction_type='chat', timestamp='2025-01-01T00:00:00Z',
                          user_id='user_a', source_system='slack')
        test_graph.add_node('node2', type='entity', tenant_id='tenant_b',
                          account_id='acc_b', interaction_id='int_b',
                          interaction_type='email', timestamp='2025-01-01T00:00:00Z',
                          user_id='user_b', source_system='gmail')
        
        from NodeRAG.src.pipeline.summary_generation import SummaryGeneration
        
        config = MagicMock()
        config.graph_path = "/tmp/test_graph.pkl"
        
        with patch('os.path.exists', return_value=False):
            pipeline = SummaryGeneration(config)
            pipeline.G = test_graph
            
            metadata = pipeline._extract_metadata_from_community(['node1', 'node2'])
            
            if metadata.tenant_id == 'AGGREGATED':
                print("✅ Cross-tenant metadata returns AGGREGATED")
            else:
                print(f"❌ Expected AGGREGATED, got {metadata.tenant_id}")
                return False
                
            metadata_single = pipeline._extract_metadata_from_community(['node1'])
            if metadata_single.tenant_id == 'tenant_a':
                print("✅ Single-tenant metadata preserved")
            else:
                print(f"❌ Expected tenant_a, got {metadata_single.tenant_id}")
                return False
                
    except Exception as e:
        print(f"❌ Metadata logic failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n4. Testing graph initialization...")
    try:
        with patch('os.path.exists', return_value=False):
            pipeline = SummaryGeneration(config)
            
            if hasattr(pipeline, 'G') and isinstance(pipeline.G, nx.Graph):
                print("✅ Graph initialized correctly")
            else:
                print(f"❌ Graph not initialized: {type(getattr(pipeline, 'G', None))}")
                return False
                
    except Exception as e:
        print(f"❌ Graph initialization failed: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("✅ ALL CORE TESTS PASSED")
    print("=" * 60)
    
    TenantContext.clear_current_tenant()
    return True

if __name__ == "__main__":
    success = test_neo4j_validation()
    sys.exit(0 if success else 1)
