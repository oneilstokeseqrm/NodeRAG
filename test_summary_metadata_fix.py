"""Test that summary generation nodes have metadata"""
import sys
import os
import json
import tempfile
import networkx as nx
from datetime import datetime, timezone

sys.path.insert(0, '.')

from NodeRAG.src.pipeline.summary_generation import SummaryGeneration
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.test_utils.config_helper import create_test_nodeconfig
from NodeRAG.storage import storage

def test_summary_metadata():
    """Test that high_level_element nodes get metadata"""
    print("=== Testing Summary Generation Metadata Fix ===\n")
    
    config = create_test_nodeconfig()
    
    G = nx.Graph()
    test_metadata_fields = {
        'tenant_id': 'test_tenant',
        'account_id': 'acc_12345678-1234-4567-8901-123456789012',
        'interaction_id': 'int_12345678-1234-4567-8901-123456789012',
        'interaction_type': 'email',
        'timestamp': '2024-01-01T10:00:00Z',
        'user_id': 'test@example.com',
        'source_system': 'gmail'
    }
    
    G.add_node('test_node_1', type='semantic_unit', **test_metadata_fields)
    G.add_node('test_node_2', type='entity', **test_metadata_fields)
    
    storage(G).save_pickle(config.graph_path)
    
    test_summary = {
        'community': ['test_node_1', 'test_node_2'],
        'response': {
            'high_level_elements': [
                {
                    'description': 'Test high level element description',
                    'title': 'Test Title'
                }
            ]
        }
    }
    
    with open(config.summary_path, 'w') as f:
        f.write(json.dumps(test_summary) + '\n')
    
    try:
        summary_gen = SummaryGeneration(config)
        import asyncio
        asyncio.run(summary_gen.high_level_element_summary())
        
        success = True
        required_fields = ['tenant_id', 'account_id', 'interaction_id',
                         'interaction_type', 'timestamp', 'user_id', 'source_system']
        
        for node_id, node_data in summary_gen.G.nodes(data=True):
            if node_data.get('type') in ['high_level_element', 'high_level_element_title']:
                print(f"Found {node_data['type']} node: {node_id[:20]}...")
                
                missing = [f for f in required_fields if f not in node_data]
                if missing:
                    print(f"❌ Missing metadata fields: {missing}")
                    success = False
                else:
                    print("✅ All metadata fields present!")
                    print(f"   tenant_id: {node_data['tenant_id']}")
                    
        return success
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_summary_metadata()
    exit(0 if success else 1)
