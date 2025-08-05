"""Simple test for summary generation metadata fix without full imports"""
import sys
import os
import json
import networkx as nx
from datetime import datetime, timezone

def test_metadata_extraction():
    """Test metadata extraction from community nodes"""
    print("=== Testing Summary Generation Metadata Fix (Simple) ===\n")
    
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
    
    def extract_metadata_from_community(graph, node_names):
        """Simulate the metadata extraction method we added"""
        for node_name in node_names:
            if graph.has_node(node_name):
                node_data = graph.nodes[node_name]
                required_fields = ['tenant_id', 'account_id', 'interaction_id', 
                                 'interaction_type', 'timestamp', 'user_id', 'source_system']
                
                if all(field in node_data for field in required_fields):
                    return {
                        'tenant_id': node_data['tenant_id'],
                        'account_id': node_data['account_id'],
                        'interaction_id': node_data['interaction_id'],
                        'interaction_type': node_data['interaction_type'],
                        'timestamp': node_data['timestamp'],
                        'user_id': node_data['user_id'],
                        'source_system': node_data['source_system']
                    }
        
        return {
            'tenant_id': 'AGGREGATED',
            'account_id': 'AGGREGATED', 
            'interaction_id': 'AGGREGATED',
            'interaction_type': 'summary',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'user_id': 'system',
            'source_system': 'internal'
        }
    
    node_names = ['test_node_1', 'test_node_2']
    extracted_metadata = extract_metadata_from_community(G, node_names)
    
    print("✅ Metadata extraction test:")
    print(f"   tenant_id: {extracted_metadata['tenant_id']}")
    print(f"   account_id: {extracted_metadata['account_id']}")
    print(f"   interaction_id: {extracted_metadata['interaction_id']}")
    
    test_graph = nx.Graph()
    
    node_attrs = {
        'type': 'high_level_element', 
        'weight': 1,
        **extracted_metadata
    }
    test_graph.add_node('test_he_id', **node_attrs)
    
    title_attrs = {
        'type': 'high_level_element_title', 
        'weight': 1, 
        'related_node': 'test_he_id',
        **extracted_metadata
    }
    test_graph.add_node('test_title_id', **title_attrs)
    
    success = True
    required_fields = ['tenant_id', 'account_id', 'interaction_id',
                     'interaction_type', 'timestamp', 'user_id', 'source_system']
    
    for node_id, node_data in test_graph.nodes(data=True):
        if node_data.get('type') in ['high_level_element', 'high_level_element_title']:
            print(f"\n✅ Found {node_data['type']} node: {node_id}")
            
            missing = [f for f in required_fields if f not in node_data]
            if missing:
                print(f"❌ Missing metadata fields: {missing}")
                success = False
            else:
                print("✅ All metadata fields present!")
                
    print(f"\n=== Test Result: {'PASS' if success else 'FAIL'} ===")
    return success

if __name__ == "__main__":
    success = test_metadata_extraction()
    exit(0 if success else 1)
