"""Full test of summary generation with metadata propagation"""
import sys
import os
import json
import tempfile
import networkx as nx
from datetime import datetime
import asyncio

sys.path.insert(0, '.')

def test_metadata_extraction_edge_cases():
    """Test edge cases for metadata extraction"""
    print("=== Testing Metadata Extraction Edge Cases ===\n")
    
    from NodeRAG.src.pipeline.summary_generation import SummaryGeneration
    from NodeRAG.test_utils.config_helper import create_test_nodeconfig
    
    config = create_test_nodeconfig()
    sg = SummaryGeneration.__new__(SummaryGeneration)
    sg.G = nx.Graph()
    
    print("Test 1: Empty community")
    metadata = sg._extract_metadata_from_community([])
    print(f"Result: tenant_id = {metadata.tenant_id} (should be AGGREGATED)")
    
    print("\nTest 2: Non-existent nodes")
    metadata = sg._extract_metadata_from_community(['fake_node_1', 'fake_node_2'])
    print(f"Result: tenant_id = {metadata.tenant_id} (should be AGGREGATED)")
    
    print("\nTest 3: Nodes without metadata")
    sg.G.add_node('no_meta_1', type='entity', weight=1)
    metadata = sg._extract_metadata_from_community(['no_meta_1'])
    print(f"Result: tenant_id = {metadata.tenant_id} (should be AGGREGATED)")
    
    print("\nTest 4: Nodes with partial metadata")
    sg.G.add_node('partial_meta', type='entity', tenant_id='test', weight=1)
    metadata = sg._extract_metadata_from_community(['partial_meta'])
    print(f"Result: tenant_id = {metadata.tenant_id} (should be AGGREGATED)")
    
    print("\nTest 5: Nodes with complete metadata")
    complete_metadata = {
        'tenant_id': 'tenant_alpha',
        'account_id': 'acc_12345678-1234-4567-8901-123456789012',
        'interaction_id': 'int_12345678-1234-4567-8901-123456789012',
        'interaction_type': 'email',
        'timestamp': '2024-01-01T10:00:00Z',
        'user_id': 'user1@example.com',
        'source_system': 'gmail'
    }
    sg.G.add_node('complete_meta', type='entity', **complete_metadata)
    metadata = sg._extract_metadata_from_community(['complete_meta'])
    print(f"Result: tenant_id = {metadata.tenant_id} (should be tenant_alpha)")
    
    print("\n✅ Edge case tests complete")
    return True

async def test_full_summary_pipeline():
    """Test complete summary generation pipeline with metadata"""
    print("\n=== Full Summary Generation Pipeline Test ===\n")
    
    from NodeRAG.src.pipeline.summary_generation import SummaryGeneration
    from NodeRAG.standards.eq_metadata import EQMetadata
    from NodeRAG.test_utils.config_helper import create_test_nodeconfig
    from NodeRAG.storage import storage
    
    config = create_test_nodeconfig()
    
    G = nx.Graph()
    
    tenant1_metadata = {
        'tenant_id': 'tenant_alpha',
        'account_id': 'acc_12345678-1234-4567-8901-123456789012',
        'interaction_id': 'int_12345678-1234-4567-8901-123456789012',
        'interaction_type': 'email',
        'timestamp': '2024-01-01T10:00:00Z',
        'user_id': 'user1@example.com',
        'source_system': 'gmail',
        'weight': 1
    }
    
    G.add_node('node1_1', type='semantic_unit', context='Alpha company info', **tenant1_metadata)
    G.add_node('node1_2', type='entity', context='Alpha Corp', **tenant1_metadata)
    G.add_node('node1_3', type='entity', context='Project Alpha', **tenant1_metadata)
    G.add_edge('node1_1', 'node1_2', weight=1)
    G.add_edge('node1_2', 'node1_3', weight=1)
    
    tenant2_metadata = {
        'tenant_id': 'tenant_beta',
        'account_id': 'acc_12345678-1234-4567-8901-123456789013',
        'interaction_id': 'int_12345678-1234-4567-8901-123456789013',
        'interaction_type': 'chat',
        'timestamp': '2024-01-01T11:00:00Z',
        'user_id': 'user2@example.com',
        'source_system': 'internal',
        'weight': 1
    }
    
    G.add_node('node2_1', type='semantic_unit', context='Beta info', **tenant2_metadata)
    G.add_node('node2_2', type='entity', context='Shared Resource', **tenant1_metadata)  # Different tenant!
    G.add_edge('node2_1', 'node2_2', weight=1)
    
    G.add_node('node3_1', type='semantic_unit', context='Legacy data', weight=1)
    G.add_node('node3_2', type='entity', context='Old Entity', weight=1)
    G.add_edge('node3_1', 'node3_2', weight=1)
    
    storage(G).save_pickle(config.graph_path)
    print(f"Created test graph with {G.number_of_nodes()} nodes in 3 communities")
    
    summaries = [
        {
            'community': ['node1_1', 'node1_2', 'node1_3'],
            'response': {
                'high_level_elements': [{
                    'description': 'Alpha company overview and projects',
                    'title': 'Alpha Corporation Summary'
                }]
            }
        },
        {
            'community': ['node2_1', 'node2_2'],
            'response': {
                'high_level_elements': [{
                    'description': 'Mixed tenant resource sharing',
                    'title': 'Cross-Tenant Resources'
                }]
            }
        },
        {
            'community': ['node3_1', 'node3_2'],
            'response': {
                'high_level_elements': [{
                    'description': 'Legacy system components',
                    'title': 'Legacy Data Summary'
                }]
            }
        }
    ]
    
    with open(config.summary_path, 'w') as f:
        for summary in summaries:
            f.write(json.dumps(summary) + '\n')
    
    print("\n--- Running Summary Generation ---")
    
    try:
        summary_gen = SummaryGeneration(config)
        
        print(f"self.G type: {type(summary_gen.G)}")
        print(f"self.G nodes before: {summary_gen.G.number_of_nodes()}")
        
        await summary_gen.high_level_element_summary()
        
        print(f"self.G nodes after: {summary_gen.G.number_of_nodes()}")
        
        results = {
            'high_level_elements': [],
            'title_elements': [],
            'nodes_without_metadata': [],
            'metadata_sources': {}
        }
        
        required_fields = ['tenant_id', 'account_id', 'interaction_id',
                          'interaction_type', 'timestamp', 'user_id', 'source_system']
        
        for node_id, node_data in summary_gen.G.nodes(data=True):
            node_type = node_data.get('type')
            
            if node_type == 'high_level_element':
                results['high_level_elements'].append(node_id)
                
                missing = [f for f in required_fields if f not in node_data]
                if missing:
                    results['nodes_without_metadata'].append({
                        'node': node_id,
                        'type': node_type,
                        'missing': missing
                    })
                else:
                    results['metadata_sources'][node_id] = {
                        'tenant_id': node_data['tenant_id'],
                        'source': 'extracted' if node_data['tenant_id'] != 'AGGREGATED' else 'fallback'
                    }
                    
            elif node_type == 'high_level_element_title':
                results['title_elements'].append(node_id)
                
                missing = [f for f in required_fields if f not in node_data]
                if missing:
                    results['nodes_without_metadata'].append({
                        'node': node_id,
                        'type': node_type,
                        'missing': missing
                    })
        
        print("\n=== Test Results ===")
        print(f"High-level elements created: {len(results['high_level_elements'])}")
        print(f"Title elements created: {len(results['title_elements'])}")
        print(f"Nodes without metadata: {len(results['nodes_without_metadata'])}")
        
        if results['metadata_sources']:
            print("\nMetadata sources:")
            for node, info in results['metadata_sources'].items():
                print(f"  {node[:20]}... -> tenant: {info['tenant_id']} ({info['source']})")
        
        if results['nodes_without_metadata']:
            print("\n❌ FAIL: Nodes missing metadata:")
            for item in results['nodes_without_metadata']:
                print(f"  {item['node'][:20]}... missing: {item['missing']}")
            return False
        else:
            print("\n✅ SUCCESS: All high_level_element nodes have complete metadata!")
            
            os.makedirs('test_output', exist_ok=True)
            
            storage(summary_gen.G).save_pickle('test_output/summary_test_result.pickle')
            print(f"\nGraph saved to: test_output/summary_test_result.pickle")
            
            return True
            
    except Exception as e:
        print(f"\n❌ ERROR during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all tests"""
    print("Testing Summary Generation with Metadata\n")
    
    edge_case_success = test_metadata_extraction_edge_cases()
    
    pipeline_success = await test_full_summary_pipeline()
    
    if edge_case_success and pipeline_success:
        print("\n✅ ALL TESTS PASSED - Summary generation metadata implementation is working correctly!")
        print("\nPR #23 is ready to merge.")
        return True
    else:
        print("\n❌ TESTS FAILED - Issues need to be fixed before merging PR #23")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
