"""Test relationship metadata propagation in graph pipeline"""
import sys
sys.path.append('.')

import asyncio
import networkx as nx
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.test_utils.config_helper import create_test_nodeconfig, cleanup_test_output

def test_relationship_metadata():
    """Test that relationships properly support metadata"""
    print("=== Testing Relationship Metadata Support ===\n")
    
    test_metadata = EQMetadata(
        tenant_id='relationship_test_tenant',
        account_id='acc_12345678-1234-4567-8901-123456789012',
        interaction_id='int_12345678-1234-4567-8901-123456789012',
        interaction_type='email',
        text='Test email about relationships',
        timestamp='2024-01-01T10:00:00Z',
        user_id='reltest@example.com',
        source_system='gmail'
    )
    
    config = create_test_nodeconfig()
    pipeline = Graph_pipeline(config)
    
    print("Test 1: Checking metadata requirement...")
    try:
        asyncio.run(pipeline.add_relationships(
            ['Apple, acquired, Beats'],
            'text_hash_123'
        ))
        print("❌ add_relationships accepted call without metadata")
        return False
    except TypeError:
        print("✅ add_relationships correctly requires metadata parameter")
    
    print("\nTest 2: Checking None metadata rejection...")
    try:
        asyncio.run(pipeline.add_relationships(
            ['Apple, acquired, Beats'],
            'text_hash_123',
            None  # Explicit None
        ))
        print("❌ add_relationships accepted None metadata")
        return False
    except ValueError as e:
        if "REQUIRED" in str(e):
            print("✅ add_relationships correctly rejects None metadata")
        else:
            print(f"❌ Wrong error message: {e}")
            return False
    
    print("\nTest 3: Creating relationships with metadata...")
    try:
        relationships = [
            'Apple Inc, acquired, Beats Electronics',
            'Tim Cook, leads, Apple Inc'
        ]
        
        entity_ids = asyncio.run(pipeline.add_relationships(
            relationships,
            'text_hash_789',
            test_metadata
        ))
        
        print(f"✅ Created relationships, returned {len(entity_ids)} entity IDs")
        
        nodes_checked = 0
        metadata_present = True
        
        for node_id, node_data in pipeline.G.nodes(data=True):
            if node_data.get('text_hash_id') == 'text_hash_789':
                nodes_checked += 1
                
                checks = {
                    'tenant_id': node_data.get('tenant_id') == test_metadata.tenant_id,
                    'account_id': node_data.get('account_id') == test_metadata.account_id,
                    'user_id': node_data.get('user_id') == test_metadata.user_id,
                    'no_text_field': 'text' not in node_data
                }
                
                if not all(checks.values()):
                    print(f"❌ Node {node_id} missing metadata: {checks}")
                    metadata_present = False
                    break
        
        if nodes_checked > 0 and metadata_present:
            print(f"✅ All {nodes_checked} relationship nodes have proper metadata")
        else:
            print(f"❌ Metadata issues found on nodes")
            return False
        
        edges_checked = 0
        edge_metadata_present = True
        
        for edge in pipeline.G.edges(data=True):
            source, target, data = edge
            if data.get('text_hash_id') == 'text_hash_789':
                edges_checked += 1
                
                checks = {
                    'tenant_id': data.get('tenant_id') == test_metadata.tenant_id,
                    'account_id': data.get('account_id') == test_metadata.account_id,
                    'user_id': data.get('user_id') == test_metadata.user_id,
                    'no_text_field': 'text' not in data
                }
                
                if not all(checks.values()):
                    print(f"❌ Edge missing metadata: {checks}")
                    edge_metadata_present = False
                    break
        
        if edges_checked > 0 and edge_metadata_present:
            print(f"✅ All {edges_checked} relationship edges have proper metadata")
        else:
            print(f"❌ Metadata issues found on edges")
            return False
            
    except Exception as e:
        print(f"❌ Relationship creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n✅ All relationship metadata tests passed!")
    return True

def test_relationship_deduplication():
    """Test that relationship deduplication still works with metadata"""
    print("\n=== Testing Relationship Deduplication ===\n")
    
    config = create_test_nodeconfig()
    pipeline = Graph_pipeline(config)
    
    metadata1 = EQMetadata(
        tenant_id='tenant_1',
        account_id='acc_12345678-1234-4567-8901-123456789012',
        interaction_id='int_12345678-1234-4567-8901-123456789012',
        interaction_type='email',
        text='First interaction',
        timestamp='2024-01-01T10:00:00Z',
        user_id='user1@example.com',
        source_system='gmail'
    )
    
    metadata2 = EQMetadata(
        tenant_id='tenant_2',
        account_id='acc_12345678-1234-4567-8901-123456789013',
        interaction_id='int_12345678-1234-4567-8901-123456789013',
        interaction_type='chat',
        text='Second interaction',
        timestamp='2024-01-02T10:00:00Z',
        user_id='user2@example.com',
        source_system='internal'
    )
    
    rel = ['Apple, acquired, Beats']
    
    asyncio.run(pipeline.add_relationships(rel, 'text_1', metadata1))
    initial_edges = pipeline.G.number_of_edges()
    
    asyncio.run(pipeline.add_relationships(rel, 'text_2', metadata2))
    final_edges = pipeline.G.number_of_edges()
    
    if initial_edges == final_edges:
        print("✅ Relationship deduplication working")
        
        for edge in pipeline.G.edges(data=True):
            if edge[2].get('weight', 1) > 1:
                print(f"✅ Edge weight increased to {edge[2]['weight']}")
                break
    else:
        print("❌ Deduplication not working correctly")
        return False
    
    return True

if __name__ == "__main__":
    try:
        success1 = test_relationship_metadata()
        success2 = test_relationship_deduplication()
        
        if success1 and success2:
            print("\n✅ All relationship metadata tests passed!")
            exit(0)
        else:
            print("\n❌ Some tests failed")
            exit(1)
    finally:
        cleanup_test_output()
