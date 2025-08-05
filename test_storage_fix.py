"""Test that storage.load_pickle() fix works correctly"""
import networkx as nx
import tempfile
import os
import sys

sys.path.insert(0, '.')

from NodeRAG.storage import storage

def test_storage_pickle_operations():
    """Test that graph can be saved and loaded correctly"""
    print("=== Testing Storage Pickle Operations ===\n")
    
    G = nx.Graph()
    test_metadata = {
        'tenant_id': 'test_tenant',
        'account_id': 'acc_12345678-1234-4567-8901-123456789012',
        'interaction_id': 'int_12345678-1234-4567-8901-123456789012',
        'interaction_type': 'email',
        'timestamp': '2024-01-01T10:00:00Z',
        'user_id': 'test@example.com',
        'source_system': 'gmail'
    }
    
    G.add_node('test_node_1', type='semantic_unit', **test_metadata)
    G.add_node('test_node_2', type='entity', **test_metadata)
    G.add_edge('test_node_1', 'test_node_2', weight=1)
    
    print(f"Original graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    print(f"Original graph type: {type(G)}")
    
    with tempfile.NamedTemporaryFile(suffix='.pickle', delete=False) as tmp:
        test_path = tmp.name
    
    try:
        storage(G).save_pickle(test_path)
        print(f"✅ Graph saved to {test_path}")
        
        loaded_G = storage.load_pickle(test_path)
        print(f"Loaded graph type: {type(loaded_G)}")
        print(f"Loaded graph: {loaded_G.number_of_nodes()} nodes, {loaded_G.number_of_edges()} edges")
        
        assert isinstance(loaded_G, nx.Graph), f"Expected nx.Graph, got {type(loaded_G)}"
        print("✅ Loaded graph is correct type")
        
        assert loaded_G.has_node('test_node_1'), "has_node() should work"
        assert loaded_G.has_node('test_node_2'), "has_node() should work"
        print("✅ has_node() operations work")
        
        node_data = loaded_G.nodes['test_node_1']
        assert node_data['tenant_id'] == 'test_tenant', f"Expected test_tenant, got {node_data['tenant_id']}"
        assert node_data['type'] == 'semantic_unit', f"Expected semantic_unit, got {node_data['type']}"
        print("✅ Metadata preserved correctly")
        
        assert loaded_G.has_edge('test_node_1', 'test_node_2'), "Edge should be preserved"
        print("✅ Edges preserved correctly")
        
        print("\n✅ ALL STORAGE TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Storage test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if os.path.exists(test_path):
            os.unlink(test_path)

if __name__ == "__main__":
    success = test_storage_pickle_operations()
    exit(0 if success else 1)
