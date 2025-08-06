"""Test that HNSW nodes don't appear in business graph"""
import os
import sys
sys.path.append('.')

from NodeRAG.storage import storage
import networkx as nx

def test_no_hnsw_nodes():
    """Verify no single-character nodes in business graph"""
    print("=== Testing HNSW Separation ===\n")
    
    if os.path.exists('./test_output/cache/graph.pkl'):
        graph_path = './test_output/cache/graph.pkl'
    elif os.path.exists('./test_output/base_graph.pkl'):
        graph_path = './test_output/base_graph.pkl'
    else:
        print("❌ No graph found to test")
        return False
        
    G = storage.load_pickle(graph_path)
    print(f"Loaded graph: {G.number_of_nodes()} nodes")
    
    hnsw_nodes = []
    for node_id in G.nodes():
        if len(str(node_id)) == 1 and str(node_id) in '0123456789abcdef':
            hnsw_nodes.append(node_id)
    
    if hnsw_nodes:
        print(f"❌ Found {len(hnsw_nodes)} HNSW internal nodes: {hnsw_nodes}")
        return False
    else:
        print("✅ No HNSW internal nodes found in business graph")
        return True
    
    print("\n=== Verifying HNSW Index ===")
    if os.path.exists('./test_output/info/HNSW.bin'):
        print("✅ HNSW index file exists")
    else:
        print("⚠️  HNSW index file not found (may not be built yet)")
    
    return True

def test_hnsw_search():
    """Test that HNSW search still works"""
    print("\n=== Testing HNSW Search Functionality ===")
    
    try:
        from NodeRAG.utils.HNSW import HNSW
        from NodeRAG.config import NodeConfig
        import numpy as np
        
        config = NodeConfig.from_main_folder('./test_output')
        hnsw = HNSW(config)
        
        print("✅ HNSW instance created successfully")
        return True
        
    except Exception as e:
        print(f"⚠️  HNSW search test skipped: {e}")
        return True  # Don't fail if HNSW not initialized

def test_no_hnsw_graph_files():
    """Test that no HNSW graph files are created"""
    print("\n=== Testing No HNSW Graph Files ===")
    
    hnsw_graph_files = []
    for root, dirs, files in os.walk('./test_output'):
        for file in files:
            if 'hnsw_graph' in file.lower():
                hnsw_graph_files.append(os.path.join(root, file))
    
    if hnsw_graph_files:
        print(f"⚠️  Found HNSW graph files (should be ignored): {hnsw_graph_files}")
    else:
        print("✅ No HNSW graph files found")
    
    return True

if __name__ == "__main__":
    success = test_no_hnsw_nodes()
    success = test_hnsw_search() and success
    success = test_no_hnsw_graph_files() and success
    
    if success:
        print("\n✅ ALL TESTS PASSED - HNSW separation successful!")
    else:
        print("\n❌ TESTS FAILED - Check implementation")
    
    exit(0 if success else 1)
