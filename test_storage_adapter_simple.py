#!/usr/bin/env python3
"""Simple test of storage adapter without full NodeRAG imports"""

import sys
import os
import tempfile
import pandas as pd
import networkx as nx
from pathlib import Path

sys.path.insert(0, '/home/ubuntu/repos/NodeRAG')

def test_storage_adapter_basic():
    """Test basic storage adapter functionality"""
    print("Testing storage adapter basic functionality...")
    
    try:
        from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
        print("✅ Successfully imported PipelineStorageAdapter")
    except Exception as e:
        print(f"❌ Failed to import PipelineStorageAdapter: {e}")
        return False
    
    try:
        adapter = PipelineStorageAdapter(backend_mode='file')
        print(f"✅ Adapter initialized with backend: {adapter.backend_mode}")
    except Exception as e:
        print(f"❌ Failed to initialize adapter: {e}")
        return False
    
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            test_graph = nx.Graph()
            test_graph.add_node('A', weight=1)
            test_graph.add_node('B', weight=2)
            test_graph.add_edge('A', 'B', weight=0.5)
            
            graph_path = f'{tmpdir}/test_graph.pkl'
            success = adapter.save_pickle(test_graph, graph_path, component_type='graph')
            print(f"✅ Graph save success: {success}")
            
            loaded_graph = adapter.load_pickle(graph_path, component_type='graph')
            if loaded_graph and len(loaded_graph.nodes()) == 2:
                print(f"✅ Graph loaded correctly: {len(loaded_graph.nodes())} nodes, {len(loaded_graph.edges())} edges")
            else:
                print("❌ Graph loading failed")
                return False
                
        except Exception as e:
            print(f"❌ Graph operations failed: {e}")
            return False
        
        try:
            test_data = pd.DataFrame({
                'hash_id': ['id1', 'id2'],
                'type': ['entity', 'relationship'],
                'context': ['Test entity', 'Test relationship']
            })
            
            parquet_path = f'{tmpdir}/test_data.parquet'
            success = adapter.save_parquet(test_data, parquet_path, component_type='data')
            print(f"✅ Parquet save success: {success}")
            
            loaded_data = adapter.load_parquet(parquet_path, component_type='data')
            if loaded_data is not None and len(loaded_data) == 2:
                print(f"✅ Parquet loaded correctly: {len(loaded_data)} rows")
            else:
                print("❌ Parquet loading failed")
                return False
                
        except Exception as e:
            print(f"❌ Parquet operations failed: {e}")
            return False
        
        try:
            test_json = {'test': 'data', 'items': [1, 2, 3]}
            json_path = f'{tmpdir}/test_data.json'
            
            success = adapter.save_json(test_json, json_path)
            print(f"✅ JSON save success: {success}")
            
            loaded_json = adapter.load_json(json_path)
            if loaded_json and loaded_json.get('test') == 'data':
                print(f"✅ JSON loaded correctly")
            else:
                print("❌ JSON loading failed")
                return False
                
        except Exception as e:
            print(f"❌ JSON operations failed: {e}")
            return False
    
    print("✅ All storage adapter tests passed!")
    return True

if __name__ == "__main__":
    success = test_storage_adapter_basic()
    if success:
        print("\n🎉 Storage adapter validation completed successfully!")
    else:
        print("\n❌ Storage adapter validation failed!")
        sys.exit(1)
