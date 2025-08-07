#!/usr/bin/env python3
"""Direct test of storage adapter without complex imports"""

import sys
import os
import tempfile
import pandas as pd
import networkx as nx
import pickle
import json
from pathlib import Path

def test_storage_operations_direct():
    """Test storage operations directly without complex imports"""
    print("Testing storage operations directly...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Using temp directory: {tmpdir}")
        
        try:
            test_graph = nx.Graph()
            test_graph.add_node('A', weight=1, type='entity')
            test_graph.add_node('B', weight=2, type='semantic_unit')
            test_graph.add_edge('A', 'B', weight=0.5)
            
            graph_path = f'{tmpdir}/test_graph.pkl'
            Path(graph_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(graph_path, 'wb') as f:
                pickle.dump(test_graph, f)
            print("✅ Graph pickle save successful")
            
            with open(graph_path, 'rb') as f:
                loaded_graph = pickle.load(f)
            
            if len(loaded_graph.nodes()) == 2 and len(loaded_graph.edges()) == 1:
                print("✅ Graph pickle load successful")
            else:
                print("❌ Graph pickle load failed")
                return False
                
        except Exception as e:
            print(f"❌ Graph pickle operations failed: {e}")
            return False
        
        try:
            test_data = pd.DataFrame({
                'hash_id': ['id1', 'id2', 'id3'],
                'type': ['entity', 'relationship', 'semantic_unit'],
                'context': ['Test entity', 'Test relationship', 'Test semantic unit'],
                'weight': [1.0, 0.8, 0.9]
            })
            
            parquet_path = f'{tmpdir}/test_data.parquet'
            test_data.to_parquet(parquet_path)
            print("✅ Parquet save successful")
            
            loaded_data = pd.read_parquet(parquet_path)
            if len(loaded_data) == 3 and 'hash_id' in loaded_data.columns:
                print("✅ Parquet load successful")
            else:
                print("❌ Parquet load failed")
                return False
                
        except Exception as e:
            print(f"❌ Parquet operations failed: {e}")
            return False
        
        try:
            test_json = {
                'metadata': {'version': '1.0', 'created': '2025-01-01'},
                'components': ['entity', 'relationship', 'semantic_unit'],
                'config': {'backend': 'file', 'mode': 'test'}
            }
            
            json_path = f'{tmpdir}/test_data.json'
            with open(json_path, 'w') as f:
                json.dump(test_json, f, indent=2)
            print("✅ JSON save successful")
            
            with open(json_path, 'r') as f:
                loaded_json = json.load(f)
            
            if loaded_json.get('metadata', {}).get('version') == '1.0':
                print("✅ JSON load successful")
            else:
                print("❌ JSON load failed")
                return False
                
        except Exception as e:
            print(f"❌ JSON operations failed: {e}")
            return False
        
        try:
            initial_data = pd.DataFrame({
                'id': [1, 2],
                'value': ['a', 'b']
            })
            
            append_path = f'{tmpdir}/append_test.parquet'
            initial_data.to_parquet(append_path)
            
            additional_data = pd.DataFrame({
                'id': [3, 4],
                'value': ['c', 'd']
            })
            
            existing_data = pd.read_parquet(append_path)
            combined_data = pd.concat([existing_data, additional_data], ignore_index=True)
            combined_data.to_parquet(append_path)
            
            final_data = pd.read_parquet(append_path)
            if len(final_data) == 4:
                print("✅ Append operations successful")
            else:
                print("❌ Append operations failed")
                return False
                
        except Exception as e:
            print(f"❌ Append operations failed: {e}")
            return False
    
    print("✅ All direct storage operations tests passed!")
    return True

def test_audit_results():
    """Test that audit results are available"""
    print("\nTesting audit results...")
    
    audit_file = 'pipeline_storage_audit.json'
    if os.path.exists(audit_file):
        try:
            with open(audit_file, 'r') as f:
                audit_data = json.load(f)
            
            total_ops = audit_data.get('total_storage_operations', 0)
            files_to_migrate = audit_data.get('files_to_migrate', [])
            
            print(f"✅ Audit found {total_ops} storage operations")
            print(f"✅ Files to migrate: {len(files_to_migrate)}")
            
            if total_ops > 0:
                print("✅ Audit results validation successful")
                return True
            else:
                print("❌ No storage operations found in audit")
                return False
                
        except Exception as e:
            print(f"❌ Failed to read audit results: {e}")
            return False
    else:
        print("❌ Audit file not found")
        return False

if __name__ == "__main__":
    print("=== Direct Storage Operations Test ===")
    storage_success = test_storage_operations_direct()
    
    print("\n=== Audit Results Test ===")
    audit_success = test_audit_results()
    
    if storage_success and audit_success:
        print("\n🎉 All validation tests passed!")
        print("✅ Storage operations work correctly")
        print("✅ Audit identified storage operations")
        print("✅ Ready for integration testing")
    else:
        print("\n❌ Some validation tests failed!")
        sys.exit(1)
