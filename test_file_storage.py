#!/usr/bin/env python3
"""Test file storage mode functionality"""

import tempfile
import os
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
import pandas as pd
import networkx as nx

def test_file_storage():
    """Test file storage mode"""
    print("Testing file storage mode...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = {
            'config': {'main_folder': tmpdir, 'language': 'en', 'chunk_size': 512},
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        
        StorageFactory.initialize(config, backend_mode='file')
        adapter = PipelineStorageAdapter()
        
        print(f'Backend mode: {adapter.backend_mode}')
        
        # Test graph operations
        test_graph = nx.Graph()
        test_graph.add_node('A', weight=1)
        test_graph.add_node('B', weight=2)
        test_graph.add_edge('A', 'B', weight=0.5)
        
        graph_path = f'{tmpdir}/test_graph.pkl'
        success = adapter.save_pickle(test_graph, graph_path, component_type='graph')
        print(f'Graph save success: {success}')
        
        loaded_graph = adapter.load_pickle(graph_path, component_type='graph')
        print(f'Graph loaded: {loaded_graph is not None}')
        if loaded_graph:
            print(f'Nodes: {len(loaded_graph.nodes())}, Edges: {len(loaded_graph.edges())}')
        
        test_data = pd.DataFrame({
            'hash_id': ['id1', 'id2'],
            'type': ['entity', 'relationship'],
            'context': ['Test entity', 'Test relationship']
        })
        
        parquet_path = f'{tmpdir}/test_data.parquet'
        success = adapter.save_parquet(test_data, parquet_path, component_type='data')
        print(f'Parquet save success: {success}')
        
        loaded_data = adapter.load_parquet(parquet_path, component_type='data')
        print(f'Parquet loaded: {loaded_data is not None}')
        if loaded_data is not None:
            print(f'Rows: {len(loaded_data)}')
        
        print('File storage mode test completed successfully!')
        return True

if __name__ == "__main__":
    test_file_storage()
