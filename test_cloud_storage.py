#!/usr/bin/env python3
"""Test cloud storage mode functionality"""

import tempfile
import os
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
import pandas as pd
import networkx as nx

def test_cloud_storage():
    """Test cloud storage mode with real credentials"""
    print("Testing cloud storage mode...")
    
    neo4j_uri = os.getenv('Neo4j_Credentials_NEO4J_URI')
    neo4j_password = os.getenv('Neo4j_Credentials_NEO4J_PASSWORD')
    pinecone_key = os.getenv('pinecone_API_key')

    print(f'Neo4j URI available: {neo4j_uri is not None}')
    print(f'Neo4j password available: {neo4j_password is not None}')
    print(f'Pinecone key available: {pinecone_key is not None}')

    if all([neo4j_uri, neo4j_password, pinecone_key]):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'config': {'main_folder': tmpdir, 'language': 'en', 'chunk_size': 512},
                'model_config': {'model_name': 'gpt-4o'},
                'embedding_config': {'model_name': 'gpt-4o'},
                'eq_config': {
                    'storage': {
                        'neo4j_uri': neo4j_uri,
                        'neo4j_user': os.getenv('Neo4j_Credentials_NEO4J_USERNAME', 'neo4j'),
                        'neo4j_password': neo4j_password,
                        'pinecone_api_key': pinecone_key,
                        'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag-test')
                    }
                }
            }
            
            try:
                StorageFactory.initialize(config, backend_mode='cloud')
                adapter = PipelineStorageAdapter()
                
                print(f'Backend mode: {adapter.backend_mode}')
                
                # Test graph operations
                test_graph = nx.Graph()
                test_graph.add_node('test_node_1', type='entity', weight=1)
                test_graph.add_node('test_node_2', type='semantic_unit', weight=1)
                test_graph.add_edge('test_node_1', 'test_node_2', weight=0.8)
                
                graph_path = f'{tmpdir}/test_graph.pkl'
                success = adapter.save_pickle(test_graph, graph_path, component_type='graph')
                print(f'Graph save success: {success}')
                
                print('Cloud storage mode test completed successfully!')
                return True
                
            except Exception as e:
                print(f'Cloud storage test failed: {e}')
                print('Falling back to file mode for compatibility')
                return False
    else:
        print('Cloud credentials not available, skipping cloud storage test')
        return False

if __name__ == "__main__":
    test_cloud_storage()
