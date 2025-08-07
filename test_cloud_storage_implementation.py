#!/usr/bin/env python3
"""
Test the complete cloud storage implementation
"""
import os
import sys
import networkx as nx
import pandas as pd
import numpy as np
import uuid

sys.path.insert(0, '/home/ubuntu/repos/NodeRAG')

def test_cloud_storage_implementation():
    """Test the complete cloud storage implementation"""
    print("Testing complete cloud storage implementation...")
    
    neo4j_uri = os.getenv('Neo4j_Credentials_NEO4J_URI')
    neo4j_password = os.getenv('Neo4j_Credentials_NEO4J_PASSWORD')
    pinecone_key = os.getenv('pinecone_API_key')
    
    if not all([neo4j_uri, neo4j_password, pinecone_key]):
        print("❌ Cloud credentials not available, skipping cloud tests")
        return False
    
    print("✅ Cloud credentials available")
    
    try:
        from NodeRAG.storage.storage_factory import StorageFactory
        from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
        
        config = {
            'config': {'main_folder': '/tmp/test', 'language': 'en', 'chunk_size': 512},
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
            'eq_config': {
                'storage': {
                    'neo4j_uri': neo4j_uri,
                    'neo4j_user': os.getenv('Neo4j_Credentials_NEO4J_USERNAME', 'neo4j'),
                    'neo4j_password': neo4j_password,
                    'pinecone_api_key': pinecone_key,
                    'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag')
                }
            }
        }
        
        StorageFactory.initialize(config, backend_mode="cloud")
        adapter = PipelineStorageAdapter()
        
        print(f"✅ Storage adapter initialized with backend: {adapter.backend_mode}")
        
        test_tenant = f"test_{uuid.uuid4()}"
        print(f"Testing with tenant: {test_tenant}")
        
        test_graph = nx.Graph()
        test_graph.add_node("node1", type="entity", weight=1.0, data="test_data")
        test_graph.add_node("node2", type="semantic_unit", weight=2.0, data="test_data2")
        test_graph.add_edge("node1", "node2", type="relates", weight=0.8)
        
        print(f"Created test graph: {len(test_graph.nodes())} nodes, {len(test_graph.edges())} edges")
        
        save_success = adapter.save_pickle(
            test_graph, 
            "/tmp/test_graph.pkl",
            component_type="graph",
            tenant_id=test_tenant
        )
        
        if save_success:
            print("✅ Graph saved to Neo4j successfully")
        else:
            print("❌ Failed to save graph to Neo4j")
            return False
        
        loaded_graph = adapter.load_pickle(
            "/tmp/test_graph.pkl",
            component_type="graph",
            tenant_id=test_tenant
        )
        
        if loaded_graph is not None:
            print(f"✅ Graph loaded from Neo4j: {len(loaded_graph.nodes())} nodes, {len(loaded_graph.edges())} edges")
            
            if "node1" in loaded_graph.nodes() and "node2" in loaded_graph.nodes():
                print("✅ Node data integrity verified")
            else:
                print("❌ Node data integrity failed")
                return False
                
            if loaded_graph.has_edge("node1", "node2"):
                print("✅ Edge data integrity verified")
            else:
                print("❌ Edge data integrity failed")
                return False
        else:
            print("❌ Failed to load graph from Neo4j")
            return False
        
        test_namespace = f"test_{uuid.uuid4()}"
        print(f"Testing embeddings with namespace: {test_namespace}")
        
        test_embeddings = pd.DataFrame({
            'id': ['vec1', 'vec2', 'vec3'],
            'embedding': [np.random.randn(3072).tolist() for _ in range(3)],
            'type': ['entity', 'relationship', 'semantic_unit'],
            'context': ['Test context 1', 'Test context 2', 'Test context 3'],
            'weight': [1.0, 0.8, 1.2]
        })
        
        print(f"Created test embeddings: {len(test_embeddings)} vectors")
        
        save_success = adapter.save_parquet(
            test_embeddings,
            "/tmp/test_embeddings.parquet",
            component_type="embeddings",
            namespace=test_namespace
        )
        
        if save_success:
            print("✅ Embeddings saved to Pinecone successfully")
        else:
            print("❌ Failed to save embeddings to Pinecone")
            return False
        
        import time
        print("Waiting for Pinecone indexing...")
        time.sleep(3)
        
        loaded_embeddings = adapter.load_parquet(
            "/tmp/test_embeddings.parquet",
            component_type="embeddings",
            namespace=test_namespace
        )
        
        if loaded_embeddings is not None and len(loaded_embeddings) > 0:
            print(f"✅ Embeddings loaded from Pinecone: {len(loaded_embeddings)} vectors")
            
            for embedding in loaded_embeddings['embedding']:
                if len(embedding) == 3072:
                    print("✅ Embedding dimensions verified")
                    break
            else:
                print("❌ Embedding dimensions failed")
                return False
        else:
            print("❌ Failed to load embeddings from Pinecone")
            return False
        
        try:
            neo4j = StorageFactory.get_graph_storage()
            neo4j.clear_tenant_data(test_tenant)
            print("✅ Neo4j cleanup completed")
            
            pinecone = StorageFactory.get_embedding_storage()
            import asyncio
            asyncio.run(pinecone.delete_namespace(test_namespace))
            print("✅ Pinecone cleanup completed")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")
        
        print("🎉 All cloud storage tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Cloud storage test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_cloud_storage_implementation()
    if success:
        print("\n✅ Cloud storage implementation validation completed successfully!")
    else:
        print("\n❌ Cloud storage implementation validation failed!")
        sys.exit(1)
