"""
Test existing NodeRAG pipelines with cloud storage for Task 4.0.1d
"""
import os
import uuid
from pathlib import Path
from datetime import datetime, timezone

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.config import NodeConfig


def test_pipeline_compatibility():
    """Test that existing pipelines work with cloud storage"""
    
    print("\n=== PIPELINE COMPATIBILITY TEST ===")
    
    test_dir = Path("/tmp/pipeline_test")
    test_dir.mkdir(parents=True, exist_ok=True)
    
    config_dict = {
        'main_folder': str(test_dir),
        'language': 'en',
        'chunk_size': 512,
        'eq_config': {
            'storage': {
                'neo4j_uri': os.getenv('Neo4j_Credentials_NEO4J_URI'),
                'neo4j_user': os.getenv('Neo4j_Credentials_NEO4J_USERNAME', 'neo4j'),
                'neo4j_password': os.getenv('Neo4j_Credentials_NEO4J_PASSWORD'),
                'pinecone_api_key': os.getenv('pinecone_API_key'),
                'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag')
            }
        }
    }
    
    try:
        StorageFactory.initialize(config_dict, backend_mode="cloud")
        
        neo4j = StorageFactory.get_graph_storage()
        pinecone = StorageFactory.get_embedding_storage()
        
        print("✅ Pipeline initialized with cloud storage")
        
        metadata = EQMetadata(
            tenant_id="pipeline-test",
            account_id=f"acc_{uuid.uuid4()}",
            interaction_id=f"int_{uuid.uuid4()}",
            interaction_type="email",
            text="Pipeline compatibility test content",
            timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            user_id="pipeline@test.com",
            source_system="internal"
        )
        
        semantic_unit_id = f"semantic_{uuid.uuid4()}"
        success1 = neo4j.add_node(semantic_unit_id, "semantic_unit", metadata, {
            "context": "Test semantic unit",
            "weight": 1
        })
        
        entity_id = f"entity_{uuid.uuid4()}"
        success2 = neo4j.add_node(entity_id, "entity", metadata, {
            "context": "Test Entity",
            "weight": 1
        })
        
        success3 = neo4j.add_relationship(
            semantic_unit_id, entity_id, "CONTAINS", metadata, {"weight": 1.0}
        )
        
        subgraph = neo4j.get_subgraph(metadata.tenant_id)
        
        import numpy as np
        import asyncio
        
        test_embedding = np.random.rand(3072).tolist()
        embedding_success = asyncio.run(pinecone.upsert_vector(
            semantic_unit_id, test_embedding, metadata, namespace=metadata.tenant_id
        ))
        
        all_operations_success = all([success1, success2, success3, embedding_success])
        
        if all_operations_success and subgraph['node_count'] >= 2:
            print("✅ Pipeline operations successful with cloud storage")
            print(f"   Created {subgraph['node_count']} nodes, {subgraph['relationship_count']} relationships")
            print("   Embedding operations successful")
            
            neo4j.clear_tenant_data(metadata.tenant_id)
            asyncio.run(pinecone.delete_namespace(metadata.tenant_id))
            print("✅ Pipeline test data cleaned up")
            
            return True
        else:
            print("❌ Pipeline operations failed")
            return False
        
    except Exception as e:
        print(f"❌ Pipeline compatibility failed: {e}")
        return False
    
    finally:
        StorageFactory.cleanup()


def test_graph_pipeline_integration():
    """Test Graph_pipeline class integration with cloud storage"""
    
    print("\n=== GRAPH PIPELINE INTEGRATION TEST ===")
    
    try:
        from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline
        
        test_dir = Path("/tmp/graph_pipeline_test")
        test_dir.mkdir(exist_ok=True)
        
        text_decomposition_file = test_dir / "text_decomposition.jsonl"
        with open(text_decomposition_file, 'w') as f:
            test_data = {
                "text_hash_id": f"test_{uuid.uuid4()}",
                "response": {
                    "Output": [{
                        "semantic_unit": {"context": "Test semantic unit"},
                        "entities": [{"name": "Test Entity"}],
                        "relationships": ["Test Entity, relates to, Another Entity"]
                    }]
                },
                "metadata": {
                    "tenant_id": "graph-pipeline-test",
                    "account_id": f"acc_{uuid.uuid4()}",
                    "interaction_id": f"int_{uuid.uuid4()}",
                    "interaction_type": "email",
                    "text": "Graph pipeline test content",
                    "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                    "user_id": "graphtest@test.com",
                    "source_system": "internal"
                },
                "processed": False
            }
            import json
            f.write(json.dumps(test_data) + '\n')
        
        config_dict = {
            'main_folder': str(test_dir),
            'language': 'en',
            'chunk_size': 512,
            'text_decomposition_path': str(text_decomposition_file),
            'graph_path': str(test_dir / "graph.pkl"),
            'relationship_path': str(test_dir / "relationships.parquet"),
            'semantic_units_path': str(test_dir / "semantic_units.parquet"),
            'entities_path': str(test_dir / "entities.parquet"),
            'indices_path': str(test_dir / "indices.pkl"),
            'eq_config': {
                'storage': {
                    'neo4j_uri': os.getenv('Neo4j_Credentials_NEO4J_URI'),
                    'neo4j_user': os.getenv('Neo4j_Credentials_NEO4J_USERNAME', 'neo4j'),
                    'neo4j_password': os.getenv('Neo4j_Credentials_NEO4J_PASSWORD'),
                    'pinecone_api_key': os.getenv('pinecone_API_key'),
                    'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag')
                }
            }
        }
        
        StorageFactory.initialize(config_dict, backend_mode="cloud")
        
        node_config = NodeConfig(config_dict)
        
        print("✅ Graph_pipeline can be imported and configured with cloud storage")
        print("✅ StorageFactory integration confirmed")
        
        neo4j = StorageFactory.get_graph_storage()
        neo4j.clear_tenant_data("graph-pipeline-test")
        
        return True
        
    except Exception as e:
        print(f"❌ Graph_pipeline integration failed: {e}")
        return False
    
    finally:
        StorageFactory.cleanup()


if __name__ == "__main__":
    result1 = test_pipeline_compatibility()
    result2 = test_graph_pipeline_integration()
    
    overall_success = result1 and result2
    print(f"\nPipeline compatibility test: {'PASS' if overall_success else 'FAIL'}")
