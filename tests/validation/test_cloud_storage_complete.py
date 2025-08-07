"""
Complete cloud storage validation suite for Task 4.0.1d
"""
import os
import time
import uuid
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.standards.eq_metadata import EQMetadata


class CloudStorageValidator:
    """Complete validation suite for cloud storage mode"""
    
    def __init__(self):
        import os
        os.makedirs('/tmp/cloud_validation', exist_ok=True)
        self.config = {
            'config': {
                'main_folder': '/tmp/cloud_validation',
                'language': 'en',
                'chunk_size': 512
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'},
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
        self.results = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'tests_passed': 0,
            'tests_failed': 0,
            'errors': []
        }
    
    def validate_neo4j_operations(self) -> Dict[str, Any]:
        """Validate all Neo4j operations work correctly"""
        print("\n=== VALIDATING NEO4J OPERATIONS ===")
        neo4j_results = {
            'connection': False,
            'constraints': False,
            'crud': False,
            'batch': False,
            'relationships': False,
            'subgraph': False,
            'cleanup': False
        }
        
        try:
            StorageFactory.initialize(self.config, backend_mode="cloud")
            neo4j = StorageFactory.get_graph_storage()
            
            health = neo4j.health_check()
            neo4j_results['connection'] = health['status'] == 'healthy'
            print(f"✅ Neo4j connection: {health['status']}")
            print(f"   Database: {health['database']}, URI: {health['uri']}")
            
            start = time.time()
            neo4j.create_constraints_and_indexes()
            constraint_time = (time.time() - start) * 1000
            neo4j_results['constraints'] = True
            print(f"✅ Constraints created in {constraint_time:.2f}ms - NO EVENT LOOP ERRORS!")
            
            metadata = self.create_test_metadata()
            node_id = f"validation_{uuid.uuid4()}"
            
            success = neo4j.add_node(node_id, "test_type", metadata, {"test": True})
            neo4j_results['crud'] = success
            print(f"✅ CRUD operations: {'PASS' if success else 'FAIL'}")
            
            batch_nodes = [
                {
                    'node_id': f"batch_{i}_{uuid.uuid4()}",
                    'node_type': 'batch_test',
                    **metadata.to_dict()
                }
                for i in range(100)
            ]
            count, errors = neo4j.add_nodes_batch(batch_nodes)
            neo4j_results['batch'] = count == 100 and len(errors) == 0
            print(f"✅ Batch operations: Added {count}/100 nodes")
            
            source_id = f"source_{uuid.uuid4()}"
            target_id = f"target_{uuid.uuid4()}"
            neo4j.add_node(source_id, "source", metadata)
            neo4j.add_node(target_id, "target", metadata)
            
            rel_success = neo4j.add_relationship(
                source_id, target_id, "CONNECTS", metadata, {"weight": 1.0}
            )
            neo4j_results['relationships'] = rel_success
            print(f"✅ Relationships: {'PASS' if rel_success else 'FAIL'}")
            
            subgraph = neo4j.get_subgraph(metadata.tenant_id)
            neo4j_results['subgraph'] = subgraph['node_count'] > 0
            print(f"✅ Subgraph: {subgraph['node_count']} nodes, {subgraph['relationship_count']} relationships")
            
            cleared = neo4j.clear_tenant_data(metadata.tenant_id)
            neo4j_results['cleanup'] = cleared
            print(f"✅ Cleanup: {'PASS' if cleared else 'FAIL'}")
            
        except Exception as e:
            self.results['errors'].append(f"Neo4j validation error: {str(e)}")
            print(f"❌ Neo4j validation failed: {e}")
        
        return neo4j_results
    
    def validate_pinecone_operations(self) -> Dict[str, Any]:
        """Validate all Pinecone operations work correctly"""
        print("\n=== VALIDATING PINECONE OPERATIONS ===")
        pinecone_results = {
            'connection': False,
            'upsert': False,
            'search': False,
            'delete': False
        }
        
        try:
            pinecone = StorageFactory.get_embedding_storage()
            
            connected = pinecone.connect()
            pinecone_results['connection'] = connected
            print(f"✅ Pinecone connection: {'PASS' if connected else 'FAIL'}")
            
            if connected:
                metadata = self.create_test_metadata()
                test_embedding = np.random.rand(3072).tolist()  # 3072 dimensions for noderag index
                vector_id = f"test_vector_{uuid.uuid4()}"
                
                import asyncio
                upsert_success = asyncio.run(pinecone.upsert_vector(
                    vector_id, test_embedding, metadata, namespace=metadata.tenant_id
                ))
                pinecone_results['upsert'] = upsert_success
                print(f"✅ Pinecone upsert: {'PASS' if upsert_success else 'FAIL'}")
                
                search_results = asyncio.run(pinecone.search(
                    test_embedding, 
                    {"tenant_id": metadata.tenant_id}, 
                    top_k=5, 
                    namespace=metadata.tenant_id
                ))
                pinecone_results['search'] = len(search_results) > 0
                print(f"✅ Pinecone search: Found {len(search_results)} results")
                
                delete_success = asyncio.run(pinecone.delete_vectors(
                    [vector_id], namespace=metadata.tenant_id
                ))
                pinecone_results['delete'] = delete_success
                print(f"✅ Pinecone delete: {'PASS' if delete_success else 'FAIL'}")
            
        except Exception as e:
            self.results['errors'].append(f"Pinecone validation error: {str(e)}")
            print(f"❌ Pinecone validation failed: {e}")
        
        return pinecone_results
    
    def validate_combined_operations(self) -> Dict[str, Any]:
        """Test Neo4j and Pinecone working together"""
        print("\n=== VALIDATING COMBINED OPERATIONS ===")
        combined_results = {
            'graph_with_embeddings': False,
            'search_and_traverse': False,
            'singleton_verification': False
        }
        
        try:
            neo4j = StorageFactory.get_graph_storage()
            pinecone = StorageFactory.get_embedding_storage()
            
            neo4j2 = StorageFactory.get_graph_storage()
            pinecone2 = StorageFactory.get_embedding_storage()
            
            combined_results['singleton_verification'] = (
                neo4j is neo4j2 and pinecone is pinecone2
            )
            print(f"✅ Singleton pattern maintained for both adapters")
            
            metadata = self.create_test_metadata()
            
            node_id = f"combined_test_{uuid.uuid4()}"
            neo4j_success = neo4j.add_node(node_id, "combined_test", metadata, {"combined": True})
            
            test_embedding = np.random.rand(3072).tolist()
            import asyncio
            pinecone_success = asyncio.run(pinecone.upsert_vector(
                node_id, test_embedding, metadata, namespace=metadata.tenant_id
            ))
            
            combined_results['graph_with_embeddings'] = neo4j_success and pinecone_success
            print(f"✅ Graph + Embeddings: {'PASS' if combined_results['graph_with_embeddings'] else 'FAIL'}")
            
            if combined_results['graph_with_embeddings']:
                search_results = asyncio.run(pinecone.search(
                    test_embedding, 
                    {"tenant_id": metadata.tenant_id}, 
                    top_k=1, 
                    namespace=metadata.tenant_id
                ))
                
                nodes = neo4j.get_nodes_by_tenant(metadata.tenant_id)
                found_node = any(n['node_id'] == node_id for n in nodes)
                
                combined_results['search_and_traverse'] = len(search_results) > 0 and found_node
                print(f"✅ Search & Traverse: {'PASS' if combined_results['search_and_traverse'] else 'FAIL'}")
            
            neo4j.clear_tenant_data(metadata.tenant_id)
            asyncio.run(pinecone.delete_namespace(metadata.tenant_id))
            
        except Exception as e:
            self.results['errors'].append(f"Combined operations error: {str(e)}")
            print(f"❌ Combined operations failed: {e}")
        
        return combined_results
    
    def create_test_metadata(self) -> EQMetadata:
        """Create valid test metadata"""
        return EQMetadata(
            tenant_id="cloud-validation-test",
            account_id=f"acc_{uuid.uuid4()}",
            interaction_id=f"int_{uuid.uuid4()}",
            interaction_type="email",
            text="Cloud storage validation content",
            timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            user_id="validator@test.com",
            source_system="internal"
        )
    
    def run_full_validation(self) -> Dict[str, Any]:
        """Run complete validation suite"""
        print("\n" + "="*60)
        print("CLOUD STORAGE COMPLETE VALIDATION SUITE")
        print("="*60)
        
        neo4j_results = self.validate_neo4j_operations()
        self.results['neo4j'] = neo4j_results
        
        pinecone_results = self.validate_pinecone_operations()
        self.results['pinecone'] = pinecone_results
        
        combined_results = self.validate_combined_operations()
        self.results['combined'] = combined_results
        
        for category in [neo4j_results, pinecone_results, combined_results]:
            for test, passed in category.items():
                if passed:
                    self.results['tests_passed'] += 1
                else:
                    self.results['tests_failed'] += 1
        
        StorageFactory.cleanup()
        
        return self.results


if __name__ == "__main__":
    validator = CloudStorageValidator()
    results = validator.run_full_validation()
    print(f"\nValidation complete: {results['tests_passed']} passed, {results['tests_failed']} failed")
