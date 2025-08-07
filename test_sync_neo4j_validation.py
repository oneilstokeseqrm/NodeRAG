"""
Validation test for synchronous Neo4j driver
"""
import pytest
import uuid
import os
import time
from datetime import datetime, timezone

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.standards.eq_metadata import EQMetadata


def test_sync_neo4j_operations():
    """Test that synchronous Neo4j operations work correctly"""
    
    print("\n" + "="*50)
    print("SYNCHRONOUS NEO4J DRIVER VALIDATION TEST")
    print("="*50)
    
    config = {
        'config': {
            'main_folder': '/tmp/sync_test',
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
    
    print("\n1. Initializing StorageFactory with cloud mode...")
    StorageFactory.initialize(config, backend_mode="cloud")
    
    print("2. Getting Neo4j adapter from StorageFactory...")
    neo4j = StorageFactory.get_graph_storage()
    
    print("\n3. Testing health check...")
    start = time.time()
    health = neo4j.health_check()
    health_time = (time.time() - start) * 1000
    assert health['status'] == 'healthy', f"Neo4j unhealthy: {health}"
    print(f"   ✅ Neo4j health check passed in {health_time:.2f}ms")
    print(f"   Database: {health['database']}, URI: {health['uri']}")
    
    print("\n4. Testing constraints and indexes creation...")
    start = time.time()
    try:
        neo4j.create_constraints_and_indexes()
        constraint_time = (time.time() - start) * 1000
        print(f"   ✅ Constraints and indexes created successfully in {constraint_time:.2f}ms")
        print("   NO EVENT LOOP ERRORS!")
    except Exception as e:
        pytest.fail(f"Failed to create constraints: {e}")
    
    print("\n5. Testing single node operations...")
    metadata = EQMetadata(
        tenant_id="sync-test",
        account_id=f"acc_{uuid.uuid4()}",
        interaction_id=f"int_{uuid.uuid4()}",
        interaction_type="email",
        text="Sync test content",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id="sync-test@example.com",
        source_system="internal"
    )
    
    node_id = f"sync_test_{uuid.uuid4()}"
    start = time.time()
    success = neo4j.add_node(
        node_id=node_id,
        node_type="test_entity",
        metadata=metadata,
        properties={"sync_test": True}
    )
    add_time = (time.time() - start) * 1000
    assert success, "Failed to add node"
    print(f"   ✅ Node {node_id[:20]}... added in {add_time:.2f}ms")
    
    print("\n6. Testing node retrieval...")
    start = time.time()
    nodes = neo4j.get_nodes_by_tenant(metadata.tenant_id)
    retrieve_time = (time.time() - start) * 1000
    assert len(nodes) > 0, "No nodes retrieved"
    assert any(n['node_id'] == node_id for n in nodes), "Created node not found"
    print(f"   ✅ Retrieved {len(nodes)} nodes in {retrieve_time:.2f}ms")
    
    print("\n7. Testing batch operations...")
    batch_nodes = [
        {
            'node_id': f"batch_{i}_{uuid.uuid4()}",
            'node_type': 'batch_test',
            **metadata.to_dict()
        }
        for i in range(10)
    ]
    
    start = time.time()
    count, errors = neo4j.add_nodes_batch(batch_nodes)
    batch_time = (time.time() - start) * 1000
    assert count > 0, f"Batch add failed: {errors}"
    assert len(errors) == 0, f"Batch errors: {errors}"
    print(f"   ✅ Batch added {count} nodes in {batch_time:.2f}ms")
    
    print("\n8. Testing relationship operations...")
    source_id = f"rel_source_{uuid.uuid4()}"
    target_id = f"rel_target_{uuid.uuid4()}"
    
    neo4j.add_node(source_id, "source_type", metadata)
    neo4j.add_node(target_id, "target_type", metadata)
    
    start = time.time()
    rel_success = neo4j.add_relationship(
        source_id=source_id,
        target_id=target_id,
        relationship_type="CONNECTS_TO",
        metadata=metadata,
        properties={"weight": 1.0}
    )
    rel_time = (time.time() - start) * 1000
    assert rel_success, "Failed to add relationship"
    print(f"   ✅ Relationship added in {rel_time:.2f}ms")
    
    print("\n9. Testing subgraph retrieval...")
    start = time.time()
    subgraph = neo4j.get_subgraph(metadata.tenant_id)
    subgraph_time = (time.time() - start) * 1000
    assert subgraph['node_count'] > 0, "No nodes in subgraph"
    print(f"   ✅ Retrieved subgraph with {subgraph['node_count']} nodes, {subgraph['relationship_count']} relationships in {subgraph_time:.2f}ms")
    
    print("\n10. Testing statistics...")
    stats = neo4j.get_statistics()
    assert 'total_nodes' in stats, "Statistics missing total_nodes"
    print(f"   ✅ Database stats: {stats['total_nodes']} total nodes, {stats['total_relationships']} total relationships")
    
    print("\n11. Cleaning up test data...")
    start = time.time()
    cleared = neo4j.clear_tenant_data(metadata.tenant_id)
    cleanup_time = (time.time() - start) * 1000
    assert cleared, "Failed to clear tenant data"
    print(f"   ✅ Cleaned up test data in {cleanup_time:.2f}ms")
    
    print("\n12. Verifying singleton pattern...")
    neo4j2 = StorageFactory.get_graph_storage()
    assert neo4j is neo4j2, "Singleton pattern broken"
    print("   ✅ Singleton pattern maintained")
    
    print("\n13. Final StorageFactory cleanup...")
    StorageFactory.cleanup()
    print("   ✅ StorageFactory cleaned up")
    
    print("\n" + "="*50)
    print("✅ ALL SYNCHRONOUS NEO4J OPERATIONS COMPLETED SUCCESSFULLY!")
    print("NO EVENT LOOP CONFLICTS DETECTED!")
    print("="*50 + "\n")
    
    return True


if __name__ == "__main__":
    test_sync_neo4j_operations()
