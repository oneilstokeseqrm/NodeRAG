"""Verify data consistency after fixes"""
import asyncio
import uuid
from datetime import datetime, timezone
import numpy as np

from NodeRAG.storage import Neo4jAdapter, PineconeAdapter, TransactionManager
from NodeRAG.standards import EQMetadata


async def verify_consistency():
    """Test that failed operations don't create inconsistent state"""
    print("Verifying data consistency after transaction manager fixes...\n")
    
    neo4j = Neo4jAdapter({
        "uri": "neo4j+s://b875880c.databases.neo4j.io",
        "user": "neo4j",
        "password": "GjiwqwFNiYJnIIZwFcnInbdH2a61f40mJnPolkUd1u4"
    })
    await neo4j.connect()
    
    pinecone = PineconeAdapter(
        api_key="pcsk_3EYvVL_Dxe7oL1cZsn1syqfNfHqAJVKKTMAcJPW3NnZjLmiNeD5aP7VVYuAzLzHWnsLccp",
        index_name="noderag"
    )
    pinecone.connect()
    
    tm = TransactionManager(neo4j, pinecone)
    
    await neo4j.clear_tenant_data("consistency_test")
    await pinecone.delete_namespace("consistency_test")
    
    metadata = EQMetadata(
        tenant_id="consistency_test",
        interaction_id=f"int_{uuid.uuid4()}",
        interaction_type="email",
        text="Consistency verification",
        account_id=f"acc_{uuid.uuid4()}",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id=f"usr_{uuid.uuid4()}",
        source_system="internal"
    )
    
    print("Test 1: Dimension mismatch rollback")
    success, error = await tm.add_node_with_embedding(
        node_id="should_not_exist",
        node_type="entity",
        metadata=metadata,
        embedding=[0.1] * 100,  # Wrong dimension
        node_properties={"test": "rollback"}
    )
    
    print(f"  Result: success={success}, error={error}")
    assert success is False, "Expected failure but got success!"
    assert "dimension" in str(error).lower(), f"Expected dimension error but got: {error}"
    
    nodes = await neo4j.get_nodes_by_tenant("consistency_test")
    assert len(nodes) == 0, f"Expected 0 nodes but found {len(nodes)}"
    print("  ✓ Rollback worked - no nodes in Neo4j")
    
    print("\nTest 2: Valid operations")
    for i in range(3):
        success, error = await tm.add_node_with_embedding(
            node_id=f"valid_node_{i}",
            node_type="entity",
            metadata=metadata,
            embedding=np.random.randn(3072).tolist(),
            node_properties={"index": i}
        )
        assert success is True, f"Valid operation failed: {error}"
    print("  ✓ Added 3 valid nodes")
    
    print("\nTest 3: Verifying data consistency")
    neo4j_nodes = await neo4j.get_nodes_by_tenant("consistency_test")
    print(f"  Neo4j nodes: {len(neo4j_nodes)}")
    
    await asyncio.sleep(10)  # Wait for Pinecone indexing
    
    pinecone_results = await pinecone.search(
        query_embedding=np.random.randn(3072).tolist(),
        filters={"tenant_id": "consistency_test"},
        top_k=10,
        namespace="consistency_test"
    )
    print(f"  Pinecone vectors: {len(pinecone_results)}")
    
    neo4j_ids = {n["node_id"] for n in neo4j_nodes}
    pinecone_ids = {r["id"] for r in pinecone_results}
    
    if neo4j_ids == pinecone_ids:
        print("  ✓ Data consistency verified - same IDs in both stores")
    else:
        print(f"  ✗ Inconsistency detected!")
        print(f"    Neo4j only: {neo4j_ids - pinecone_ids}")
        print(f"    Pinecone only: {pinecone_ids - neo4j_ids}")
        
    await neo4j.clear_tenant_data("consistency_test")
    await pinecone.delete_namespace("consistency_test")
    await neo4j.close()
    pinecone.close()
    
    print("\n✅ Consistency verification complete!")
    return len(neo4j_ids) == len(pinecone_ids) == 3


if __name__ == "__main__":
    success = asyncio.run(verify_consistency())
    exit(0 if success else 1)
