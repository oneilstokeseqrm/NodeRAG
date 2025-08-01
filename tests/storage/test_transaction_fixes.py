"""Test transaction manager fixes for error handling"""
import pytest
import pytest_asyncio
import asyncio
import numpy as np
from datetime import datetime, timezone
import uuid
import os

from NodeRAG.storage import Neo4jAdapter, PineconeAdapter, TransactionManager
from NodeRAG.standards import EQMetadata


@pytest_asyncio.fixture
async def adapters():
    """Create real adapters for testing"""
    neo4j = Neo4jAdapter({
        "uri": os.getenv("NEO4J_URI", "neo4j+s://b875880c.databases.neo4j.io"),
        "user": os.getenv("NEO4J_USERNAME", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "GjiwqwFNiYJnIIZwFcnInbdH2a61f40mJnPolkUd1u4")
    })
    await neo4j.connect()
    
    pinecone = PineconeAdapter(
        api_key=os.getenv("PINECONE_API_KEY", "pcsk_3EYvVL_Dxe7oL1cZsn1syqfNfHqAJVKKTMAcJPW3NnZjLmiNeD5aP7VVYuAzLzHWnsLccp"),
        index_name=os.getenv("PINECONE_INDEX_NAME", "noderag")
    )
    pinecone.connect()
    
    await neo4j.clear_tenant_data("fix_test_tenant")
    await pinecone.delete_namespace("fix_test_tenant")
    
    yield neo4j, pinecone
    
    await neo4j.clear_tenant_data("fix_test_tenant")
    await pinecone.delete_namespace("fix_test_tenant")
    await neo4j.close()
    pinecone.close()


@pytest.mark.asyncio
async def test_dimension_mismatch_triggers_rollback(adapters):
    """Test that dimension mismatch now properly triggers rollback"""
    neo4j, pinecone = adapters
    tm = TransactionManager(neo4j, pinecone)
    
    metadata = EQMetadata(
        tenant_id="fix_test_tenant",
        interaction_id=f"int_{uuid.uuid4()}",
        interaction_type="email",
        text="Test rollback fix",
        account_id=f"acc_{uuid.uuid4()}",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id=f"usr_{uuid.uuid4()}",
        source_system="internal"
    )
    
    success, error = await tm.add_node_with_embedding(
        node_id="dimension_fail_test",
        node_type="entity",
        metadata=metadata,
        embedding=[0.1] * 100,  # Wrong dimension
        node_properties={"test": "should_rollback"}
    )
    
    assert success is False
    assert "dimension" in error.lower() or "3072" in error
    
    nodes = await neo4j.get_nodes_by_tenant("fix_test_tenant")
    node_ids = [n["node_id"] for n in nodes]
    assert "dimension_fail_test" not in node_ids
    
    print("✓ Dimension mismatch now properly triggers rollback")


@pytest.mark.asyncio
async def test_batch_operation_with_failures(adapters):
    """Test batch operations handle mixed success/failure correctly"""
    neo4j, pinecone = adapters
    tm = TransactionManager(neo4j, pinecone)
    
    metadata = EQMetadata(
        tenant_id="fix_test_tenant",
        interaction_id=f"int_{uuid.uuid4()}",
        interaction_type="email",
        text="Batch test",
        account_id=f"acc_{uuid.uuid4()}",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id=f"usr_{uuid.uuid4()}",
        source_system="internal"
    )
    
    nodes = [
        {
            "node_id": "batch_good_1",
            "node_type": "entity",
            "metadata": metadata,
            "embedding": np.random.randn(3072).tolist(),
            "node_properties": {"valid": True}
        },
        {
            "node_id": "batch_bad_dimension",
            "node_type": "entity",
            "metadata": metadata,
            "embedding": [0.1] * 100,  # Wrong dimension
            "node_properties": {"valid": False}
        },
        {
            "node_id": "batch_good_2",
            "node_type": "entity",
            "metadata": metadata,
            "embedding": np.random.randn(3072).tolist(),
            "node_properties": {"valid": True}
        }
    ]
    
    success_count, errors = await tm.add_nodes_batch_with_embeddings(nodes)
    
    assert success_count == 2  # Two valid nodes
    assert len(errors) == 1  # One failed node
    assert "batch_bad_dimension" in str(errors)
    
    print(f"✓ Batch operation handled correctly: {success_count} success, {len(errors)} errors")


@pytest.mark.asyncio
async def test_data_consistency_maintained(adapters):
    """Test that data consistency is maintained between stores"""
    neo4j, pinecone = adapters
    tm = TransactionManager(neo4j, pinecone)
    
    metadata = EQMetadata(
        tenant_id="fix_test_tenant",
        interaction_id=f"int_{uuid.uuid4()}",
        interaction_type="email",
        text="Consistency test",
        account_id=f"acc_{uuid.uuid4()}",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id=f"usr_{uuid.uuid4()}",
        source_system="internal"
    )
    
    for i in range(3):
        success, error = await tm.add_node_with_embedding(
            node_id=f"consistency_test_{i}",
            node_type="entity",
            metadata=metadata,
            embedding=np.random.randn(3072).tolist(),
            node_properties={"index": i}
        )
        assert success is True
    
    success, error = await tm.add_node_with_embedding(
        node_id="consistency_fail",
        node_type="entity",
        metadata=metadata,
        embedding=[0.1] * 50,  # Wrong dimension
        node_properties={"should": "fail"}
    )
    assert success is False
    
    neo4j_nodes = await neo4j.get_nodes_by_tenant("fix_test_tenant")
    neo4j_ids = {n["node_id"] for n in neo4j_nodes}
    
    await asyncio.sleep(5)
    
    pinecone_results = await pinecone.search(
        query_embedding=np.random.randn(3072).tolist(),
        filters={"tenant_id": "fix_test_tenant"},
        top_k=10,
        namespace="fix_test_tenant"
    )
    pinecone_ids = {r["id"] for r in pinecone_results}
    
    assert neo4j_ids == pinecone_ids
    assert "consistency_fail" not in neo4j_ids
    assert "consistency_fail" not in pinecone_ids
    
    print(f"✓ Data consistency maintained: {len(neo4j_ids)} nodes in both stores")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
