"""Integration tests for Transaction Manager with real adapters"""
import pytest
import asyncio
import numpy as np
from datetime import datetime

from NodeRAG.storage.neo4j_adapter import Neo4jAdapter
from NodeRAG.storage.pinecone_adapter import PineconeAdapter
from NodeRAG.storage.transactions.transaction_manager import TransactionManager
from NodeRAG.standards import EQMetadata
from NodeRAG.utils import NodeIDGenerator


def get_test_neo4j_config():
    """Get test Neo4j configuration"""
    return {
        "uri": "neo4j+s://b875880c.databases.neo4j.io",
        "user": "neo4j", 
        "password": "GjiwqwFNiYJnIIZwFcnInbdH2a61f40mJnPolkUd1u4",
        "database": "neo4j"
    }


def get_test_pinecone_config():
    """Get test Pinecone configuration"""
    return {
        "api_key": "pcsk_3EYvVL_Dxe7oL1cZsn1syqfNfHqAJVKKTMAcJPW3NnZjLmiNeD5aP7VVYuAzLzHWnsLccp",
        "index_name": "noderag"
    }


@pytest.fixture
async def neo4j_adapter():
    """Create real Neo4j adapter for integration testing"""
    config = get_test_neo4j_config()
    adapter = Neo4jAdapter(config)
    
    connected = await adapter.connect()
    if not connected:
        pytest.skip("Could not connect to Neo4j")
    
    await adapter.clear_tenant_data("txn_test_tenant")
    
    yield adapter
    
    await adapter.clear_tenant_data("txn_test_tenant")
    await adapter.close()


@pytest.fixture
def pinecone_adapter():
    """Create real Pinecone adapter for integration testing"""
    config = get_test_pinecone_config()
    adapter = PineconeAdapter(**config)
    
    connected = adapter.connect()
    if not connected:
        pytest.skip("Could not connect to Pinecone")
    
    asyncio.run(adapter.delete_namespace("txn_test_tenant"))
    
    yield adapter
    
    asyncio.run(adapter.delete_namespace("txn_test_tenant"))
    adapter.close()


@pytest.fixture
async def transaction_manager(neo4j_adapter, pinecone_adapter):
    """Create transaction manager with real adapters"""
    return TransactionManager(neo4j_adapter, pinecone_adapter)


@pytest.fixture
def sample_metadata():
    """Sample metadata for testing"""
    return EQMetadata(
        tenant_id="txn_test_tenant",
        interaction_id="int_txn_test_123",
        interaction_type="email",
        text="Transaction integration test content",
        account_id="acc_txn_test_456",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_txn_test_789",
        source_system="test_system"
    )


@pytest.mark.integration
@pytest.mark.asyncio
class TestTransactionIntegration:
    """Integration tests for transaction manager with real stores"""
    
    async def test_successful_node_and_embedding_creation(self, transaction_manager, sample_metadata):
        """Test successful creation of node and embedding"""
        node_id = NodeIDGenerator.generate_entity_id(
            entity_name="Transaction Test Entity",
            entity_type="PERSON",
            tenant_id=sample_metadata.tenant_id
        )
        
        embedding = np.random.randn(3072).tolist()
        
        success, error = await transaction_manager.add_node_with_embedding(
            node_id=node_id,
            node_type="entity",
            metadata=sample_metadata,
            embedding=embedding,
            node_properties={"name": "Transaction Test Entity", "entity_type": "PERSON"},
            vector_metadata={"node_type": "entity", "test": True}
        )
        
        assert success is True
        assert error is None
        
        neo4j_nodes = await transaction_manager.neo4j.get_nodes_by_tenant("txn_test_tenant")
        created_node = next((n for n in neo4j_nodes if n["node_id"] == node_id), None)
        assert created_node is not None
        assert created_node["tenant_id"] == "txn_test_tenant"
        assert created_node["name"] == "Transaction Test Entity"
        
        await asyncio.sleep(10)
        
        vector = await transaction_manager.pinecone.get_vector(node_id, sample_metadata.tenant_id)
        assert vector is not None
        assert vector["metadata"]["tenant_id"] == "txn_test_tenant"
        assert vector["metadata"]["node_type"] == "entity"
        
        print(f"✓ Successfully created node {node_id} in both stores")
    
    async def test_batch_transaction_success(self, transaction_manager, sample_metadata):
        """Test batch operations through transaction manager"""
        nodes_data = []
        
        for i in range(5):
            node_id = f"txn_batch_node_{i}"
            nodes_data.append({
                "node_id": node_id,
                "node_type": "semantic_unit",
                "metadata": sample_metadata,
                "embedding": np.random.randn(3072).tolist(),
                "node_properties": {"content": f"Batch content {i}", "index": i},
                "vector_metadata": {"node_type": "semantic_unit", "batch_index": i}
            })
        
        success_count, errors = await transaction_manager.add_nodes_batch_with_embeddings(nodes_data)
        
        assert success_count > 0
        assert len(errors) == 0
        
        neo4j_nodes = await transaction_manager.neo4j.get_nodes_by_tenant("txn_test_tenant")
        batch_nodes = [n for n in neo4j_nodes if n["node_id"].startswith("txn_batch_node_")]
        assert len(batch_nodes) == 5
        
        await asyncio.sleep(10)
        
        results = await transaction_manager.pinecone.search(
            query_embedding=np.random.randn(3072).tolist(),
            filters={"tenant_id": "txn_test_tenant"},
            top_k=10,
            namespace="txn_test_tenant"
        )
        
        batch_vectors = [r for r in results if r["id"].startswith("txn_batch_node_")]
        assert len(batch_vectors) >= 5
        
        print(f"✓ Successfully created {success_count} nodes in batch transaction")
    
    async def test_rollback_on_pinecone_failure(self, transaction_manager, sample_metadata):
        """Test rollback when Pinecone operation fails"""
        node_id = "txn_rollback_test_node"
        invalid_embedding = [0.1] * 100  # Wrong dimension, should be 3072
        
        success, error = await transaction_manager.add_node_with_embedding(
            node_id=node_id,
            node_type="entity",
            metadata=sample_metadata,
            embedding=invalid_embedding,
            node_properties={"name": "Rollback Test Entity"}
        )
        
        assert success is False
        assert error is not None
        
        neo4j_nodes = await transaction_manager.neo4j.get_nodes_by_tenant("txn_test_tenant")
        rollback_node = next((n for n in neo4j_nodes if n["node_id"] == node_id), None)
        assert rollback_node is None
        
        print(f"✓ Successfully rolled back failed transaction: {error}")
    
    async def test_consistency_after_multiple_transactions(self, transaction_manager, sample_metadata):
        """Test data consistency after multiple successful and failed transactions"""
        successful_ids = []
        failed_ids = []
        
        operations = [
            ("success_1", np.random.randn(3072).tolist(), True),
            ("success_2", np.random.randn(3072).tolist(), True),
            ("fail_1", [0.1] * 100, False),  # Wrong dimension
            ("success_3", np.random.randn(3072).tolist(), True),
            ("fail_2", [0.1] * 100, False),  # Wrong dimension
        ]
        
        for node_suffix, embedding, should_succeed in operations:
            node_id = f"txn_consistency_{node_suffix}"
            
            success, error = await transaction_manager.add_node_with_embedding(
                node_id=node_id,
                node_type="entity",
                metadata=sample_metadata,
                embedding=embedding,
                node_properties={"test": "consistency"}
            )
            
            if should_succeed:
                assert success is True
                successful_ids.append(node_id)
            else:
                assert success is False
                failed_ids.append(node_id)
        
        all_nodes = await transaction_manager.neo4j.get_nodes_by_tenant("txn_test_tenant")
        node_ids = {n["node_id"] for n in all_nodes}
        
        for success_id in successful_ids:
            assert success_id in node_ids
        
        for fail_id in failed_ids:
            assert fail_id not in node_ids
        
        await asyncio.sleep(10)
        
        results = await transaction_manager.pinecone.search(
            query_embedding=np.random.randn(3072).tolist(),
            filters={"tenant_id": "txn_test_tenant"},
            top_k=20,
            namespace="txn_test_tenant"
        )
        
        pinecone_ids = {r["id"] for r in results}
        
        for success_id in successful_ids:
            assert success_id in pinecone_ids
        
        for fail_id in failed_ids:
            assert fail_id not in pinecone_ids
        
        print(f"✓ Consistency verified: {len(successful_ids)} successful, {len(failed_ids)} failed")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
