"""Unit tests for Transaction Manager"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock
from datetime import datetime

from NodeRAG.storage.transactions.transaction_manager import (
    TransactionManager, Transaction, TransactionOperation, TransactionState
)
from NodeRAG.storage.transactions.test_utils import MockFailingAdapter
from NodeRAG.standards import EQMetadata


@pytest.fixture
def sample_metadata():
    """Sample metadata for testing"""
    return EQMetadata(
        tenant_id="test_tenant",
        interaction_id="int_test_123",
        interaction_type="email",
        text="Test transaction content",
        account_id="acc_test_456",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_test_789",
        source_system="test_system"
    )


@pytest.fixture
def mock_neo4j():
    """Mock Neo4j adapter"""
    adapter = AsyncMock()
    adapter.add_node = AsyncMock(return_value=True)
    adapter.delete_node = AsyncMock(return_value=True)
    adapter.add_nodes_batch = AsyncMock(return_value=10)
    adapter.health_check = AsyncMock(return_value={"status": "healthy"})
    return adapter


@pytest.fixture
def mock_pinecone():
    """Mock Pinecone adapter"""
    adapter = AsyncMock()
    adapter.upsert_vector = AsyncMock(return_value=True)
    adapter.delete_vectors = AsyncMock(return_value=True)
    adapter.upsert_vectors_batch = AsyncMock(return_value=(10, []))
    adapter.get_stats = AsyncMock(return_value={"total_vectors": 100, "dimension": 3072})
    return adapter


@pytest.fixture
def transaction_manager(mock_neo4j, mock_pinecone):
    """Create transaction manager with mocks"""
    return TransactionManager(mock_neo4j, mock_pinecone)


class TestTransactionManager:
    """Test transaction manager functionality"""
    
    def test_begin_transaction(self, transaction_manager):
        """Test transaction creation"""
        transaction = transaction_manager.begin_transaction("test_tenant")
        
        assert transaction.tenant_id == "test_tenant"
        assert transaction.state == TransactionState.INITIATED
        assert transaction.id.startswith("txn_")
        assert len(transaction.operations) == 0
        assert transaction.id in transaction_manager.active_transactions
    
    @pytest.mark.asyncio
    async def test_add_node_with_embedding_success(self, transaction_manager, sample_metadata):
        """Test successful node + embedding addition"""
        node_id = "test_node_123"
        embedding = [0.1] * 3072
        
        success, error = await transaction_manager.add_node_with_embedding(
            node_id=node_id,
            node_type="entity",
            metadata=sample_metadata,
            embedding=embedding,
            node_properties={"name": "Test Entity"},
            vector_metadata={"node_type": "entity"}
        )
        
        assert success is True
        assert error is None
        
        transaction_manager.neo4j.add_node.assert_called_once()
        transaction_manager.pinecone.upsert_vector.assert_called_once()
        
        assert len(transaction_manager.active_transactions) == 0
    
    @pytest.mark.asyncio
    async def test_add_node_with_embedding_neo4j_failure(self, transaction_manager, sample_metadata):
        """Test rollback when Neo4j fails"""
        transaction_manager.neo4j.add_node = AsyncMock(side_effect=Exception("Neo4j error"))
        
        node_id = "test_node_fail"
        embedding = [0.1] * 3072
        
        success, error = await transaction_manager.add_node_with_embedding(
            node_id=node_id,
            node_type="entity",
            metadata=sample_metadata,
            embedding=embedding
        )
        
        assert success is False
        assert "Neo4j error" in error
        
        transaction_manager.pinecone.upsert_vector.assert_not_called()
        
        transaction_manager.neo4j.delete_node.assert_not_called()  # Nothing to rollback
    
    @pytest.mark.asyncio
    async def test_add_node_with_embedding_pinecone_failure(self, transaction_manager, sample_metadata):
        """Test rollback when Pinecone fails after Neo4j succeeds"""
        transaction_manager.pinecone.upsert_vector = AsyncMock(side_effect=Exception("Pinecone error"))
        
        node_id = "test_node_fail_2"
        embedding = [0.1] * 3072
        
        success, error = await transaction_manager.add_node_with_embedding(
            node_id=node_id,
            node_type="entity",
            metadata=sample_metadata,
            embedding=embedding
        )
        
        assert success is False
        assert "Pinecone error" in error
        
        transaction_manager.neo4j.add_node.assert_called_once()
        
        transaction_manager.neo4j.delete_node.assert_called_once_with(node_id)
    
    @pytest.mark.asyncio
    async def test_batch_operations_success(self, transaction_manager, sample_metadata):
        """Test batch operations with transaction"""
        nodes_data = []
        for i in range(5):
            nodes_data.append({
                "node_id": f"batch_node_{i}",
                "node_type": "entity",
                "metadata": sample_metadata,
                "embedding": [0.1] * 3072,
                "node_properties": {"index": i},
                "vector_metadata": {"batch": True}
            })
        
        success_count, errors = await transaction_manager.add_nodes_batch_with_embeddings(nodes_data)
        
        assert success_count == 10  # Mocked return value
        assert len(errors) == 0
        
        transaction_manager.neo4j.add_nodes_batch.assert_called_once()
        transaction_manager.pinecone.upsert_vectors_batch.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_transaction_logging(self, transaction_manager, sample_metadata):
        """Test transaction event logging"""
        transaction_manager.transaction_log = []
        
        await transaction_manager.add_node_with_embedding(
            node_id="log_test",
            node_type="entity",
            metadata=sample_metadata,
            embedding=[0.1] * 3072
        )
        
        log = transaction_manager.get_transaction_log()
        assert len(log) >= 2  # BEGIN and COMMIT
        
        begin_event = next(e for e in log if e["event"] == "BEGIN")
        assert begin_event["details"]["tenant_id"] == "test_tenant"
        
        commit_event = next(e for e in log if e["event"] == "COMMIT")
        assert commit_event["details"]["operations"] == 2
    
    @pytest.mark.asyncio
    async def test_concurrent_transactions(self, transaction_manager, sample_metadata):
        """Test multiple concurrent transactions"""
        async def create_node(i):
            return await transaction_manager.add_node_with_embedding(
                node_id=f"concurrent_{i}",
                node_type="entity",
                metadata=sample_metadata,
                embedding=[0.1] * 3072
            )
        
        results = await asyncio.gather(*[create_node(i) for i in range(5)])
        
        for success, error in results:
            assert success is True
            assert error is None
        
        assert transaction_manager.neo4j.add_node.call_count == 5
        assert transaction_manager.pinecone.upsert_vector.call_count == 5
    
    @pytest.mark.asyncio
    async def test_rollback_with_mock_adapters(self):
        """Test rollback with controlled failure scenarios"""
        neo4j = MockFailingAdapter()
        pinecone = MockFailingAdapter(fail_on_operation="upsert_vector")
        tm = TransactionManager(neo4j, pinecone)
        
        metadata = EQMetadata(
            tenant_id="test_tenant",
            interaction_id="int_test_123",
            interaction_type="email",
            text="Test",
            account_id="acc_test_456",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_test_789",
            source_system="test"
        )
        
        success, error = await tm.add_node_with_embedding(
            node_id="rollback_test",
            node_type="entity",
            metadata=metadata,
            embedding=[0.1] * 3072
        )
        
        assert success is False
        assert "Mock upsert_vector failure" in error
        
        assert ("add_node", ("rollback_test", "entity", metadata.to_dict()), {"properties": {}}) in neo4j.operations_log
        assert ("delete_node", ("rollback_test",), {}) in neo4j.operations_log  # Rollback
        
        assert len([op for op in pinecone.operations_log if op[0] == "upsert_vector"]) == 1
        assert len([op for op in pinecone.operations_log if op[0] == "delete_vectors"]) == 0
    
    @pytest.mark.asyncio
    async def test_health_check(self, transaction_manager):
        """Test health check functionality"""
        health = await transaction_manager.health_check()
        
        assert health["transaction_manager"]["status"] == "healthy"
        assert health["transaction_manager"]["active_transactions"] == 0
        assert health["neo4j"]["status"] == "healthy"
        assert health["pinecone"]["status"] == "healthy"
        assert health["pinecone"]["total_vectors"] == 100
    
    def test_transaction_state_transitions(self):
        """Test transaction state machine"""
        transaction = Transaction("test_txn", "test_tenant")
        
        assert transaction.state == TransactionState.INITIATED
        
        transaction.state = TransactionState.NEO4J_PREPARED
        assert transaction.state == TransactionState.NEO4J_PREPARED
        
        transaction.state = TransactionState.PINECONE_PREPARED
        assert transaction.state == TransactionState.PINECONE_PREPARED
        
        transaction.state = TransactionState.COMMITTED
        assert transaction.state == TransactionState.COMMITTED
        assert transaction.completed_at is None  # Set by transaction manager
    
    def test_transaction_operation_creation(self):
        """Test TransactionOperation object"""
        mock_method = AsyncMock()
        mock_rollback = AsyncMock()
        
        op = TransactionOperation(
            operation_type="test_op",
            target_store="neo4j",
            method=mock_method,
            args=("arg1", "arg2"),
            kwargs={"key": "value"},
            rollback_method=mock_rollback,
            rollback_args=("rollback_arg",)
        )
        
        assert op.operation_type == "test_op"
        assert op.target_store == "neo4j"
        assert op.executed is False
        assert op.result is None
        assert op.args == ("arg1", "arg2")
        assert op.kwargs == {"key": "value"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
