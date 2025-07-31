"""Unit tests for Pinecone adapter"""
import pytest
import asyncio
import numpy as np
pass

from NodeRAG.storage.pinecone_adapter import PineconeAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.utils.id_generation import NodeIDGenerator
from tests.config.test_pinecone_config import get_test_pinecone_config


@pytest.fixture
def pinecone_adapter():
    """Create Pinecone adapter for testing"""
    config = get_test_pinecone_config()
    adapter = PineconeAdapter(
        api_key=config["api_key"],
        index_name=config["index_name"]
    )
    
    connected = adapter.connect()
    if not connected:
        pytest.skip("Could not connect to Pinecone")
    
    yield adapter
    
    asyncio.run(adapter.delete_namespace("test_tenant"))
    asyncio.run(adapter.delete_namespace("test_tenant_1"))
    asyncio.run(adapter.delete_namespace("test_tenant_2"))
    adapter.close()


@pytest.fixture
def sample_metadata():
    """Sample metadata for testing"""
    return EQMetadata(
        tenant_id="test_tenant",
        interaction_id="int_12345678-1234-4567-8901-123456789012",
        interaction_type="email",
        text="Test email content for Pinecone",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="outlook"
    )


@pytest.fixture
def sample_embedding():
    """Generate sample embedding vector"""
    return np.random.rand(3072).tolist()


class TestPineconeAdapter:
    """Test Pinecone adapter functionality"""
    
    @pytest.mark.asyncio
    async def test_connection(self, pinecone_adapter):
        """Test basic connection"""
        assert pinecone_adapter.index is not None
        
        stats = await pinecone_adapter.get_stats()
        assert "dimension" in stats
        assert stats["dimension"] == 3072
    
    @pytest.mark.asyncio
    async def test_upsert_single_vector(self, pinecone_adapter, sample_metadata, sample_embedding):
        """Test upserting a single vector"""
        vector_id = NodeIDGenerator.generate_semantic_unit_id(
            text=sample_metadata.text,
            tenant_id=sample_metadata.tenant_id,
            doc_id="doc_test_001",
            chunk_index=0
        )
        
        success = await pinecone_adapter.upsert_vector(
            vector_id=vector_id,
            embedding=sample_embedding,
            metadata=sample_metadata,
            additional_metadata={"node_type": "semantic_unit"}
        )
        
        assert success is True
        
        await asyncio.sleep(10)
        
        stats = await pinecone_adapter.get_stats()
        print(f"Index stats after upsert: {stats}")
        
        vector = None
        for attempt in range(3):
            vector = await pinecone_adapter.get_vector(vector_id, sample_metadata.tenant_id)
            if vector is not None:
                break
            print(f"Attempt {attempt + 1}: Vector not found, waiting 5 more seconds...")
            await asyncio.sleep(5)
        print(f"Retrieved vector: {vector}")
        print(f"Looking for vector_id: {vector_id} in namespace: {sample_metadata.tenant_id}")
        
        if vector is None:
            all_results = await pinecone_adapter.search(
                query_embedding=sample_embedding,
                filters={"tenant_id": sample_metadata.tenant_id},
                top_k=10,
                namespace=sample_metadata.tenant_id
            )
            print(f"All vectors in namespace {sample_metadata.tenant_id}: {all_results}")
        
        assert vector is not None
        assert vector["id"] == vector_id
        assert vector["metadata"]["tenant_id"] == "test_tenant"
    
    @pytest.mark.asyncio
    async def test_upsert_vectors_batch(self, pinecone_adapter, sample_metadata):
        """Test batch vector upsert"""
        vectors = []
        
        for i in range(10):
            vector_id = f"test_batch_vector_{i}"
            embedding = np.random.rand(3072).tolist()
            metadata = sample_metadata
            additional = {"node_type": "entity", "index": i}
            
            vectors.append((vector_id, embedding, metadata, additional))
        
        successful_count, errors = await pinecone_adapter.upsert_vectors_batch(vectors)
        
        assert successful_count == 10
        assert len(errors) == 0
        
        await asyncio.sleep(10)
        
        stats = await pinecone_adapter.get_stats()
        assert stats["total_vectors"] > 0
    
    @pytest.mark.asyncio
    async def test_metadata_preparation(self, pinecone_adapter, sample_metadata):
        """Test metadata preparation for Pinecone"""
        prepared = pinecone_adapter.prepare_metadata(
            sample_metadata,
            {"node_type": "entity", "confidence": 0.95}
        )
        
        assert prepared["tenant_id"] == "test_tenant"
        assert prepared["interaction_id"] == "int_12345678-1234-4567-8901-123456789012"
        assert prepared["interaction_type"] == "email"
        assert prepared["account_id"] == "acc_12345678-1234-4567-8901-123456789012"
        assert prepared["timestamp"] == "2024-01-15T10:30:00Z"
        assert prepared["user_id"] == "usr_12345678-1234-4567-8901-123456789012"
        assert prepared["source_system"] == "outlook"
        
        assert prepared["node_type"] == "entity"
        assert prepared["confidence"] == 0.95
        
        assert "text" not in prepared
    
    @pytest.mark.asyncio
    async def test_search_with_filters(self, pinecone_adapter, sample_metadata, sample_embedding):
        """Test vector search with metadata filtering"""
        await pinecone_adapter.delete_namespace("test_tenant")
        await asyncio.sleep(5)
        
        vector1_id = "search_test_1"
        vector2_id = "search_test_2"
        
        await pinecone_adapter.upsert_vector(
            vector_id=vector1_id,
            embedding=sample_embedding,
            metadata=sample_metadata,
            additional_metadata={"node_type": "semantic_unit"}
        )
        
        chat_metadata = EQMetadata(
            tenant_id="test_tenant",
            interaction_id="int_12345678-1234-4567-8901-123456789013",
            interaction_type="chat",
            text="Test chat content",
            account_id="acc_12345678-1234-4567-8901-123456789012",
            timestamp="2024-01-15T11:30:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789012",
            source_system="internal"
        )
        
        await pinecone_adapter.upsert_vector(
            vector_id=vector2_id,
            embedding=sample_embedding,
            metadata=chat_metadata,
            additional_metadata={"node_type": "semantic_unit"}
        )
        
        await asyncio.sleep(12)
        
        results = await pinecone_adapter.search(
            query_embedding=sample_embedding,
            filters={"tenant_id": "test_tenant", "interaction_type": "email"},
            top_k=5
        )
        
        print(f"Search results: {results}")
        print(f"Looking for vector1_id: {vector1_id}")
        print(f"Search filters: tenant_id=test_tenant, interaction_type=email")
        
        all_results = await pinecone_adapter.search(
            query_embedding=sample_embedding,
            filters={"tenant_id": "test_tenant"},
            top_k=10
        )
        print(f"All results in test_tenant: {all_results}")
        
        assert len(results) >= 1
        assert any(r["id"] == vector1_id for r in results)
        assert not any(r["id"] == vector2_id for r in results)
    
    @pytest.mark.asyncio
    async def test_namespace_isolation(self, pinecone_adapter, sample_embedding):
        """Test tenant isolation via namespaces"""
        tenant1_metadata = EQMetadata(
            tenant_id="test_tenant_1",
            interaction_id="int_12345678-1234-4567-8901-123456789014",
            interaction_type="email",
            text="Tenant 1 content",
            account_id="acc_12345678-1234-4567-8901-123456789014",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789014",
            source_system="outlook"
        )
        
        tenant2_metadata = EQMetadata(
            tenant_id="test_tenant_2",
            interaction_id="int_12345678-1234-4567-8901-123456789015",
            interaction_type="email",
            text="Tenant 2 content",
            account_id="acc_12345678-1234-4567-8901-123456789015",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789015",
            source_system="gmail"
        )
        
        await pinecone_adapter.upsert_vector(
            vector_id="tenant1_vector",
            embedding=sample_embedding,
            metadata=tenant1_metadata
        )
        
        await pinecone_adapter.upsert_vector(
            vector_id="tenant2_vector",
            embedding=sample_embedding,
            metadata=tenant2_metadata
        )
        
        await asyncio.sleep(8)
        
        results_tenant1 = await pinecone_adapter.search(
            query_embedding=sample_embedding,
            filters={"tenant_id": "test_tenant_1"},
            top_k=10,
            namespace="test_tenant_1"
        )
        
        assert len(results_tenant1) >= 1
        assert all(r["metadata"].get("tenant_id") == "test_tenant_1" for r in results_tenant1)
    
    @pytest.mark.asyncio
    async def test_delete_vectors(self, pinecone_adapter, sample_metadata, sample_embedding):
        """Test vector deletion"""
        vector_id = "delete_test_vector"
        
        await pinecone_adapter.upsert_vector(
            vector_id=vector_id,
            embedding=sample_embedding,
            metadata=sample_metadata
        )
        
        await asyncio.sleep(5)
        
        success = await pinecone_adapter.delete_vectors(
            vector_ids=[vector_id],
            namespace=sample_metadata.tenant_id
        )
        
        assert success is True
        
        await asyncio.sleep(5)
        
        vector = await pinecone_adapter.get_vector(vector_id, sample_metadata.tenant_id)
        assert vector is None
    
    @pytest.mark.asyncio
    async def test_namespace_cleanup(self, pinecone_adapter):
        """Test namespace deletion for cleanup"""
        test_namespace = "test_cleanup_namespace"
        
        test_metadata = EQMetadata(
            tenant_id=test_namespace,
            interaction_id="int_12345678-1234-4567-8901-123456789016",
            interaction_type="email",
            text="Cleanup test",
            account_id="acc_12345678-1234-4567-8901-123456789016",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789016",
            source_system="outlook"
        )
        
        await pinecone_adapter.upsert_vector(
            vector_id="cleanup_vector",
            embedding=np.random.rand(3072).tolist(),
            metadata=test_metadata
        )
        
        await asyncio.sleep(5)
        
        success = await pinecone_adapter.delete_namespace(test_namespace)
        assert success is True
        
        await asyncio.sleep(8)
        stats = await pinecone_adapter.get_stats()
        
        if test_namespace in stats.get("namespaces", {}):
            assert stats["namespaces"][test_namespace]["vector_count"] == 0
    
    @pytest.mark.asyncio
    async def test_invalid_metadata_handling(self, pinecone_adapter, sample_embedding):
        """Test handling of invalid metadata"""
        invalid_metadata = EQMetadata(
            tenant_id="",
            interaction_id="int_invalid_123",
            interaction_type="email",
            text="Invalid metadata test",
            account_id="acc_invalid_456",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_invalid_789",
            source_system="outlook"
        )
        
        success = await pinecone_adapter.upsert_vector(
            vector_id="invalid_vector",
            embedding=sample_embedding,
            metadata=invalid_metadata
        )
        
        assert success is False
    
    @pytest.mark.asyncio
    async def test_get_statistics(self, pinecone_adapter):
        """Test statistics retrieval"""
        stats = await pinecone_adapter.get_stats()
        
        assert "total_vectors" in stats
        assert "dimension" in stats
        assert stats["dimension"] == 3072
        assert "namespaces" in stats
        
        print(f"Index statistics: {stats}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
