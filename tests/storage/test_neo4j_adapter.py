"""Unit tests for Neo4j adapter"""
import pytest
import pytest_asyncio
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

from NodeRAG.storage.neo4j_adapter import Neo4jAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
from tests.config.test_neo4j_config import get_test_neo4j_config


@pytest_asyncio.fixture
async def neo4j_adapter():
    """Create Neo4j adapter for testing"""
    config = get_test_neo4j_config()
    adapter = Neo4jAdapter(config)
    
    connected = await adapter.connect()
    if not connected:
        pytest.skip("Could not connect to Neo4j database")
    
    await adapter.create_constraints_and_indexes()
    
    yield adapter
    
    try:
        async with adapter.driver.session(database=adapter.database) as session:
            await session.run("MATCH (n) WHERE n.tenant_id STARTS WITH 'test_tenant_' DETACH DELETE n")
            await session.run("MATCH (n) WHERE n.node_id CONTAINS 'test' OR n.node_id CONTAINS 'sem_' OR n.node_id CONTAINS 'ent_' OR n.node_id CONTAINS 'stats_' OR n.node_id CONTAINS 'clear_' OR n.node_id CONTAINS 'sub_' OR n.node_id CONTAINS 'node_meta_' OR n.node_id CONTAINS 'tenant' OR n.node_id CONTAINS 'batch' OR n.node_id CONTAINS 'rel_' DETACH DELETE n")
    except Exception as e:
        print(f"Cleanup warning: {e}")
    
    await adapter.close()


@pytest.fixture
def sample_metadata():
    """Create sample EQ metadata for testing"""
    return EQMetadata(
        tenant_id="test_tenant_123",
        interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
        interaction_type="email",
        text="This is test content for the semantic unit",
        account_id="acc_550e8400-e29b-41d4-a716-446655440000",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_550e8400-e29b-41d4-a716-446655440000",
        source_system="outlook"
    )


@pytest.mark.asyncio
class TestNeo4jAdapter:
    """Test cases for Neo4j adapter"""
    
    async def test_connection(self, neo4j_adapter):
        """Test Neo4j connection"""
        health = await neo4j_adapter.health_check()
        assert health['status'] == 'healthy'
        assert 'response_time_seconds' in health
    
    async def test_add_single_node(self, neo4j_adapter, sample_metadata):
        """Test adding a single node"""
        node_id = "sem_test123456789abc"
        success = await neo4j_adapter.add_node(
            node_id=node_id,
            node_type="semantic_unit",
            metadata=sample_metadata,
            properties={"content": "Test semantic unit content"}
        )
        
        assert success is True
        
        nodes = await neo4j_adapter.get_nodes_by_tenant("test_tenant_123")
        assert len(nodes) == 1
        assert nodes[0]['node_id'] == node_id
        assert nodes[0]['node_type'] == "semantic_unit"
        assert nodes[0]['tenant_id'] == "test_tenant_123"
    
    async def test_add_nodes_batch(self, neo4j_adapter, sample_metadata):
        """Test adding multiple nodes in batch"""
        nodes = []
        for i in range(5):
            node_data = sample_metadata.to_dict()
            node_data.update({
                'node_id': f"sem_batch{i:03d}",
                'node_type': 'semantic_unit',
                'content': f"Batch content {i}"
            })
            nodes.append(node_data)
        
        success_count, errors = await neo4j_adapter.add_nodes_batch(nodes)
        
        assert success_count == 5
        assert len(errors) == 0
        
        tenant_nodes = await neo4j_adapter.get_nodes_by_tenant("test_tenant_123")
        assert len(tenant_nodes) == 5
    
    async def test_add_relationship(self, neo4j_adapter, sample_metadata):
        """Test adding a relationship between nodes"""
        node1_id = "sem_rel_test_001"
        node2_id = "ent_rel_test_002"
        
        await neo4j_adapter.add_node(node1_id, "semantic_unit", sample_metadata)
        await neo4j_adapter.add_node(node2_id, "entity", sample_metadata)
        
        success = await neo4j_adapter.add_relationship(
            source_id=node1_id,
            target_id=node2_id,
            relationship_type="CONTAINS",
            metadata=sample_metadata,
            properties={"weight": 1.0}
        )
        
        assert success is True
        
        subgraph = await neo4j_adapter.get_subgraph("test_tenant_123")
        assert subgraph['relationship_count'] == 1
        assert len(subgraph['relationships']) == 1
    
    async def test_get_nodes_by_metadata(self, neo4j_adapter, sample_metadata):
        """Test filtering nodes by metadata"""
        metadata1 = sample_metadata
        metadata2 = EQMetadata(
            tenant_id="test_tenant_123",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440001",
            interaction_type="chat",
            text="Different content",
            account_id="acc_550e8400-e29b-41d4-a716-446655440001",
            timestamp="2024-01-15T11:30:00Z",
            user_id="usr_550e8400-e29b-41d4-a716-446655440001",
            source_system="internal"
        )
        
        await neo4j_adapter.add_node("node_meta_1", "semantic_unit", metadata1)
        await neo4j_adapter.add_node("node_meta_2", "entity", metadata2)
        
        email_nodes = await neo4j_adapter.get_nodes_by_metadata({"interaction_type": "email"})
        chat_nodes = await neo4j_adapter.get_nodes_by_metadata({"interaction_type": "chat"})
        
        assert len(email_nodes) == 1
        assert len(chat_nodes) == 1
        assert email_nodes[0]['node_id'] == "node_meta_1"
        assert chat_nodes[0]['node_id'] == "node_meta_2"
    
    async def test_tenant_isolation(self, neo4j_adapter, sample_metadata):
        """Test that tenant data is properly isolated"""
        tenant1_metadata = sample_metadata
        tenant2_metadata = EQMetadata(
            tenant_id="test_tenant_456",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440002",
            interaction_type="email",
            text="Tenant 2 content",
            account_id="acc_550e8400-e29b-41d4-a716-446655440002",
            timestamp="2024-01-15T12:30:00Z",
            user_id="usr_550e8400-e29b-41d4-a716-446655440002",
            source_system="gmail"
        )
        
        await neo4j_adapter.add_node("tenant1_node", "semantic_unit", tenant1_metadata)
        await neo4j_adapter.add_node("tenant2_node", "semantic_unit", tenant2_metadata)
        
        tenant1_nodes = await neo4j_adapter.get_nodes_by_tenant("test_tenant_123")
        tenant2_nodes = await neo4j_adapter.get_nodes_by_tenant("test_tenant_456")
        
        assert len(tenant1_nodes) == 1
        assert len(tenant2_nodes) == 1
        assert tenant1_nodes[0]['tenant_id'] == "test_tenant_123"
        assert tenant2_nodes[0]['tenant_id'] == "test_tenant_456"
    
    async def test_get_subgraph(self, neo4j_adapter, sample_metadata):
        """Test subgraph retrieval with filtering"""
        await neo4j_adapter.add_node("sub_node_1", "semantic_unit", sample_metadata)
        await neo4j_adapter.add_node("sub_node_2", "entity", sample_metadata)
        await neo4j_adapter.add_relationship("sub_node_1", "sub_node_2", "CONTAINS", sample_metadata)
        
        subgraph = await neo4j_adapter.get_subgraph("test_tenant_123")
        
        assert subgraph['node_count'] == 2
        assert subgraph['relationship_count'] == 1
        assert len(subgraph['nodes']) == 2
        assert len(subgraph['relationships']) == 1
        
        account_subgraph = await neo4j_adapter.get_subgraph(
            "test_tenant_123", 
            account_id="acc_550e8400-e29b-41d4-a716-446655440000"
        )
        assert account_subgraph['node_count'] == 2
    
    async def test_statistics(self, neo4j_adapter, sample_metadata):
        """Test database statistics collection"""
        await neo4j_adapter.add_node("stats_node_1", "semantic_unit", sample_metadata)
        await neo4j_adapter.add_node("stats_node_2", "entity", sample_metadata)
        
        stats = await neo4j_adapter.get_statistics()
        
        assert 'total_nodes' in stats
        assert 'total_relationships' in stats
        assert 'nodes_by_type' in stats
        assert 'nodes_by_tenant' in stats
        assert stats['total_nodes'] >= 2
    
    async def test_clear_tenant_data(self, neo4j_adapter, sample_metadata):
        """Test clearing tenant data"""
        await neo4j_adapter.add_node("clear_test_node", "semantic_unit", sample_metadata)
        
        nodes_before = await neo4j_adapter.get_nodes_by_tenant("test_tenant_123")
        assert len(nodes_before) >= 1
        
        success = await neo4j_adapter.clear_tenant_data("test_tenant_123")
        assert success is True
        
        nodes_after = await neo4j_adapter.get_nodes_by_tenant("test_tenant_123")
        assert len(nodes_after) == 0
    
    async def test_invalid_metadata_validation(self, neo4j_adapter):
        """Test that invalid metadata is rejected"""
        invalid_metadata = EQMetadata(
            tenant_id="",  # Empty tenant_id should fail
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Test content",
            account_id="acc_550e8400-e29b-41d4-a716-446655440000",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_550e8400-e29b-41d4-a716-446655440000",
            source_system="outlook"
        )
        
        success = await neo4j_adapter.add_node(
            "invalid_node", "semantic_unit", invalid_metadata
        )
        
        assert success is False
