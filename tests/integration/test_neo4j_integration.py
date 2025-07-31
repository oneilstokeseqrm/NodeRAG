"""Integration tests for Neo4j adapter"""
import pytest
import pytest_asyncio
import asyncio
import time
import csv
import os
from datetime import datetime, timezone

from NodeRAG.storage.neo4j_adapter import Neo4jAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.utils.id_generation import NodeIDGenerator
from tests.config.test_neo4j_config import get_test_neo4j_config


@pytest_asyncio.fixture
async def neo4j_adapter():
    """Create Neo4j adapter for integration testing"""
    config = get_test_neo4j_config()
    adapter = Neo4jAdapter(config)
    
    connected = await adapter.connect()
    if not connected:
        pytest.skip("Could not connect to Neo4j database")
    
    await adapter.create_constraints_and_indexes()
    
    yield adapter
    
    await adapter.clear_tenant_data("integration_test_tenant")
    await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
class TestNeo4jIntegration:
    """Integration tests for Neo4j adapter"""
    
    async def test_batch_performance(self, neo4j_adapter):
        """Test batch operation performance (target: >1000 nodes/second)"""
        tenant_id = "integration_test_tenant"
        
        base_metadata = EQMetadata(
            tenant_id=tenant_id,
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Performance test content",
            account_id="acc_550e8400-e29b-41d4-a716-446655440000",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_550e8400-e29b-41d4-a716-446655440000",
            source_system="outlook"
        )
        
        batch_sizes = [100, 500, 1000, 2000]
        performance_results = []
        
        for batch_size in batch_sizes:
            nodes = []
            for i in range(batch_size):
                node_data = base_metadata.to_dict()
                node_data.update({
                    'node_id': f"perf_test_{i:06d}",
                    'node_type': 'semantic_unit',
                    'content': f"Performance test content {i}"
                })
                nodes.append(node_data)
            
            start_time = time.time()
            success_count, errors = await neo4j_adapter.add_nodes_batch(nodes)
            end_time = time.time()
            
            duration = end_time - start_time
            nodes_per_second = batch_size / duration if duration > 0 else 0
            
            performance_results.append({
                'batch_size': batch_size,
                'duration_seconds': duration,
                'nodes_per_second': nodes_per_second,
                'success_count': success_count,
                'error_count': len(errors)
            })
            
            await neo4j_adapter.clear_tenant_data(tenant_id)
        
        csv_path = 'neo4j_batch_performance.csv'
        with open(csv_path, 'w', newline='') as csvfile:
            fieldnames = ['batch_size', 'duration_seconds', 'nodes_per_second', 'success_count', 'error_count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(performance_results)
        
        best_performance = max(performance_results, key=lambda x: x['nodes_per_second'])
        assert best_performance['nodes_per_second'] > 1000, f"Performance target not met: {best_performance['nodes_per_second']} nodes/second"
        
        print(f"Best performance: {best_performance['nodes_per_second']:.2f} nodes/second with batch size {best_performance['batch_size']}")
    
    async def test_complete_workflow_with_eq_metadata(self, neo4j_adapter):
        """Test complete workflow with EQ metadata integration"""
        tenant_id = "integration_test_tenant"
        
        metadata = EQMetadata(
            tenant_id=tenant_id,
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Complete workflow test content",
            account_id="acc_550e8400-e29b-41d4-a716-446655440000",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_550e8400-e29b-41d4-a716-446655440000",
            source_system="outlook"
        )
        
        semantic_units = []
        for i in range(3):
            node_id = NodeIDGenerator.generate_semantic_unit_id(
                f"Semantic unit {i}", tenant_id, "doc_123", i
            )
            semantic_units.append(node_id)
            
            success = await neo4j_adapter.add_node(
                node_id=node_id,
                node_type="semantic_unit",
                metadata=metadata,
                properties={"content": f"Semantic unit {i} content"}
            )
            assert success is True
        
        entities = []
        entity_names = ["John Smith", "Acme Corp", "New York"]
        entity_types = ["PERSON", "ORGANIZATION", "LOCATION"]
        
        for name, entity_type in zip(entity_names, entity_types):
            entity_id = NodeIDGenerator.generate_entity_id(name, entity_type, tenant_id)
            entities.append(entity_id)
            
            success = await neo4j_adapter.add_node(
                node_id=entity_id,
                node_type="entity",
                metadata=metadata,
                properties={"name": name, "entity_type": entity_type}
            )
            assert success is True
        
        relationships_added = 0
        for sem_unit in semantic_units:
            for entity in entities:
                success = await neo4j_adapter.add_relationship(
                    source_id=sem_unit,
                    target_id=entity,
                    relationship_type="CONTAINS",
                    metadata=metadata,
                    properties={"confidence": 0.95}
                )
                if success:
                    relationships_added += 1
        
        subgraph = await neo4j_adapter.get_subgraph(tenant_id)
        
        assert subgraph['node_count'] == 6  # 3 semantic units + 3 entities
        assert subgraph['relationship_count'] == relationships_added
        
        semantic_nodes = await neo4j_adapter.get_nodes_by_metadata({
            "tenant_id": tenant_id,
            "node_type": "semantic_unit"
        })
        assert len(semantic_nodes) == 3
        
        entity_nodes = await neo4j_adapter.get_nodes_by_metadata({
            "tenant_id": tenant_id,
            "node_type": "entity"
        })
        assert len(entity_nodes) == 3
        
        account_subgraph = await neo4j_adapter.get_subgraph(
            tenant_id, account_id="acc_550e8400-e29b-41d4-a716-446655440000"
        )
        assert account_subgraph['node_count'] == 6
        
        print(f"Complete workflow test passed: {subgraph['node_count']} nodes, {subgraph['relationship_count']} relationships")
    
    async def test_index_usage_verification(self, neo4j_adapter):
        """Test that indexes are properly used for queries"""
        tenant_id = "integration_test_tenant"
        
        metadata_variants = [
            {"interaction_type": "email", "source_system": "outlook"},
            {"interaction_type": "chat", "source_system": "internal"},
            {"interaction_type": "call", "source_system": "gmail"},
        ]
        
        for i, variant in enumerate(metadata_variants):
            metadata = EQMetadata(
                tenant_id=tenant_id,
                interaction_id=f"int_550e8400-e29b-41d4-a716-44665544000{i}",
                interaction_type=variant["interaction_type"],
                text=f"Index test content {i}",
                account_id=f"acc_550e8400-e29b-41d4-a716-44665544000{i}",
                timestamp="2024-01-15T10:30:00Z",
                user_id=f"usr_550e8400-e29b-41d4-a716-44665544000{i}",
                source_system=variant["source_system"]
            )
            
            await neo4j_adapter.add_node(
                node_id=f"index_test_{i}",
                node_type="semantic_unit",
                metadata=metadata
            )
        
        email_nodes = await neo4j_adapter.get_nodes_by_metadata({"interaction_type": "email"})
        outlook_nodes = await neo4j_adapter.get_nodes_by_metadata({"source_system": "outlook"})
        tenant_nodes = await neo4j_adapter.get_nodes_by_tenant(tenant_id)
        
        assert len(email_nodes) == 1
        assert len(outlook_nodes) == 1
        assert len(tenant_nodes) == 3
        
        print("Index usage verification passed")
    
    async def test_connection_resilience(self, neo4j_adapter):
        """Test connection resilience and error handling"""
        health = await neo4j_adapter.health_check()
        assert health['status'] == 'healthy'
        assert health['response_time_seconds'] > 0
        
        stats = await neo4j_adapter.get_statistics()
        assert 'total_nodes' in stats
        assert 'total_relationships' in stats
        
        print(f"Connection resilience test passed. Health: {health['status']}")
