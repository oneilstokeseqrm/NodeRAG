"""
Example usage of Neo4j adapter for NodeRAG
"""
import asyncio
from datetime import datetime, timezone

from NodeRAG.storage.neo4j_adapter import Neo4jAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.utils.id_generation import NodeIDGenerator


async def main():
    """Demonstrate Neo4j adapter usage"""
    
    config = {
        'uri': 'neo4j+s://b875880c.databases.neo4j.io',
        'user': 'neo4j',
        'password': 'GjiwqwFNiYJnIIZwFcnInbdH2a61f40mJnPolkUd1u4',
        'database': 'neo4j'
    }
    
    adapter = Neo4jAdapter(config)
    
    try:
        print("Connecting to Neo4j...")
        connected = await adapter.connect()
        if not connected:
            print("Failed to connect to Neo4j")
            return
        
        print("Creating constraints and indexes...")
        await adapter.create_constraints_and_indexes()
        
        metadata = EQMetadata(
            tenant_id="example_tenant_123",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="This is an example email content for demonstration",
            account_id="acc_123e4567-e89b-12d3-a456-426614174000",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_987fcdeb-51a2-43d1-9f12-345678901234",
            source_system="outlook"
        )
        
        semantic_unit_id = NodeIDGenerator.generate_semantic_unit_id(
            metadata.text, metadata.tenant_id, "doc_example_123", 0
        )
        
        print(f"Adding semantic unit: {semantic_unit_id}")
        success = await adapter.add_node(
            node_id=semantic_unit_id,
            node_type="semantic_unit",
            metadata=metadata,
            properties={
                "content": metadata.text,
                "chunk_index": 0,
                "doc_id": "doc_example_123"
            }
        )
        print(f"Semantic unit added: {success}")
        
        entity_id = NodeIDGenerator.generate_entity_id("John Smith", "PERSON", metadata.tenant_id)
        
        print(f"Adding entity: {entity_id}")
        success = await adapter.add_node(
            node_id=entity_id,
            node_type="entity",
            metadata=metadata,
            properties={
                "name": "John Smith",
                "entity_type": "PERSON"
            }
        )
        print(f"Entity added: {success}")
        
        print("Adding relationship...")
        success = await adapter.add_relationship(
            source_id=semantic_unit_id,
            target_id=entity_id,
            relationship_type="CONTAINS",
            metadata=metadata,
            properties={"confidence": 0.95}
        )
        print(f"Relationship added: {success}")
        
        print("\nQuerying nodes by tenant...")
        tenant_nodes = await adapter.get_nodes_by_tenant("example_tenant_123")
        print(f"Found {len(tenant_nodes)} nodes for tenant")
        for node in tenant_nodes:
            print(f"  - {node['node_id']} ({node['node_type']})")
        
        print("\nRetrieving subgraph...")
        subgraph = await adapter.get_subgraph("example_tenant_123")
        print(f"Subgraph: {subgraph['node_count']} nodes, {subgraph['relationship_count']} relationships")
        
        print("\nGetting database statistics...")
        stats = await adapter.get_statistics()
        print(f"Total nodes: {stats.get('total_nodes', 0)}")
        print(f"Total relationships: {stats.get('total_relationships', 0)}")
        
        print("\nCleaning up test data...")
        cleanup_success = await adapter.clear_tenant_data("example_tenant_123")
        print(f"Cleanup successful: {cleanup_success}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await adapter.close()
        print("Connection closed")


if __name__ == "__main__":
    asyncio.run(main())
