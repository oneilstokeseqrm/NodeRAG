"""Example usage of Pinecone adapter for NodeRAG"""
import asyncio
import numpy as np

from NodeRAG.storage.pinecone_adapter import PineconeAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.utils.id_generation import NodeIDGenerator


async def main():
    """Demonstrate Pinecone adapter usage"""
    
    adapter = PineconeAdapter(
        api_key="pcsk_3EYvVL_Dxe7oL1cZsn1syqfNfHqAJVKKTMAcJPW3NnZjLmiNeD5aP7VVYuAzLzHWnsLccp",
        index_name="noderag"
    )
    
    print("Connecting to Pinecone...")
    connected = adapter.connect()
    if not connected:
        print("Failed to connect to Pinecone")
        return
    
    print("Connected successfully!")
    
    try:
        metadata = EQMetadata(
            tenant_id="example_tenant",
            interaction_id="int_12345678-1234-4567-8901-123456789012",
            interaction_type="email",
            text="Customer inquiry about invoice payment status",
            account_id="acc_12345678-1234-4567-8901-123456789012",
            timestamp="2024-01-20T15:00:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789012",
            source_system="outlook"
        )
        
        sem_id = NodeIDGenerator.generate_semantic_unit_id(
            text=metadata.text,
            tenant_id=metadata.tenant_id,
            doc_id="doc_example_001",
            chunk_index=0
        )
        
        semantic_embedding = np.random.rand(3072).tolist()
        
        print(f"\nUpserting semantic unit: {sem_id}")
        success = await adapter.upsert_vector(
            vector_id=sem_id,
            embedding=semantic_embedding,
            metadata=metadata,
            additional_metadata={
                "node_type": "semantic_unit",
                "chunk_index": 0
            }
        )
        print(f"Semantic unit upserted: {success}")
        
        entities = [
            ("Customer", "PERSON"),
            ("Invoice", "DOCUMENT"),
            ("Payment Status", "CONCEPT")
        ]
        
        entity_vectors = []
        for entity_name, entity_type in entities:
            entity_id = NodeIDGenerator.generate_entity_id(
                entity_name=entity_name,
                entity_type=entity_type,
                tenant_id=metadata.tenant_id
            )
            
            entity_embedding = np.random.rand(3072).tolist()
            
            entity_vectors.append((
                entity_id,
                entity_embedding,
                metadata,
                {
                    "node_type": "entity",
                    "entity_name": entity_name,
                    "entity_type": entity_type
                }
            ))
        
        print("\nBatch upserting entities...")
        success_count, errors = await adapter.upsert_vectors_batch(entity_vectors)
        print(f"Entities upserted: {success_count}, Errors: {len(errors)}")
        
        print("\nWaiting for indexing...")
        await asyncio.sleep(8)
        
        print("\nSearching for similar content...")
        query_embedding = np.random.rand(3072).tolist()
        
        results = await adapter.search(
            query_embedding=query_embedding,
            filters={
                "tenant_id": "example_tenant",
                "interaction_type": "email"
            },
            top_k=5,
            namespace="example_tenant"
        )
        
        print(f"\nFound {len(results)} similar vectors:")
        for result in results:
            print(f"  - ID: {result['id']}")
            print(f"    Score: {result['score']:.4f}")
            print(f"    Type: {result['metadata'].get('node_type', 'unknown')}")
        
        print("\nIndex statistics:")
        stats = await adapter.get_stats()
        print(f"  Total vectors: {stats.get('total_vectors', 0)}")
        print(f"  Dimension: {stats.get('dimension', 0)}")
        print(f"  Namespaces: {len(stats.get('namespaces', {}))}")
        
        print("\nTesting metadata filtering...")
        
        chat_metadata = EQMetadata(
            tenant_id="example_tenant",
            interaction_id="int_12345678-1234-4567-8901-123456789013",
            interaction_type="chat",
            text="Quick chat about delivery",
            account_id="acc_12345678-1234-4567-8901-123456789012",
            timestamp="2024-01-20T16:00:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789012",
            source_system="internal"
        )
        
        chat_id = "chat_example_001"
        chat_embedding = np.random.rand(3072).tolist()
        
        await adapter.upsert_vector(
            vector_id=chat_id,
            embedding=chat_embedding,
            metadata=chat_metadata,
            additional_metadata={"node_type": "semantic_unit"}
        )
        
        await asyncio.sleep(8)
        
        email_results = await adapter.search(
            query_embedding=query_embedding,
            filters={
                "tenant_id": "example_tenant",
                "interaction_type": "email"
            },
            top_k=10,
            namespace="example_tenant"
        )
        
        print(f"\nEmail-only search found {len(email_results)} vectors")
        
        print("\nCleaning up example data...")
        cleanup_success = await adapter.delete_namespace("example_tenant")
        print(f"Cleanup successful: {cleanup_success}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        adapter.close()
        print("\nConnection closed")


if __name__ == "__main__":
    asyncio.run(main())
