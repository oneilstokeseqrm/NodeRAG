"""Example usage of Transaction Manager"""
import asyncio
import numpy as np
from datetime import datetime

from NodeRAG.storage.neo4j_adapter import Neo4jAdapter
from NodeRAG.storage.pinecone_adapter import PineconeAdapter
from NodeRAG.storage.transactions.transaction_manager import TransactionManager
from NodeRAG.standards import EQMetadata
from NodeRAG.utils import NodeIDGenerator


async def main():
    """Demonstrate transaction manager usage"""
    
    neo4j_config = {
        "uri": "neo4j+s://b875880c.databases.neo4j.io",
        "user": "neo4j",
        "password": "GjiwqwFNiYJnIIZwFcnInbdH2a61f40mJnPolkUd1u4",
        "database": "neo4j"
    }
    
    pinecone_config = {
        "api_key": "pcsk_3EYvVL_Dxe7oL1cZsn1syqfNfHqAJVKKTMAcJPW3NnZjLmiNeD5aP7VVYuAzLzHWnsLccp",
        "index_name": "noderag"
    }
    
    neo4j = Neo4jAdapter(neo4j_config)
    await neo4j.connect()
    
    pinecone = PineconeAdapter(**pinecone_config)
    pinecone.connect()
    
    tm = TransactionManager(neo4j, pinecone)
    
    try:
        print("Example 1: Adding single node with embedding...")
        
        metadata = EQMetadata(
            tenant_id="example_txn_tenant",
            interaction_id="int_example_001",
            interaction_type="email",
            text="Customer inquiry about new product features",
            account_id="acc_example_001",
            timestamp=datetime.utcnow().isoformat() + "Z",
            user_id="usr_example_001",
            source_system="outlook"
        )
        
        entity_id = NodeIDGenerator.generate_entity_id(
            entity_name="New Product",
            entity_type="PRODUCT",
            tenant_id=metadata.tenant_id
        )
        
        embedding = np.random.randn(3072).tolist()
        
        success, error = await tm.add_node_with_embedding(
            node_id=entity_id,
            node_type="entity",
            metadata=metadata,
            embedding=embedding,
            node_properties={
                "name": "New Product",
                "entity_type": "PRODUCT",
                "confidence": 0.95
            },
            vector_metadata={
                "node_type": "entity",
                "extraction_method": "NER"
            }
        )
        
        if success:
            print(f"✓ Successfully added entity {entity_id}")
        else:
            print(f"✗ Failed to add entity: {error}")
        
        print("\nExample 2: Batch adding semantic units...")
        
        semantic_units = []
        doc_id = "doc_example_001"
        
        chunks = [
            "The new product features advanced AI capabilities.",
            "It includes real-time data processing and analytics.",
            "Customer support is available 24/7 through multiple channels."
        ]
        
        for i, chunk in enumerate(chunks):
            sem_id = NodeIDGenerator.generate_semantic_unit_id(
                text=chunk,
                tenant_id=metadata.tenant_id,
                doc_id=doc_id,
                chunk_index=i
            )
            
            semantic_units.append({
                "node_id": sem_id,
                "node_type": "semantic_unit",
                "metadata": metadata,
                "embedding": np.random.randn(3072).tolist(),
                "node_properties": {
                    "content": chunk,
                    "chunk_index": i,
                    "doc_id": doc_id
                },
                "vector_metadata": {
                    "node_type": "semantic_unit",
                    "chunk_index": i
                }
            })
        
        success_count, errors = await tm.add_nodes_batch_with_embeddings(semantic_units)
        print(f"✓ Successfully added {success_count} semantic units")
        if errors:
            print(f"✗ Errors: {errors}")
        
        print("\nExample 3: Demonstrating rollback on failure...")
        
        invalid_embedding = [0.1] * 100  # Wrong dimension
        
        success, error = await tm.add_node_with_embedding(
            node_id="fail_example_node",
            node_type="entity",
            metadata=metadata,
            embedding=invalid_embedding,
            node_properties={"name": "This should fail"}
        )
        
        if not success:
            print(f"✓ Transaction correctly rolled back: {error}")
        else:
            print("✗ Transaction should have failed but didn't")
        
        print("\nExample 4: Health check...")
        health = await tm.health_check()
        
        print(f"Transaction Manager: {health['transaction_manager']['status']}")
        print(f"Active Transactions: {health['transaction_manager']['active_transactions']}")
        print(f"Neo4j: {health['neo4j']['status']}")
        print(f"Pinecone: {health['pinecone']['status']} ({health['pinecone']['total_vectors']} vectors)")
        
        print("\nExample 5: Recent transaction log...")
        log_entries = tm.get_transaction_log(limit=5)
        
        for entry in log_entries:
            print(f"{entry['timestamp']} - {entry['event']} - Transaction {entry['transaction_id']}")
        
        print("\nGenerating consistency validation report...")
        
        neo4j_nodes = await neo4j.get_nodes_by_tenant("example_txn_tenant")
        
        await asyncio.sleep(10)
        
        pinecone_results = await pinecone.search(
            query_embedding=np.random.randn(3072).tolist(),
            filters={"tenant_id": "example_txn_tenant"},
            top_k=100,
            namespace="example_txn_tenant"
        )
        
        with open("consistency_validation.html", "w") as f:
            f.write("<html><head><title>Consistency Validation</title></head><body>")
            f.write("<h1>Neo4j/Pinecone Consistency Report</h1>")
            
            f.write(f"<h2>Summary</h2>")
            f.write(f"<p>Neo4j nodes: {len(neo4j_nodes)}</p>")
            f.write(f"<p>Pinecone vectors: {len(pinecone_results)}</p>")
            
            f.write("<h2>Neo4j Nodes</h2>")
            f.write("<table border='1'>")
            f.write("<tr><th>Node ID</th><th>Type</th><th>Tenant</th></tr>")
            
            for node in neo4j_nodes[:10]:  # First 10
                f.write(f"<tr><td>{node['node_id']}</td>")
                f.write(f"<td>{node.get('node_type', 'N/A')}</td>")
                f.write(f"<td>{node['tenant_id']}</td></tr>")
            
            f.write("</table>")
            
            f.write("<h2>Pinecone Vectors</h2>")
            f.write("<table border='1'>")
            f.write("<tr><th>Vector ID</th><th>Type</th><th>Score</th></tr>")
            
            for result in pinecone_results[:10]:  # First 10
                f.write(f"<tr><td>{result['id']}</td>")
                f.write(f"<td>{result['metadata'].get('node_type', 'N/A')}</td>")
                f.write(f"<td>{result['score']:.4f}</td></tr>")
            
            f.write("</table>")
            f.write("</body></html>")
        
        print("✓ Consistency report generated: consistency_validation.html")
        
        print("\nCleaning up example data...")
        deleted_nodes, deleted_rels = await neo4j.clear_tenant_data("example_txn_tenant")
        await pinecone.delete_namespace("example_txn_tenant")
        print(f"✓ Cleaned up {deleted_nodes} nodes and {deleted_rels} relationships")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await neo4j.close()
        pinecone.close()
        print("\nConnections closed")


if __name__ == "__main__":
    asyncio.run(main())
