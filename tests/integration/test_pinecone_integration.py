"""Integration tests for Pinecone adapter with NodeRAG"""
import pytest
import asyncio
import time
import csv
import numpy as np

from NodeRAG.storage.pinecone_adapter import PineconeAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.utils.id_generation import NodeIDGenerator
from tests.config.test_pinecone_config import get_test_pinecone_config


@pytest.fixture
def pinecone_adapter():
    """Create Pinecone adapter for integration testing"""
    config = get_test_pinecone_config()
    adapter = PineconeAdapter(
        api_key=config["api_key"],
        index_name=config["index_name"]
    )
    
    connected = adapter.connect()
    if not connected:
        pytest.skip("Could not connect to Pinecone")
    
    yield adapter
    
    test_namespaces = [
        "integration_test_tenant",
        "perf_test_tenant",
        "filter_test_tenant"
    ]
    for ns in test_namespaces:
        asyncio.run(adapter.delete_namespace(ns))
    
    adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
class TestPineconeIntegration:
    """Integration tests for Pinecone adapter"""
    
    async def test_namespace_isolation_verification(self, pinecone_adapter):
        """Test complete namespace isolation for multi-tenancy"""
        tenants = ["tenant_alpha", "tenant_beta", "tenant_gamma"]
        results_html = []
        
        for tenant in tenants:
            metadata = EQMetadata(
                tenant_id=tenant,
                interaction_id="int_12345678-1234-4567-8901-123456789012",
                interaction_type="email",
                text=f"Content for {tenant}",
                account_id="acc_12345678-1234-4567-8901-123456789012",
                timestamp="2024-01-15T10:30:00Z",
                user_id="usr_12345678-1234-4567-8901-123456789012",
                source_system="outlook"
            )
            
            vectors = []
            for i in range(5):
                vector_id = f"{tenant}_vec_{i}"
                embedding = np.random.rand(3072).tolist()
                vectors.append((vector_id, embedding, metadata, {"index": i}))
            
            success_count, errors = await pinecone_adapter.upsert_vectors_batch(vectors)
            assert success_count == 5
            assert len(errors) == 0
            
            results_html.append({
                "tenant": tenant,
                "vectors_created": success_count,
                "namespace": tenant
            })
        
        await asyncio.sleep(5)
        
        for tenant in tenants:
            query_embedding = np.random.rand(3072).tolist()
            results = await pinecone_adapter.search(
                query_embedding=query_embedding,
                filters={"tenant_id": tenant},
                top_k=10,
                namespace=tenant
            )
            
            for result in results:
                assert result["metadata"]["tenant_id"] == tenant
            
            results_html.append({
                "search_namespace": tenant,
                "results_found": len(results),
                "isolation_verified": True
            })
        
        with open("pinecone_namespace_test.html", "w") as f:
            f.write("<html><head><title>Pinecone Namespace Test</title></head><body>")
            f.write("<h1>Namespace Isolation Test Results</h1>")
            f.write("<table border='1'>")
            f.write("<tr><th>Tenant</th><th>Vectors Created</th><th>Namespace</th><th>Search Results</th><th>Isolation Verified</th></tr>")
            
            for result in results_html:
                if "search_namespace" in result:
                    f.write(f"<tr><td>{result['search_namespace']}</td><td>-</td><td>-</td>")
                    f.write(f"<td>{result['results_found']}</td><td>{result['isolation_verified']}</td></tr>")
                else:
                    f.write(f"<tr><td>{result['tenant']}</td><td>{result['vectors_created']}</td>")
                    f.write(f"<td>{result['namespace']}</td><td>-</td><td>-</td></tr>")
            
            f.write("</table></body></html>")
        
        for tenant in tenants:
            await pinecone_adapter.delete_namespace(tenant)
    
    async def test_metadata_filtering_all_fields(self, pinecone_adapter):
        """Test filtering on all metadata fields"""
        base_metadata = EQMetadata(
            tenant_id="filter_test_tenant",
            interaction_id="int_12345678-1234-4567-8901-123456789012",
            interaction_type="email",
            text="Filter test content",
            account_id="acc_12345678-1234-4567-8901-123456789012",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789012",
            source_system="outlook"
        )
        
        test_cases = [
            {"interaction_type": "email", "source_system": "outlook"},
            {"interaction_type": "chat", "source_system": "internal"},
            {"interaction_type": "email", "source_system": "gmail"},
            {"interaction_type": "call", "source_system": "internal"},
        ]
        
        filter_results = []
        
        for i, variant in enumerate(test_cases):
            metadata = EQMetadata(
                tenant_id="filter_test_tenant",
                interaction_id=f"int_12345678-1234-4567-8901-12345678901{i}",
                interaction_type=variant["interaction_type"],
                text=f"Content for {variant['interaction_type']}",
                account_id=f"acc_12345678-1234-4567-8901-12345678901{i}",
                timestamp="2024-01-15T10:30:00Z",
                user_id=f"usr_12345678-1234-4567-8901-12345678901{i}",
                source_system=variant["source_system"]
            )
            
            vector_id = f"filter_vec_{i}"
            embedding = np.random.rand(3072).tolist()
            
            await pinecone_adapter.upsert_vector(
                vector_id=vector_id,
                embedding=embedding,
                metadata=metadata,
                additional_metadata={"test_case": i}
            )
        
        await asyncio.sleep(5)
        
        filter_tests = [
            {"interaction_type": "email"},
            {"source_system": "outlook"},
            {"interaction_type": "email", "source_system": "outlook"},
            {"interaction_type": "chat"},
        ]
        
        for filters in filter_tests:
            filters["tenant_id"] = "filter_test_tenant"
            
            results = await pinecone_adapter.search(
                query_embedding=np.random.rand(3072).tolist(),
                filters=filters,
                top_k=10,
                namespace="filter_test_tenant"
            )
            
            filter_results.append({
                "filters": filters,
                "result_count": len(results),
                "vector_ids": [r["id"] for r in results]
            })
        
        with open("pinecone_metadata_filter.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Filter", "Result Count", "Vector IDs"])
            
            for result in filter_results:
                filter_str = ", ".join([f"{k}={v}" for k, v in result["filters"].items()])
                writer.writerow([filter_str, result["result_count"], ", ".join(result["vector_ids"])])
    
    async def test_batch_upsert_performance(self, pinecone_adapter):
        """Test batch upsert performance"""
        batch_sizes = [100, 500, 1000]
        performance_results = []
        
        for batch_size in batch_sizes:
            vectors = []
            metadata = EQMetadata(
                tenant_id="perf_test_tenant",
                interaction_id="int_12345678-1234-4567-8901-123456789012",
                interaction_type="email",
                text="Performance test",
                account_id="acc_12345678-1234-4567-8901-123456789012",
                timestamp="2024-01-15T10:30:00Z",
                user_id="usr_12345678-1234-4567-8901-123456789012",
                source_system="internal"
            )
            
            for i in range(batch_size):
                vector_id = f"perf_{batch_size}_{i:05d}"
                embedding = np.random.rand(3072).tolist()
                vectors.append((vector_id, embedding, metadata, {"index": i}))
            
            start_time = time.time()
            success_count, errors = await pinecone_adapter.upsert_vectors_batch(vectors)
            end_time = time.time()
            
            duration = end_time - start_time
            vectors_per_second = batch_size / duration if duration > 0 else 0
            
            performance_results.append({
                "batch_size": batch_size,
                "duration_seconds": duration,
                "vectors_per_second": vectors_per_second,
                "success_count": success_count,
                "error_count": len(errors)
            })
            
            await pinecone_adapter.delete_namespace("perf_test_tenant")
            await asyncio.sleep(2)
        
        with open("pinecone_performance.html", "w") as f:
            f.write("<html><head><title>Pinecone Performance Report</title></head><body>")
            f.write("<h1>Batch Upsert Performance</h1>")
            f.write("<table border='1'>")
            f.write("<tr><th>Batch Size</th><th>Duration (s)</th><th>Vectors/Second</th><th>Success Count</th><th>Errors</th></tr>")
            
            for result in performance_results:
                f.write(f"<tr><td>{result['batch_size']}</td>")
                f.write(f"<td>{result['duration_seconds']:.2f}</td>")
                f.write(f"<td>{result['vectors_per_second']:.2f}</td>")
                f.write(f"<td>{result['success_count']}</td>")
                f.write(f"<td>{result['error_count']}</td></tr>")
            
            f.write("</table>")
            
            f.write("<h2>Search Performance</h2>")
            
            search_vectors = []
            for i in range(100):
                search_vectors.append((
                    f"search_perf_{i}",
                    np.random.rand(3072).tolist(),
                    metadata,
                    {"index": i}
                ))
            
            await pinecone_adapter.upsert_vectors_batch(search_vectors)
            await asyncio.sleep(5)
            
            search_times = []
            for _ in range(10):
                start = time.time()
                results = await pinecone_adapter.search(
                    query_embedding=np.random.rand(3072).tolist(),
                    filters={"tenant_id": "perf_test_tenant"},
                    top_k=10,
                    namespace="perf_test_tenant"
                )
                search_times.append(time.time() - start)
            
            avg_search_time = sum(search_times) / len(search_times)
            
            f.write(f"<p>Average search time (10 queries): {avg_search_time:.3f} seconds</p>")
            f.write(f"<p>Min search time: {min(search_times):.3f} seconds</p>")
            f.write(f"<p>Max search time: {max(search_times):.3f} seconds</p>")
            
            f.write("</body></html>")
    
    async def test_complete_workflow(self, pinecone_adapter):
        """Test complete NodeRAG workflow with Pinecone"""
        tenant_id = "integration_test_tenant"
        
        doc_metadata = EQMetadata(
            tenant_id=tenant_id,
            interaction_id="int_12345678-1234-4567-8901-123456789012",
            interaction_type="email",
            text="Complete workflow test document",
            account_id="acc_12345678-1234-4567-8901-123456789012",
            timestamp="2024-01-20T15:00:00Z",
            user_id="usr_12345678-1234-4567-8901-123456789012",
            source_system="outlook"
        )
        
        semantic_units = []
        for i in range(3):
            sem_id = NodeIDGenerator.generate_semantic_unit_id(
                text=f"Semantic unit {i}",
                tenant_id=tenant_id,
                doc_id="doc_workflow_001",
                chunk_index=i
            )
            
            embedding = np.random.rand(3072).tolist()
            
            success = await pinecone_adapter.upsert_vector(
                vector_id=sem_id,
                embedding=embedding,
                metadata=doc_metadata,
                additional_metadata={
                    "node_type": "semantic_unit",
                    "chunk_index": i
                }
            )
            
            assert success is True
            semantic_units.append(sem_id)
        
        entities = []
        entity_names = ["John Smith", "Acme Corp", "Invoice #12345"]
        
        for name in entity_names:
            entity_id = NodeIDGenerator.generate_entity_id(
                entity_name=name,
                entity_type="ENTITY",
                tenant_id=tenant_id
            )
            
            embedding = np.random.rand(3072).tolist()
            
            success = await pinecone_adapter.upsert_vector(
                vector_id=entity_id,
                embedding=embedding,
                metadata=doc_metadata,
                additional_metadata={
                    "node_type": "entity",
                    "entity_name": name
                }
            )
            
            assert success is True
            entities.append(entity_id)
        
        await asyncio.sleep(5)
        
        query_embedding = np.random.rand(3072).tolist()
        
        results = await pinecone_adapter.search(
            query_embedding=query_embedding,
            filters={"tenant_id": tenant_id},
            top_k=10,
            namespace=tenant_id
        )
        
        assert len(results) == 6
        
        for result in results:
            assert result["metadata"]["tenant_id"] == tenant_id
            assert result["metadata"]["interaction_id"] == "int_12345678-1234-4567-8901-123456789012"
        
        print(f"Complete workflow test passed with {len(results)} vectors")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])
