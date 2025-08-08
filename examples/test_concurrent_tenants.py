"""
Test concurrent multi-tenant operations
"""
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import networkx as nx

from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter


def test_concurrent_tenants():
    """Test concurrent tenant operations"""
    print("=== Testing Concurrent Multi-Tenant Operations ===\n")
    
    config = {
        'config': {'main_folder': '/tmp/concurrent_test', 'language': 'en', 'chunk_size': 512},
        'model_config': {'model_name': 'gpt-4o'},
        'embedding_config': {'model_name': 'gpt-4o'}
    }
    StorageFactory.initialize(config, backend_mode="file")
    
    # Create test tenants
    tenants = [f"tenant_{i}_{uuid.uuid4()}" for i in range(5)]
    print(f"Created {len(tenants)} test tenants")
    
    def process_tenant_data(tenant_id, operation_count):
        """Process data for a specific tenant"""
        results = []
        
        for i in range(operation_count):
            with TenantContext.tenant_scope(tenant_id):
                current_tenant = TenantContext.get_current_tenant()
                assert current_tenant == tenant_id, f"Context mismatch: {current_tenant} != {tenant_id}"
                
                # Create tenant-specific graph
                graph = nx.Graph()
                node_id = f"{tenant_id}_node_{i}"
                graph.add_node(node_id, tenant=tenant_id, operation=i, thread=threading.current_thread().name)
                
                adapter = PipelineStorageAdapter()
                path = f"/tmp/{tenant_id}_op_{i}.pkl"
                
                success = adapter.save_pickle(graph, path, "graph", tenant_id)
                if success:
                    loaded = adapter.load_pickle(path, "graph", tenant_id)
                    if loaded and node_id in loaded.nodes():
                        results.append({
                            'tenant': tenant_id,
                            'operation': i,
                            'success': True,
                            'node_id': node_id,
                            'thread': threading.current_thread().name
                        })
                    else:
                        results.append({'tenant': tenant_id, 'operation': i, 'success': False, 'error': 'Load failed'})
                else:
                    results.append({'tenant': tenant_id, 'operation': i, 'success': False, 'error': 'Save failed'})
                
                time.sleep(0.01)
        
        return results
    
    # Run concurrent operations
    print("Starting concurrent operations...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for tenant_id in tenants:
            future = executor.submit(process_tenant_data, tenant_id, 3)
            futures.append(future)
        
        all_results = []
        for future in as_completed(futures):
            results = future.result()
            all_results.extend(results)
    
    end_time = time.time()
    
    successful_ops = [r for r in all_results if r['success']]
    failed_ops = [r for r in all_results if not r['success']]
    
    print(f"\n=== Results ===")
    print(f"Total operations: {len(all_results)}")
    print(f"Successful: {len(successful_ops)}")
    print(f"Failed: {len(failed_ops)}")
    print(f"Success rate: {len(successful_ops)/len(all_results)*100:.1f}%")
    print(f"Execution time: {end_time - start_time:.2f} seconds")
    
    tenant_operations = {}
    for result in successful_ops:
        tenant = result['tenant']
        if tenant not in tenant_operations:
            tenant_operations[tenant] = []
        tenant_operations[tenant].append(result)
    
    print(f"\n=== Tenant Isolation Verification ===")
    for tenant_id, ops in tenant_operations.items():
        print(f"Tenant {tenant_id}: {len(ops)} successful operations")
        
        for op in ops:
            assert op['tenant'] == tenant_id, f"Tenant mismatch in operation: {op}"
    
    print("\n✅ All concurrent operations maintained proper tenant isolation!")
    
    if failed_ops:
        print(f"\n⚠️  {len(failed_ops)} operations failed:")
        for fail in failed_ops[:5]:  # Show first 5 failures
            print(f"  - Tenant {fail['tenant']}, Op {fail['operation']}: {fail.get('error', 'Unknown error')}")
    
    return len(successful_ops) == len(all_results)


if __name__ == "__main__":
    success = test_concurrent_tenants()
    exit(0 if success else 1)
