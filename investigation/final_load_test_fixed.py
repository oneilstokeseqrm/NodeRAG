#!/usr/bin/env python3
"""
Load test with correct expectations for atomic file operations
"""
import sys
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import networkx as nx
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
from NodeRAG.storage.storage_factory import StorageFactory

def test_concurrent_load_fixed():
    """Test concurrent operations with correct expectations"""
    print("="*60)
    print("LOAD TEST: Atomic Operations Verification")
    print("="*60)
    print("Testing 100 operations across 20 unique tenants...")
    print("Expected behavior: Last write wins for same tenant")
    print()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = {
            'config': {'main_folder': tmpdir, 'language': 'en', 'chunk_size': 512},
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        adapter = PipelineStorageAdapter()
        
        operation_results = []
        
        def tenant_operation(operation_id):
            """Perform save/load operation"""
            tenant_id = f"tenant_{operation_id % 20}"  # 20 unique tenants
            
            try:
                with TenantContext.tenant_scope(tenant_id):
                    graph = nx.Graph()
                    node_id = f"{tenant_id}_final_node_{operation_id}"
                    graph.add_node(node_id, data=f"data_{operation_id}", tenant=tenant_id)
                    
                    path = f"{tmpdir}/tenant_graph.pkl"
                    if not adapter.save_pickle(graph, path, "graph", tenant_id):
                        return {'success': False, 'error': 'Save failed', 'tenant': tenant_id}
                    
                    if operation_id < 20:  # First op for each tenant
                        time.sleep(0.01)  # Let any concurrent ops complete
                        
                        loaded = adapter.load_pickle(path, "graph", tenant_id)
                        if not loaded:
                            return {'success': False, 'error': 'Load failed', 'tenant': tenant_id}
                        
                        if not isinstance(loaded, nx.Graph):
                            return {'success': False, 'error': 'Corrupted graph', 'tenant': tenant_id}
                        
                        for node in loaded.nodes():
                            if 'tenant' in loaded.nodes[node]:
                                if loaded.nodes[node]['tenant'] != tenant_id:
                                    return {'success': False, 
                                           'error': f'Cross-tenant contamination', 
                                           'tenant': tenant_id}
                        
                        return {'success': True, 'tenant': tenant_id}
                    
                    return {'success': True, 'tenant': tenant_id}
                    
            except Exception as e:
                return {'success': False, 'error': str(e), 'tenant': tenant_id}
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(tenant_operation, i) for i in range(100)]
            
            for future in as_completed(futures):
                result = future.result()
                operation_results.append(result)
        
        duration = time.time() - start_time
        
        successes = [r for r in operation_results if r['success']]
        failures = [r for r in operation_results if not r['success']]
        
        print(f"Completed in {duration:.2f} seconds")
        print(f"Results: {len(successes)}/100 successful")
        
        if failures:
            print(f"\nFailures by type:")
            failure_types = {}
            for f in failures:
                error = f.get('error', 'Unknown')
                failure_types[error] = failure_types.get(error, 0) + 1
            
            for error_type, count in failure_types.items():
                print(f"  {error_type}: {count}")
            
            print(f"\nSample failures (first 3):")
            for f in failures[:3]:
                print(f"  Tenant {f['tenant']}: {f.get('error', 'Unknown')}")
        
        success_rate = len(successes) / len(operation_results) * 100
        
        print(f"\n{'='*60}")
        if success_rate == 100:
            print("✅ PASS: All operations completed successfully")
            print("   Atomic operations verified - no corruption detected")
        elif success_rate >= 95:
            print("⚠️  WARNING: Minor failures detected")
            print(f"   Success rate: {success_rate:.1f}%")
            print("   Review failures for environmental issues")
        else:
            print("❌ FAIL: Significant failures detected")
            print(f"   Success rate: {success_rate:.1f}%")
        
        return success_rate == 100

if __name__ == "__main__":
    success = test_concurrent_load_fixed()
    exit(0 if success else 1)
