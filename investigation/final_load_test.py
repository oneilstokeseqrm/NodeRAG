#!/usr/bin/env python3
"""Final load test to verify race condition fix"""
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor
import networkx as nx
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
from NodeRAG.storage.storage_factory import StorageFactory

def test_concurrent_load():
    """Test 100 concurrent operations"""
    print("Running 100 concurrent save/load operations...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = {
            'config': {'main_folder': tmpdir, 'language': 'en', 'chunk_size': 512},
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        adapter = PipelineStorageAdapter()
        
        errors = []
        
        def operation(i):
            tenant_id = f"tenant_{i % 10}"
            try:
                with TenantContext.tenant_scope(tenant_id):
                    graph = nx.Graph()
                    node_id = f"{tenant_id}_node_{i}"
                    graph.add_node(node_id, data=f"data_{i}")
                    
                    path = f"{tmpdir}/test.pkl"
                    if not adapter.save_pickle(graph, path, "graph", tenant_id):
                        return f"Save failed for {tenant_id}"
                    
                    loaded = adapter.load_pickle(path, "graph", tenant_id)
                    if not loaded or node_id not in loaded.nodes():
                        return f"Load verification failed for {tenant_id}"
                    
                    return "SUCCESS"
            except Exception as e:
                return f"Error: {e}"
        
        start = time.time()
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(operation, i) for i in range(100)]
            results = [f.result() for f in futures]
        
        duration = time.time() - start
        successes = sum(1 for r in results if r == "SUCCESS")
        
        print(f"Completed in {duration:.2f}s")
        print(f"Success rate: {successes}/100 ({successes}%)")
        
        if successes < 100:
            print("Failures:", [r for r in results if r != "SUCCESS"][:5])
        
        return successes == 100

if __name__ == "__main__":
    success = test_concurrent_load()
    exit(0 if success else 1)
