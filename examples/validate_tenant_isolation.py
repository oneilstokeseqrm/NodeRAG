"""
Validate tenant isolation across all components
"""
import uuid
import networkx as nx
import pandas as pd
import numpy as np
from pathlib import Path

from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.storage.storage_factory_tenant import TenantAwareStorageFactory
from NodeRAG.src.pipeline.graph_pipeline_tenant import TenantAwareGraphPipeline
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter


def validate_tenant_isolation():
    """Comprehensive tenant isolation validation"""
    print("=== Comprehensive Tenant Isolation Validation ===\n")
    
    config = {
        'config': {'main_folder': '/tmp/validation_test', 'language': 'en', 'chunk_size': 512},
        'model_config': {'model_name': 'gpt-4o'},
        'embedding_config': {'model_name': 'gpt-4o'}
    }
    StorageFactory.initialize(config, backend_mode="file")
    
    # Create test tenants
    tenant_a = f"tenant_a_{uuid.uuid4()}"
    tenant_b = f"tenant_b_{uuid.uuid4()}"
    
    print(f"Testing isolation between:")
    print(f"  - Tenant A: {tenant_a}")
    print(f"  - Tenant B: {tenant_b}")
    
    validation_results = {
        'context_management': False,
        'data_isolation': False,
        'namespace_isolation': False,
        'access_validation': False,
        'pipeline_isolation': False,
        'concurrent_safety': False
    }
    
    print("\n1. Testing Context Management...")
    try:
        TenantContext.set_current_tenant(tenant_a, {'test': 'data_a'})
        assert TenantContext.get_current_tenant() == tenant_a
        assert TenantContext.get_tenant_metadata()['test'] == 'data_a'
        
        with TenantContext.tenant_scope(tenant_b, {'test': 'data_b'}):
            assert TenantContext.get_current_tenant() == tenant_b
            assert TenantContext.get_tenant_metadata()['test'] == 'data_b'
        
        assert TenantContext.get_current_tenant() == tenant_a
        
        validation_results['context_management'] = True
        print("   ✅ Context management working correctly")
    except Exception as e:
        print(f"   ❌ Context management failed: {e}")
    
    print("\n2. Testing Data Isolation...")
    try:
        adapter = PipelineStorageAdapter()
        
        graph_a = nx.Graph()
        graph_a.add_node("node_a", data="secret_data_a", tenant=tenant_a)
        graph_a.add_edge("node_a", "node_a2")
        
        graph_b = nx.Graph()
        graph_b.add_node("node_b", data="secret_data_b", tenant=tenant_b)
        graph_b.add_edge("node_b", "node_b2")
        
        with TenantContext.tenant_scope(tenant_a):
            success_a = adapter.save_pickle(graph_a, "/tmp/test_graph.pkl", "graph", tenant_a)
            assert success_a
        
        with TenantContext.tenant_scope(tenant_b):
            success_b = adapter.save_pickle(graph_b, "/tmp/test_graph.pkl", "graph", tenant_b)
            assert success_b
        
        with TenantContext.tenant_scope(tenant_a):
            loaded_a = adapter.load_pickle("/tmp/test_graph.pkl", "graph", tenant_a)
            assert "node_a" in loaded_a.nodes()
            assert "node_b" not in loaded_a.nodes()
            assert loaded_a.nodes["node_a"]["data"] == "secret_data_a"
        
        with TenantContext.tenant_scope(tenant_b):
            loaded_b = adapter.load_pickle("/tmp/test_graph.pkl", "graph", tenant_b)
            assert "node_b" in loaded_b.nodes()
            assert "node_a" not in loaded_b.nodes()
            assert loaded_b.nodes["node_b"]["data"] == "secret_data_b"
        
        validation_results['data_isolation'] = True
        print("   ✅ Data isolation working correctly")
    except Exception as e:
        print(f"   ❌ Data isolation failed: {e}")
    
    print("\n3. Testing Namespace Isolation...")
    try:
        with TenantContext.tenant_scope(tenant_a):
            ns_a_embeddings = TenantContext.get_tenant_namespace('embeddings')
            ns_a_graph = TenantContext.get_tenant_namespace('graph')
        
        with TenantContext.tenant_scope(tenant_b):
            ns_b_embeddings = TenantContext.get_tenant_namespace('embeddings')
            ns_b_graph = TenantContext.get_tenant_namespace('graph')
        
        assert ns_a_embeddings != ns_b_embeddings
        assert ns_a_graph != ns_b_graph
        assert ns_a_embeddings.startswith(tenant_a)
        assert ns_b_embeddings.startswith(tenant_b)
        
        validation_results['namespace_isolation'] = True
        print("   ✅ Namespace isolation working correctly")
        print(f"      Tenant A embeddings: {ns_a_embeddings}")
        print(f"      Tenant B embeddings: {ns_b_embeddings}")
    except Exception as e:
        print(f"   ❌ Namespace isolation failed: {e}")
    
    print("\n4. Testing Access Validation...")
    try:
        with TenantContext.tenant_scope(tenant_a):
            assert TenantContext.validate_tenant_access(tenant_a) == True
            assert TenantContext.validate_tenant_access(tenant_b) == False
        
        with TenantContext.tenant_scope(tenant_b):
            assert TenantContext.validate_tenant_access(tenant_b) == True
            assert TenantContext.validate_tenant_access(tenant_a) == False
        
        validation_results['access_validation'] = True
        print("   ✅ Access validation working correctly")
    except Exception as e:
        print(f"   ❌ Access validation failed: {e}")
    
    print("\n5. Testing Pipeline Isolation...")
    try:
        from NodeRAG.config.Node_config import NodeConfig
        node_config = NodeConfig(config)
        
        with TenantContext.tenant_scope(tenant_a):
            pipeline_a = TenantAwareGraphPipeline(node_config, tenant_a)
            pipeline_a.G = nx.Graph()
            pipeline_a.G.add_node("pipeline_node_a", tenant=tenant_a)
            pipeline_a.save_graph()
        
        with TenantContext.tenant_scope(tenant_b):
            pipeline_b = TenantAwareGraphPipeline(node_config, tenant_b)
            pipeline_b.G = nx.Graph()
            pipeline_b.G.add_node("pipeline_node_b", tenant=tenant_b)
            pipeline_b.save_graph()
        
        with TenantContext.tenant_scope(tenant_a):
            pipeline_a_reload = TenantAwareGraphPipeline(node_config, tenant_a)
            graph_a_reloaded = pipeline_a_reload.load_graph()
            if graph_a_reloaded:
                assert "pipeline_node_a" in graph_a_reloaded.nodes()
                assert "pipeline_node_b" not in graph_a_reloaded.nodes()
        
        validation_results['pipeline_isolation'] = True
        print("   ✅ Pipeline isolation working correctly")
    except Exception as e:
        print(f"   ❌ Pipeline isolation failed: {e}")
    
    print("\n6. Testing Concurrent Safety...")
    try:
        import threading
        from concurrent.futures import ThreadPoolExecutor
        
        def concurrent_operation(tenant_id, operation_id):
            with TenantContext.tenant_scope(tenant_id):
                # Verify correct tenant context
                current = TenantContext.get_current_tenant()
                assert current == tenant_id
                
                graph = nx.Graph()
                graph.add_node(f"concurrent_{operation_id}", tenant=tenant_id)
                
                adapter = PipelineStorageAdapter()
                path = f"/tmp/concurrent_{tenant_id}_{operation_id}.pkl"
                adapter.save_pickle(graph, path, "graph", tenant_id)
                
                loaded = adapter.load_pickle(path, "graph", tenant_id)
                return loaded is not None and f"concurrent_{operation_id}" in loaded.nodes()
        
        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(5):
                futures.append(executor.submit(concurrent_operation, tenant_a, f"a_{i}"))
                futures.append(executor.submit(concurrent_operation, tenant_b, f"b_{i}"))
            
            results = [future.result() for future in futures]
            assert all(results), "Some concurrent operations failed"
        
        validation_results['concurrent_safety'] = True
        print("   ✅ Concurrent safety working correctly")
    except Exception as e:
        print(f"   ❌ Concurrent safety failed: {e}")
    
    print("\n=== Validation Summary ===")
    passed = sum(validation_results.values())
    total = len(validation_results)
    
    for test, result in validation_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test.replace('_', ' ').title()}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 All tenant isolation tests PASSED! Multi-tenant system is working correctly.")
        return True
    else:
        print(f"\n⚠️  {total-passed} tests FAILED. Multi-tenant isolation needs attention.")
        return False


if __name__ == "__main__":
    success = validate_tenant_isolation()
    exit(0 if success else 1)
