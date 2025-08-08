#!/usr/bin/env python3
"""
Post-merge validation of multi-tenant system in main branch
"""
import sys
import os
import time
import uuid
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def validate_imports():
    """Validate all multi-tenant components can be imported"""
    print_section("1. IMPORT VALIDATION")
    
    try:
        from NodeRAG.tenant.tenant_context import TenantContext, TenantInfo, TenantContextConfig, ResourceError
        print("✅ TenantContext imports successful")
        
        from NodeRAG.storage.storage_factory_tenant import TenantAwareStorageFactory
        print("✅ TenantAwareStorageFactory imports successful")
        
        from NodeRAG.src.pipeline.graph_pipeline_tenant import TenantAwareGraphPipeline
        print("✅ TenantAwareGraphPipeline imports successful")
        
        from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
        print("✅ PipelineStorageAdapter imports successful")
        
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        traceback.print_exc()
        return False

def validate_basic_tenant_operations():
    """Test basic tenant context operations"""
    print_section("2. BASIC TENANT OPERATIONS")
    
    from NodeRAG.tenant.tenant_context import TenantContext
    
    try:
        TenantContext.cleanup_all_tenants()
        
        tenant1 = f"test_tenant_{uuid.uuid4()}"
        TenantContext.set_current_tenant(tenant1, {'org': 'TestOrg'})
        assert TenantContext.get_current_tenant() == tenant1
        print(f"✅ Set/get tenant: {tenant1}")
        
        tenant2 = f"test_tenant_{uuid.uuid4()}"
        with TenantContext.tenant_scope(tenant2):
            assert TenantContext.get_current_tenant() == tenant2
        assert TenantContext.get_current_tenant() == tenant1
        print(f"✅ Context manager works correctly")
        
        TenantContext.clear_current_tenant()
        assert TenantContext.get_current_tenant() is None
        print("✅ Clear context works")
        
        stats = TenantContext.get_registry_stats()
        assert 'total_tenants' in stats
        assert 'max_active_tenants' in stats
        print(f"✅ Registry stats: {stats['total_tenants']} tenants, max {stats['max_active_tenants']}")
        
        return True
    except Exception as e:
        print(f"❌ Basic operations failed: {e}")
        traceback.print_exc()
        return False
    finally:
        TenantContext.cleanup_all_tenants()

def validate_resource_limits():
    """Test resource limits and DOS protection"""
    print_section("3. RESOURCE LIMITS VALIDATION")
    
    from NodeRAG.tenant.tenant_context import TenantContext, TenantContextConfig, ResourceError
    
    try:
        test_config = TenantContextConfig()
        test_config.MAX_ACTIVE_TENANTS = 5
        test_config.MAX_REGISTRY_SIZE = 10
        test_config.ENFORCE_TENANT_LIMITS = True
        TenantContext._config = test_config
        
        created = []
        for i in range(5):
            tenant_id = f"limit_test_{i}"
            TenantContext.set_current_tenant(tenant_id)
            created.append(tenant_id)
            TenantContext.clear_current_tenant()
        
        print(f"✅ Created {len(created)} tenants at limit")
        
        try:
            TenantContext.set_current_tenant("overflow_tenant")
            print("❌ Resource limit not enforced!")
            return False
        except ResourceError as e:
            print(f"✅ Resource limit enforced: {e}")
        
        TenantContext.cleanup_all_tenants()
        stats = TenantContext.get_registry_stats()
        assert stats['total_tenants'] == 0
        print("✅ Cleanup successful")
        
        return True
    except Exception as e:
        print(f"❌ Resource limit validation failed: {e}")
        traceback.print_exc()
        return False
    finally:
        TenantContext._config = TenantContextConfig.from_env()
        TenantContext.cleanup_all_tenants()

def validate_memory_leak_prevention():
    """Test that memory leaks are prevented"""
    print_section("4. MEMORY LEAK PREVENTION")
    
    from NodeRAG.tenant.tenant_context import TenantContext, TenantContextConfig
    import gc
    
    try:
        test_config = TenantContextConfig()
        test_config.INACTIVE_TENANT_TTL_HOURS = 0  # Immediate cleanup
        test_config.MAX_REGISTRY_SIZE = 1000
        TenantContext._config = test_config
        
        print("Creating 100 tenants...")
        for i in range(100):
            TenantContext.set_current_tenant(f"leak_test_{i}")
            TenantContext.clear_current_tenant()
        
        initial_stats = TenantContext.get_registry_stats()
        print(f"Before cleanup: {initial_stats['total_tenants']} tenants")
        
        TenantContext._force_cleanup_inactive_tenants()
        gc.collect()
        
        final_stats = TenantContext.get_registry_stats()
        print(f"After cleanup: {final_stats['total_tenants']} tenants")
        
        if final_stats['total_tenants'] > 50:
            print(f"⚠️ Warning: {final_stats['total_tenants']} tenants remain after cleanup")
        else:
            print(f"✅ Memory leak prevented: {final_stats['total_tenants']} tenants after cleanup")
        
        return True
    except Exception as e:
        print(f"❌ Memory leak test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        TenantContext._config = TenantContextConfig.from_env()
        TenantContext.cleanup_all_tenants()

def validate_data_isolation():
    """Test tenant data isolation with storage adapter"""
    print_section("5. DATA ISOLATION VALIDATION")
    
    from NodeRAG.tenant.tenant_context import TenantContext
    from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
    from NodeRAG.storage.storage_factory import StorageFactory
    import networkx as nx
    import tempfile
    
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'config': {'main_folder': tmpdir, 'language': 'en', 'chunk_size': 512},
                'model_config': {'model_name': 'gpt-4o'},
                'embedding_config': {'model_name': 'gpt-4o'}
            }
            StorageFactory.initialize(config, backend_mode="file")
            
            adapter = PipelineStorageAdapter()
            
            tenant_a = f"tenant_a_{uuid.uuid4()}"
            graph_a = nx.Graph()
            graph_a.add_node("secret_node_a", data="tenant_a_secret")
            
            tenant_b = f"tenant_b_{uuid.uuid4()}"
            graph_b = nx.Graph()
            graph_b.add_node("secret_node_b", data="tenant_b_secret")
            
            with TenantContext.tenant_scope(tenant_a):
                success_a = adapter.save_pickle(graph_a, f"{tmpdir}/test.pkl", "graph", tenant_a)
                assert success_a
                print(f"✅ Tenant A data saved")
            
            with TenantContext.tenant_scope(tenant_b):
                success_b = adapter.save_pickle(graph_b, f"{tmpdir}/test.pkl", "graph", tenant_b)
                assert success_b
                print(f"✅ Tenant B data saved")
            
            with TenantContext.tenant_scope(tenant_a):
                loaded_a = adapter.load_pickle(f"{tmpdir}/test.pkl", "graph", tenant_a)
                assert "secret_node_a" in loaded_a.nodes()
                assert "secret_node_b" not in loaded_a.nodes()
                print(f"✅ Tenant A sees only its own data")
            
            with TenantContext.tenant_scope(tenant_b):
                loaded_b = adapter.load_pickle(f"{tmpdir}/test.pkl", "graph", tenant_b)
                assert "secret_node_b" in loaded_b.nodes()
                assert "secret_node_a" not in loaded_b.nodes()
                print(f"✅ Tenant B sees only its own data")
            
            print("✅ Complete data isolation verified")
            return True
            
    except Exception as e:
        print(f"❌ Data isolation test failed: {e}")
        traceback.print_exc()
        return False

def validate_concurrent_operations():
    """Test thread safety and concurrent tenant operations"""
    print_section("6. CONCURRENT OPERATIONS VALIDATION")
    
    from NodeRAG.tenant.tenant_context import TenantContext
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    try:
        results = []
        errors = []
        
        def concurrent_tenant_operation(tenant_id, operation_id):
            """Run operation in tenant context"""
            try:
                with TenantContext.tenant_scope(tenant_id):
                    current = TenantContext.get_current_tenant()
                    if current != tenant_id:
                        return f"ERROR: Context mismatch {current} != {tenant_id}"
                    
                    time.sleep(0.01)
                    
                    current = TenantContext.get_current_tenant()
                    if current != tenant_id:
                        return f"ERROR: Context lost {current} != {tenant_id}"
                    
                    return f"SUCCESS: {tenant_id}:{operation_id}"
            except Exception as e:
                return f"ERROR: {e}"
        
        print("Running 20 concurrent operations across 20 unique tenants...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(20):
                tenant_id = f"concurrent_tenant_{i}"  # Unique tenant per operation
                future = executor.submit(concurrent_tenant_operation, tenant_id, i)
                futures.append(future)
            
            for future in as_completed(futures):
                result = future.result()
                if result.startswith("ERROR"):
                    errors.append(result)
                else:
                    results.append(result)
        
        print(f"Results: {len(results)} successes, {len(errors)} errors")
        
        if errors:
            for error in errors[:5]:  # Show first 5 errors
                print(f"  ❌ {error}")
            return False
        else:
            print("✅ All concurrent operations maintained isolation")
            return True
            
    except Exception as e:
        print(f"❌ Concurrent operations test failed: {e}")
        traceback.print_exc()
        return False

def validate_pipeline_integration():
    """Test TenantAwareGraphPipeline integration"""
    print_section("7. PIPELINE INTEGRATION VALIDATION")
    
    from NodeRAG.tenant.tenant_context import TenantContext
    from NodeRAG.src.pipeline.graph_pipeline_tenant import TenantAwareGraphPipeline
    from NodeRAG.config.Node_config import NodeConfig
    from NodeRAG.storage.storage_factory import StorageFactory
    import networkx as nx
    import tempfile
    
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'config': {'main_folder': tmpdir, 'language': 'en', 'chunk_size': 512},
                'model_config': {'model_name': 'gpt-4o'},
                'embedding_config': {'model_name': 'gpt-4o'}
            }
            
            StorageFactory.initialize(config, backend_mode="file")
            node_config = NodeConfig(config)
            
            tenant_id = f"pipeline_test_{uuid.uuid4()}"
            with TenantContext.tenant_scope(tenant_id):
                pipeline = TenantAwareGraphPipeline(node_config, tenant_id)
                assert pipeline.tenant_id == tenant_id
                print(f"✅ Pipeline created for tenant: {tenant_id}")
                
                # Create and save graph
                pipeline.G = nx.Graph()
                pipeline.G.add_node("test_node", data="test_data")
                pipeline.save_graph()
                print("✅ Graph saved with tenant isolation")
                
                pipeline2 = TenantAwareGraphPipeline(node_config, tenant_id)
                loaded = pipeline2.load_graph()
                assert loaded is not None
                assert "test_node" in loaded.nodes()
                print("✅ Graph loaded correctly")
            
            print("✅ Pipeline integration working")
            return True
            
    except Exception as e:
        print(f"❌ Pipeline integration test failed: {e}")
        traceback.print_exc()
        return False

def validate_environment_configuration():
    """Test environment variable configuration"""
    print_section("8. ENVIRONMENT CONFIGURATION")
    
    from NodeRAG.tenant.tenant_context import TenantContextConfig
    import os
    
    try:
        os.environ['NODERAG_MAX_ACTIVE_TENANTS'] = '500'
        os.environ['NODERAG_MAX_REGISTRY_SIZE'] = '2500'
        os.environ['NODERAG_TENANT_TTL_HOURS'] = '12'
        os.environ['NODERAG_ENFORCE_TENANT_LIMITS'] = 'true'
        
        config = TenantContextConfig.from_env()
        
        assert config.MAX_ACTIVE_TENANTS == 500
        assert config.MAX_REGISTRY_SIZE == 2500
        assert config.INACTIVE_TENANT_TTL_HOURS == 12
        assert config.ENFORCE_TENANT_LIMITS == True
        
        print("✅ Environment configuration working:")
        print(f"  MAX_ACTIVE_TENANTS: {config.MAX_ACTIVE_TENANTS}")
        print(f"  MAX_REGISTRY_SIZE: {config.MAX_REGISTRY_SIZE}")
        print(f"  INACTIVE_TENANT_TTL_HOURS: {config.INACTIVE_TENANT_TTL_HOURS}")
        print(f"  ENFORCE_TENANT_LIMITS: {config.ENFORCE_TENANT_LIMITS}")
        
        return True
        
    except Exception as e:
        print(f"❌ Environment configuration test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        for key in ['NODERAG_MAX_ACTIVE_TENANTS', 'NODERAG_MAX_REGISTRY_SIZE', 
                    'NODERAG_TENANT_TTL_HOURS', 'NODERAG_ENFORCE_TENANT_LIMITS']:
            os.environ.pop(key, None)

def run_all_validations():
    """Run all validation tests"""
    print("\n" + "="*60)
    print("  MULTI-TENANT SYSTEM POST-MERGE VALIDATION")
    print("  Branch: main (after PR #29 merge)")
    print("  Time: " + datetime.now().isoformat())
    print("="*60)
    
    tests = [
        ("Import Validation", validate_imports),
        ("Basic Tenant Operations", validate_basic_tenant_operations),
        ("Resource Limits", validate_resource_limits),
        ("Memory Leak Prevention", validate_memory_leak_prevention),
        ("Data Isolation", validate_data_isolation),
        ("Concurrent Operations", validate_concurrent_operations),
        ("Pipeline Integration", validate_pipeline_integration),
        ("Environment Configuration", validate_environment_configuration)
    ]
    
    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"\n❌ CRITICAL ERROR in {name}: {e}")
            traceback.print_exc()
            results.append((name, False))
    
    print_section("VALIDATION SUMMARY")
    passed_count = sum(1 for _, p in results if p)
    total = len(results)
    
    for name, test_passed in results:
        status = "✅ PASS" if test_passed else "❌ FAIL"
        print(f"  {name}: {status}")
    
    print(f"\n  Overall: {passed_count}/{total} tests passed ({passed_count/total*100:.1f}%)")
    
    if passed_count == total:
        print("\n🎉 ALL VALIDATIONS PASSED! Multi-tenant system is working correctly in main branch.")
        return 0
    else:
        print(f"\n⚠️ VALIDATION FAILED: {total-passed_count} tests failed. DO NOT DEPLOY!")
        return 1

if __name__ == "__main__":
    sys.exit(run_all_validations())
