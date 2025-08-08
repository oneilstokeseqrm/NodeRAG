"""
Test core tenant functionality without external dependencies
"""
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor

print("=== Testing Core Tenant Context ===")

try:
    from NodeRAG.tenant.tenant_context import TenantContext, TenantInfo
    print("✅ TenantContext imported successfully")
    
    tenant1 = f"test_tenant_{uuid.uuid4()}"
    tenant2 = f"test_tenant_{uuid.uuid4()}"
    
    TenantContext.set_current_tenant(tenant1, {'org': 'TestOrg1'})
    assert TenantContext.get_current_tenant() == tenant1
    assert TenantContext.get_tenant_metadata()['org'] == 'TestOrg1'
    print("✅ Basic tenant context operations work")
    
    with TenantContext.tenant_scope(tenant2, {'org': 'TestOrg2'}):
        assert TenantContext.get_current_tenant() == tenant2
        assert TenantContext.get_tenant_metadata()['org'] == 'TestOrg2'
    
    assert TenantContext.get_current_tenant() == tenant1
    print("✅ Tenant scope context manager works")
    
    assert TenantContext.validate_tenant_access(tenant1) == True
    assert TenantContext.validate_tenant_access(tenant2) == False
    print("✅ Access validation works")
    
    ns1 = TenantContext.get_tenant_namespace('embeddings')
    assert ns1.startswith(tenant1)
    print("✅ Namespace generation works")
    
    print("\n=== Testing Concurrent Operations ===")
    
    def concurrent_test(tenant_id, operation_id):
        """Test concurrent tenant operations"""
        with TenantContext.tenant_scope(tenant_id):
            current = TenantContext.get_current_tenant()
            assert current == tenant_id, f"Context mismatch: {current} != {tenant_id}"
            return f"{tenant_id}_{operation_id}"
    
    # Run concurrent operations
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i in range(10):
            tenant = tenant1 if i % 2 == 0 else tenant2
            futures.append(executor.submit(concurrent_test, tenant, i))
        
        results = [future.result() for future in futures]
        assert len(results) == 10
        print("✅ Concurrent operations maintain isolation")
    
    print("\n🎉 All core tenant functionality tests PASSED!")
    
except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
