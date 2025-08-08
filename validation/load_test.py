#!/usr/bin/env python3
"""
Load test for multi-tenant system
"""
import sys
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from NodeRAG.tenant.tenant_context import TenantContext

def stress_test_tenant_system():
    """Stress test with many concurrent tenants"""
    print("Starting load test with 100 concurrent tenant operations...")
    
    start_time = time.time()
    errors = []
    
    def tenant_operation(i):
        try:
            tenant_id = f"load_test_{i}_{uuid.uuid4()}"
            with TenantContext.tenant_scope(tenant_id):
                time.sleep(0.01)
                current = TenantContext.get_current_tenant()
                assert current == tenant_id
                return True
        except Exception as e:
            return str(e)
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(tenant_operation, i) for i in range(100)]
        
        for future in as_completed(futures):
            result = future.result()
            if result != True:
                errors.append(result)
    
    duration = time.time() - start_time
    
    print(f"Load test completed in {duration:.2f} seconds")
    print(f"Success rate: {(100-len(errors))/100*100:.1f}%")
    
    if errors:
        print(f"Errors encountered: {len(errors)}")
        for error in errors[:5]:
            print(f"  - {error}")
        return False
    
    stats = TenantContext.get_registry_stats()
    print(f"Final registry state: {stats['total_tenants']} tenants")
    
    # Cleanup
    TenantContext._force_cleanup_inactive_tenants()
    
    return len(errors) == 0

if __name__ == "__main__":
    success = stress_test_tenant_system()
    exit(0 if success else 1)
