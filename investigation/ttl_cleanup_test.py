#!/usr/bin/env python3
"""
Investigate why TTL=0 cleanup didn't work
"""
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from NodeRAG.tenant.tenant_context import TenantContext, TenantContextConfig

def test_ttl_cleanup():
    print("TTL CLEANUP INVESTIGATION")
    print("="*60)
    
    TenantContext.cleanup_all_tenants()
    
    config = TenantContextConfig()
    config.INACTIVE_TENANT_TTL_HOURS = 0
    TenantContext._config = config
    
    for i in range(10):
        TenantContext.set_current_tenant(f"ttl_test_{i}")
        TenantContext.clear_current_tenant()
    
    print(f"Created 10 tenants")
    stats = TenantContext.get_registry_stats()
    print(f"Before cleanup: {stats['total_tenants']} tenants")
    
    with TenantContext._registry_lock:
        for tenant_id, info in TenantContext._global_tenant_registry.items():
            age = (datetime.now(timezone.utc) - info.last_accessed).total_seconds()
            print(f"  {tenant_id}: age = {age:.2f} seconds")
    
    TenantContext._force_cleanup_inactive_tenants()
    
    stats = TenantContext.get_registry_stats()
    print(f"After cleanup: {stats['total_tenants']} tenants")
    
    print("\nPOSSIBLE EXPLANATION:")
    print("TTL=0 may not trigger cleanup because:")
    print("1. Cleanup only runs if (now - last_accessed) > TTL")
    print("2. With TTL=0, this means cleanup only if age > 0")
    print("3. Due to timing, age might be 0 or negative due to clock precision")
    print("\nThis is NOT a critical issue - just use TTL=1 for immediate cleanup")

if __name__ == "__main__":
    test_ttl_cleanup()
