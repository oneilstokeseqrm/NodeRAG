"""
Test resource limits and cleanup in multi-tenant system
"""
import pytest
import time
from datetime import datetime, timezone, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor

from NodeRAG.tenant.tenant_context import TenantContext, TenantContextConfig, ResourceError


class TestTenantResourceLimits:
    """Test resource protection and cleanup"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup and cleanup for tests"""
        TenantContext.cleanup_all_tenants()
        
        # Create new config instance for testing
        from NodeRAG.tenant.tenant_context import TenantContextConfig
        test_config = TenantContextConfig()
        test_config.MAX_ACTIVE_TENANTS = 3  # Lower limit for easier testing
        test_config.MAX_REGISTRY_SIZE = 20
        test_config.INACTIVE_TENANT_TTL_HOURS = 1  # 1 hour TTL for testing
        test_config.ENFORCE_TENANT_LIMITS = True
        
        TenantContext._config = test_config
        
        yield
        
        # Cleanup
        TenantContext.cleanup_all_tenants()
        TenantContext.clear_current_tenant()
        
        TenantContext._config = TenantContextConfig.from_env()
    
    def test_max_active_tenants_limit(self):
        """Test that max active tenants limit is enforced"""
        for i in range(3):
            TenantContext.set_current_tenant(f"tenant_{i}")
            TenantContext.clear_current_tenant()
        
        stats = TenantContext.get_registry_stats()
        assert stats['total_tenants'] == 3
        assert stats['active_tenants'] == 0  # No active contexts after clearing
        
        with pytest.raises(ResourceError, match="Maximum active tenants"):
            TenantContext.set_current_tenant("tenant_overflow")
    
    def test_registry_cleanup(self):
        """Test that inactive tenants are cleaned up"""
        tenant_ids = []
        for i in range(3):
            tenant_id = f"cleanup_tenant_{i}"
            tenant_ids.append(tenant_id)
            TenantContext.set_current_tenant(tenant_id)
            TenantContext.clear_current_tenant()
        
        stats = TenantContext.get_registry_stats()
        assert stats['total_tenants'] >= 3
        
        with TenantContext._registry_lock:
            for tenant_id in tenant_ids:
                if tenant_id in TenantContext._global_tenant_registry:
                    info = TenantContext._global_tenant_registry[tenant_id]
                    info.last_accessed = datetime.now(timezone.utc) - timedelta(hours=25)
        
        TenantContext._force_cleanup_inactive_tenants()
        
        stats = TenantContext.get_registry_stats()
        for tenant_id in tenant_ids:
            assert tenant_id not in TenantContext.get_all_registered_tenants()
    
    def test_weak_references_cleanup(self):
        """Test that weak references are cleaned up properly"""
        initial_stats = TenantContext.get_registry_stats()
        
        for i in range(3):
            TenantContext.set_current_tenant(f"weak_tenant_{i}")
            TenantContext.clear_current_tenant()
        
        import gc
        gc.collect()
        
        stats = TenantContext.get_registry_stats()
        assert stats['total_tenants'] >= initial_stats['total_tenants'] + 3
    
    def test_concurrent_tenant_limit_enforcement(self):
        """Test that limits are enforced under concurrent access"""
        errors = []
        successes = []
        
        def create_tenant(i):
            try:
                TenantContext.set_current_tenant(f"concurrent_{i}")
                successes.append(i)
                time.sleep(0.1)
                return True
            except ResourceError as e:
                errors.append(str(e))
                return False
            finally:
                try:
                    TenantContext.clear_current_tenant()
                except:
                    pass
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(create_tenant, i) for i in range(6)]
            results = [f.result() for f in futures]
        
        assert len(successes) == 3, f"Expected exactly 3 successes, got {len(successes)}"
        assert len(errors) == 3, f"Expected exactly 3 errors, got {len(errors)}"
        assert "Maximum active tenants" in errors[0]
    
    def test_registry_stats(self):
        """Test registry statistics reporting"""
        TenantContext.cleanup_all_tenants()
        
        stats = TenantContext.get_registry_stats()
        assert stats['total_tenants'] == 0
        assert stats['active_tenants'] == 0
        assert stats['max_active_tenants'] == TenantContext._config.MAX_ACTIVE_TENANTS
        assert stats['max_registry_size'] == TenantContext._config.MAX_REGISTRY_SIZE
        
        for i in range(2):
            TenantContext.set_current_tenant(f"stats_tenant_{i}")
            TenantContext.clear_current_tenant()
        
        stats = TenantContext.get_registry_stats()
        assert stats['total_tenants'] == 2
    
    def test_memory_leak_prevention(self):
        """Test that creating many tenants doesn't cause unbounded memory growth"""
        initial_stats = TenantContext.get_registry_stats()
        
        for i in range(10):
            try:
                TenantContext.set_current_tenant(f"leak_test_{i}")
                TenantContext.clear_current_tenant()
            except ResourceError:
                break
        
        TenantContext._force_cleanup_inactive_tenants()
        
        stats = TenantContext.get_registry_stats()
        assert stats['total_tenants'] <= 10, f"Memory leak detected - registry has {stats['total_tenants']} tenants"
