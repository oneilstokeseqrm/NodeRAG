"""
Tenant context management for multi-tenant isolation (FIXED VERSION)
"""
import threading
import uuid
import weakref
from typing import Optional, Dict, Any, List, Set
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class TenantContextConfig:
    """Configuration for tenant context management"""
    
    MAX_ACTIVE_TENANTS = 1000  # Maximum concurrent tenants
    MAX_REGISTRY_SIZE = 5000   # Maximum registry entries
    
    INACTIVE_TENANT_TTL_HOURS = 24  # Remove inactive tenants after 24 hours
    CLEANUP_INTERVAL_MINUTES = 60   # Run cleanup every hour
    
    ENFORCE_TENANT_LIMITS = True
    LOG_VIOLATIONS = True
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        import os
        
        config = cls()
        config.MAX_ACTIVE_TENANTS = int(os.getenv('NODERAG_MAX_ACTIVE_TENANTS', '1000'))
        config.MAX_REGISTRY_SIZE = int(os.getenv('NODERAG_MAX_REGISTRY_SIZE', '5000'))
        config.INACTIVE_TENANT_TTL_HOURS = int(os.getenv('NODERAG_TENANT_TTL_HOURS', '24'))
        config.ENFORCE_TENANT_LIMITS = os.getenv('NODERAG_ENFORCE_TENANT_LIMITS', 'true').lower() == 'true'
        
        return config


class TenantContext:
    """Thread-local tenant context management with resource protection"""
    
    _thread_local = threading.local()
    _global_tenant_registry: Dict[str, 'TenantInfo'] = {}
    _active_contexts: Dict[int, str] = {}  # Map thread ID to tenant ID for active contexts
    _registry_lock = threading.Lock()
    _last_cleanup = datetime.now(timezone.utc)
    _config = TenantContextConfig.from_env()
    
    @classmethod
    def set_current_tenant(cls, tenant_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Set the current tenant for this thread/request with resource protection
        
        Args:
            tenant_id: Unique tenant identifier
            metadata: Optional tenant metadata (org_name, tier, etc.)
            
        Raises:
            ResourceError: If tenant limits are exceeded
            ValueError: If tenant ID format is invalid
        """
        if not tenant_id:
            raise ValueError("Tenant ID cannot be empty")
        
        # Validate tenant ID format
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', tenant_id):
            raise ValueError(f"Invalid tenant ID format: {tenant_id}")
        
        if cls._config.ENFORCE_TENANT_LIMITS:
            with cls._registry_lock:
                cls._cleanup_inactive_tenants_if_needed()
                
                if tenant_id not in cls._global_tenant_registry:
                    if len(cls._global_tenant_registry) >= cls._config.MAX_ACTIVE_TENANTS:
                        cls._force_cleanup_inactive_tenants()
                        
                        if len(cls._global_tenant_registry) >= cls._config.MAX_ACTIVE_TENANTS:
                            raise ResourceError(f"Maximum active tenants ({cls._config.MAX_ACTIVE_TENANTS}) exceeded")
                
                if len(cls._global_tenant_registry) >= cls._config.MAX_REGISTRY_SIZE:
                    cls._force_cleanup_inactive_tenants()
                    
                    if len(cls._global_tenant_registry) >= cls._config.MAX_REGISTRY_SIZE:
                        raise ResourceError(f"Maximum registry size ({cls._config.MAX_REGISTRY_SIZE}) exceeded")
        
        cls._thread_local.tenant_id = tenant_id
        cls._thread_local.metadata = metadata or {}
        cls._thread_local.session_id = str(uuid.uuid4())
        cls._thread_local.started_at = datetime.now(timezone.utc)
        
        thread_id = threading.get_ident()
        with cls._registry_lock:
            cls._active_contexts[thread_id] = tenant_id
            
            # Register tenant in global registry
            if tenant_id not in cls._global_tenant_registry:
                tenant_info = TenantInfo(tenant_id, metadata)
                cls._global_tenant_registry[tenant_id] = tenant_info
            else:
                cls._global_tenant_registry[tenant_id].record_access()
        
        logger.info(f"Set tenant context: {tenant_id} (session: {cls._thread_local.session_id})")
    
    @classmethod
    def _cleanup_inactive_tenants_if_needed(cls):
        """Check if cleanup is needed based on interval"""
        now = datetime.now(timezone.utc)
        if (now - cls._last_cleanup).total_seconds() > cls._config.CLEANUP_INTERVAL_MINUTES * 60:
            cls._force_cleanup_inactive_tenants()
    
    @classmethod
    def _force_cleanup_inactive_tenants(cls):
        """Force cleanup of inactive tenants"""
        now = datetime.now(timezone.utc)
        ttl = timedelta(hours=cls._config.INACTIVE_TENANT_TTL_HOURS)
        
        active_threads = set(threading.enumerate())
        dead_thread_ids = []
        for thread_id in cls._active_contexts.keys():
            thread_alive = any(t.ident == thread_id for t in active_threads)
            if not thread_alive:
                dead_thread_ids.append(thread_id)
        
        for thread_id in dead_thread_ids:
            cls._active_contexts.pop(thread_id, None)
        
        tenants_to_remove = []
        for tenant_id, info in cls._global_tenant_registry.items():
            if cls._config.INACTIVE_TENANT_TTL_HOURS > 0 and (now - info.last_accessed) > ttl:
                tenants_to_remove.append(tenant_id)
        
        for tenant_id in tenants_to_remove:
            del cls._global_tenant_registry[tenant_id]
            logger.info(f"Cleaned up inactive tenant: {tenant_id}")
        
        cls._last_cleanup = now
        
        if tenants_to_remove or dead_thread_ids:
            logger.info(f"Tenant cleanup removed {len(tenants_to_remove)} inactive tenants and {len(dead_thread_ids)} dead thread contexts")
    
    @classmethod
    def get_current_tenant(cls) -> Optional[str]:
        """Get the current tenant ID for this thread"""
        return getattr(cls._thread_local, 'tenant_id', None)
    
    @classmethod
    def get_current_tenant_or_default(cls) -> str:
        """Get current tenant ID or return 'default'"""
        return cls.get_current_tenant() or 'default'
    
    @classmethod
    def get_tenant_metadata(cls) -> Dict[str, Any]:
        """Get current tenant metadata"""
        return getattr(cls._thread_local, 'metadata', {})
    
    @classmethod
    def get_session_id(cls) -> Optional[str]:
        """Get current tenant session ID"""
        return getattr(cls._thread_local, 'session_id', None)
    
    @classmethod
    def clear_current_tenant(cls) -> None:
        """Clear the current tenant context"""
        thread_id = threading.get_ident()
        
        if hasattr(cls._thread_local, 'tenant_id'):
            tenant_id = cls._thread_local.tenant_id
            logger.info(f"Clearing tenant context: {tenant_id}")
            del cls._thread_local.tenant_id
            
            with cls._registry_lock:
                cls._active_contexts.pop(thread_id, None)
        
        if hasattr(cls._thread_local, 'metadata'):
            del cls._thread_local.metadata
        if hasattr(cls._thread_local, 'session_id'):
            del cls._thread_local.session_id
        if hasattr(cls._thread_local, 'started_at'):
            del cls._thread_local.started_at
    
    @classmethod
    def require_tenant(cls) -> str:
        """
        Get current tenant ID or raise error if not set
        
        Raises:
            RuntimeError: If no tenant context is set
        """
        tenant_id = cls.get_current_tenant()
        if not tenant_id:
            raise RuntimeError("No tenant context set. Call TenantContext.set_current_tenant() first.")
        return tenant_id
    
    @classmethod
    @contextmanager
    def tenant_scope(cls, tenant_id: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Context manager for tenant-scoped operations
        
        Usage:
            with TenantContext.tenant_scope('tenant123'):
                # All operations here are scoped to tenant123
                pipeline.run()
        """
        previous_tenant = cls.get_current_tenant()
        previous_metadata = cls.get_tenant_metadata() if previous_tenant else None
        
        try:
            cls.set_current_tenant(tenant_id, metadata)
            yield tenant_id
        finally:
            cls.clear_current_tenant()
            if previous_tenant:
                cls.set_current_tenant(previous_tenant, previous_metadata)
    
    @classmethod
    def get_registry_stats(cls) -> Dict[str, Any]:
        """Get statistics about the tenant registry"""
        with cls._registry_lock:
            active_tenants = set(cls._active_contexts.values())
            active_count = len(active_tenants)
            
            return {
                'total_tenants': len(cls._global_tenant_registry),
                'active_tenants': active_count,
                'max_active_tenants': cls._config.MAX_ACTIVE_TENANTS,
                'max_registry_size': cls._config.MAX_REGISTRY_SIZE,
                'last_cleanup': cls._last_cleanup.isoformat()
            }
    
    @classmethod
    def cleanup_all_tenants(cls):
        """Force cleanup of all tenant data (for testing/admin use)"""
        with cls._registry_lock:
            cls._global_tenant_registry.clear()
            cls._active_contexts.clear()
            logger.info("Cleared all tenant data from registry")
    
    @classmethod
    def get_all_registered_tenants(cls) -> List[str]:
        """Get list of all registered tenant IDs"""
        with cls._registry_lock:
            return list(cls._global_tenant_registry.keys())
    
    @classmethod
    def validate_tenant_access(cls, resource_tenant_id: str) -> bool:
        """
        Validate that current tenant can access a resource
        
        Args:
            resource_tenant_id: The tenant ID that owns the resource
            
        Returns:
            True if access is allowed, False otherwise
        """
        current_tenant = cls.get_current_tenant()
        
        # No tenant context means default/admin access
        if not current_tenant:
            logger.warning("No tenant context for access validation")
            return True
        
        # Tenant can only access their own resources
        if current_tenant != resource_tenant_id:
            logger.warning(f"Tenant {current_tenant} attempted to access {resource_tenant_id} resource")
            return False
        
        return True
    
    @classmethod
    def get_tenant_namespace(cls, component: str = 'default') -> str:
        """
        Get namespace for tenant-specific storage
        
        Args:
            component: Component type (e.g., 'embeddings', 'graph')
            
        Returns:
            Tenant-scoped namespace string
        """
        tenant_id = cls.get_current_tenant_or_default()
        return f"{tenant_id}_{component}"


class ResourceError(Exception):
    """Raised when resource limits are exceeded"""
    pass


class TenantInfo:
    """Information about a registered tenant with access tracking"""
    
    def __init__(self, tenant_id: str, metadata: Optional[Dict[str, Any]] = None):
        self.tenant_id = tenant_id
        self.metadata = metadata or {}
        self.created_at = datetime.now(timezone.utc)
        self.last_accessed = datetime.now(timezone.utc)
        self.access_count = 0
    
    def record_access(self):
        """Record an access to this tenant's resources"""
        self.last_accessed = datetime.now(timezone.utc)
        self.access_count += 1
