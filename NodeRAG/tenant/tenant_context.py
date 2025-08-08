"""
Tenant context management for multi-tenant isolation
"""
import threading
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class TenantContext:
    """Thread-local tenant context management"""
    
    _thread_local = threading.local()
    _global_tenant_registry: Dict[str, 'TenantInfo'] = {}
    
    @classmethod
    def set_current_tenant(cls, tenant_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Set the current tenant for this thread/request
        
        Args:
            tenant_id: Unique tenant identifier
            metadata: Optional tenant metadata (org_name, tier, etc.)
        """
        if not tenant_id:
            raise ValueError("Tenant ID cannot be empty")
        
        # Validate tenant ID format (alphanumeric, hyphens, underscores)
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', tenant_id):
            raise ValueError(f"Invalid tenant ID format: {tenant_id}")
        
        cls._thread_local.tenant_id = tenant_id
        cls._thread_local.metadata = metadata or {}
        cls._thread_local.session_id = str(uuid.uuid4())
        cls._thread_local.started_at = datetime.now(timezone.utc)
        
        # Register tenant if new
        if tenant_id not in cls._global_tenant_registry:
            cls._global_tenant_registry[tenant_id] = TenantInfo(tenant_id, metadata)
        
        logger.info(f"Set tenant context: {tenant_id} (session: {cls._thread_local.session_id})")
    
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
        if hasattr(cls._thread_local, 'tenant_id'):
            logger.info(f"Clearing tenant context: {cls._thread_local.tenant_id}")
            del cls._thread_local.tenant_id
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
    def get_all_registered_tenants(cls) -> List[str]:
        """Get list of all registered tenant IDs"""
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


class TenantInfo:
    """Information about a registered tenant"""
    
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
