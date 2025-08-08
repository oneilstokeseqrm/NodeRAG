"""
Tenant-aware storage factory extensions
"""
from typing import Optional, Dict, Any
import logging
from .storage_factory import StorageFactory
from ..tenant.tenant_context import TenantContext

logger = logging.getLogger(__name__)


class TenantAwareStorageFactory(StorageFactory):
    """Extended StorageFactory with tenant isolation"""
    
    @classmethod
    def get_tenant_graph_storage(cls):
        """Get graph storage with current tenant context"""
        tenant_id = TenantContext.get_current_tenant_or_default()
        storage = cls.get_graph_storage()
        
        # Wrap storage with tenant validation
        return TenantIsolatedGraphStorage(storage, tenant_id)
    
    @classmethod
    def get_tenant_embedding_storage(cls):
        """Get embedding storage with current tenant context"""
        tenant_id = TenantContext.get_current_tenant_or_default()
        namespace = TenantContext.get_tenant_namespace('embeddings')
        storage = cls.get_embedding_storage()
        
        # Wrap storage with tenant namespace
        return TenantIsolatedEmbeddingStorage(storage, namespace)
    
    @classmethod
    def validate_tenant_operation(cls, operation: str, resource_tenant: str) -> bool:
        """
        Validate that current tenant can perform operation
        
        Args:
            operation: Operation type (read, write, delete)
            resource_tenant: Tenant that owns the resource
            
        Returns:
            True if operation is allowed
        """
        current_tenant = TenantContext.get_current_tenant_or_default()
        
        # Default tenant has admin access
        if current_tenant == 'default':
            logger.debug(f"Default tenant performing {operation} on {resource_tenant} resource")
            return True
        
        # Tenants can only access their own resources
        if current_tenant != resource_tenant:
            logger.error(f"Tenant isolation violation: {current_tenant} attempted {operation} on {resource_tenant} resource")
            raise PermissionError(f"Tenant {current_tenant} cannot access {resource_tenant} resources")
        
        return True


class TenantIsolatedGraphStorage:
    """Wrapper for graph storage with tenant isolation"""
    
    def __init__(self, storage, tenant_id: str):
        self.storage = storage
        self.tenant_id = tenant_id
    
    def add_node(self, node_id: str, node_type: str, metadata: Any, properties: Dict = None):
        """Add node with tenant isolation"""
        # Ensure metadata includes tenant_id
        if hasattr(metadata, 'tenant_id'):
            metadata.tenant_id = self.tenant_id
        
        return self.storage.add_node(node_id, node_type, metadata, properties)
    
    def get_subgraph(self, tenant_id: Optional[str] = None):
        """Get subgraph for specific tenant"""
        # Use provided tenant_id or current tenant
        target_tenant = tenant_id or self.tenant_id
        TenantAwareStorageFactory.validate_tenant_operation('read', target_tenant)
        return self.storage.get_subgraph(target_tenant)
    
    def clear_tenant_data(self, tenant_id: Optional[str] = None):
        """Clear data for specific tenant"""
        target_tenant = tenant_id or self.tenant_id
        TenantAwareStorageFactory.validate_tenant_operation('delete', target_tenant)
        return self.storage.clear_tenant_data(target_tenant)
    
    def __getattr__(self, name):
        """Pass through other methods"""
        return getattr(self.storage, name)


class TenantIsolatedEmbeddingStorage:
    """Wrapper for embedding storage with tenant namespacing"""
    
    def __init__(self, storage, namespace: str):
        self.storage = storage
        self.namespace = namespace
    
    async def upsert_vector(self, vector_id: str, embedding: list, metadata: Any, namespace: Optional[str] = None):
        """Upsert vector with tenant namespace"""
        # Use provided namespace or tenant namespace
        target_namespace = namespace or self.namespace
        return await self.storage.upsert_vector(vector_id, embedding, metadata, target_namespace)
    
    async def search(self, embedding: list, filter: Dict = None, top_k: int = 10, 
                    namespace: Optional[str] = None, **kwargs):
        """Search vectors in tenant namespace"""
        target_namespace = namespace or self.namespace
        return await self.storage.search(embedding, filter, top_k, target_namespace, **kwargs)
    
    async def delete_namespace(self, namespace: Optional[str] = None):
        """Delete tenant namespace"""
        target_namespace = namespace or self.namespace
        current_tenant = TenantContext.get_current_tenant_or_default()
        
        # Validate tenant owns this namespace
        if not target_namespace.startswith(f"{current_tenant}_"):
            raise PermissionError(f"Tenant {current_tenant} cannot delete namespace {target_namespace}")
        
        return await self.storage.delete_namespace(target_namespace)
    
    def __getattr__(self, name):
        """Pass through other methods"""
        return getattr(self.storage, name)
