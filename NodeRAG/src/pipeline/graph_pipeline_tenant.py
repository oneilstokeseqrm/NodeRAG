"""
Tenant-aware Graph_pipeline for multi-tenant operations
"""
from typing import Optional, Dict, Any
import logging

from .graph_pipeline_v2 import Graph_pipeline as Graph_pipeline_v2
from ...tenant.tenant_context import TenantContext
from ...storage.storage_factory_tenant import TenantAwareStorageFactory

logger = logging.getLogger(__name__)


class TenantAwareGraphPipeline(Graph_pipeline_v2):
    """Graph pipeline with tenant isolation"""
    
    def __init__(self, config, tenant_id: Optional[str] = None):
        """
        Initialize tenant-aware pipeline
        
        Args:
            config: Pipeline configuration
            tenant_id: Optional tenant ID (can be set later via context)
        """
        # Set tenant context first, before parent initialization
        if tenant_id:
            TenantContext.set_current_tenant(tenant_id)
            self.tenant_id = tenant_id
        else:
            self.tenant_id = TenantContext.get_current_tenant_or_default()
        
        # Initialize storage adapter before parent constructor
        from .storage_adapter import PipelineStorageAdapter
        self.storage_adapter = PipelineStorageAdapter()
        
        # Now call parent constructor which may call load_graph()
        super().__init__(config)
        
        logger.info(f"Initialized tenant-aware pipeline for tenant: {self.tenant_id}")
    
    def _get_tenant_id(self) -> str:
        """Get current tenant ID"""
        return TenantContext.get_current_tenant_or_default()
    
    def save_graph(self):
        """Save graph with tenant isolation"""
        tenant_id = self._get_tenant_id()
        logger.info(f"Saving graph for tenant: {tenant_id}")
        
        # Ensure storage adapter uses tenant context
        if hasattr(self.storage_adapter, 'save_pickle'):
            success = self.storage_adapter.save_pickle(
                self.G, 
                self.config.graph_path, 
                component_type='graph',
                tenant_id=tenant_id
            )
            if success:
                if hasattr(self, 'console'):
                    self.console.print(f'[green]Graph stored for tenant {tenant_id}[/green]')
            else:
                if hasattr(self, 'console'):
                    self.console.print(f'[red]Failed to store graph for tenant {tenant_id}[/red]')
        else:
            super().save_graph()
    
    def load_graph(self):
        """Load graph with tenant isolation"""
        tenant_id = self._get_tenant_id()
        logger.info(f"Loading graph for tenant: {tenant_id}")
        
        if hasattr(self.storage_adapter, 'load_pickle'):
            graph = self.storage_adapter.load_pickle(
                self.config.graph_path,
                component_type='graph',
                tenant_id=tenant_id
            )
            
            if graph is not None:
                self.G = graph
                if hasattr(self, 'console'):
                    self.console.print(f'[green]Graph loaded for tenant {tenant_id}[/green]')
                return graph
        
        return super().load_graph()
    
    def save_embeddings(self, embeddings):
        """Save embeddings with tenant namespace"""
        tenant_id = self._get_tenant_id()
        namespace = TenantContext.get_tenant_namespace('embeddings')
        logger.info(f"Saving embeddings to namespace: {namespace}")
        
        import pandas as pd
        if not isinstance(embeddings, pd.DataFrame):
            embeddings = pd.DataFrame(embeddings)
        
        return self.storage_adapter.save_parquet(
            embeddings,
            self.config.embedding_path,
            component_type='embeddings',
            namespace=namespace
        )
    
    def load_embeddings(self):
        """Load embeddings from tenant namespace"""
        namespace = TenantContext.get_tenant_namespace('embeddings')
        logger.info(f"Loading embeddings from namespace: {namespace}")
        
        return self.storage_adapter.load_parquet(
            self.config.embedding_path,
            component_type='embeddings',
            namespace=namespace
        )
    
    def validate_tenant_access(self, resource_tenant_id: str) -> bool:
        """Validate that current tenant can access a resource"""
        return TenantContext.validate_tenant_access(resource_tenant_id)
    
    @classmethod
    def run_for_tenant(cls, config, tenant_id: str, operation: str = 'build', **kwargs):
        """
        Run pipeline operation for specific tenant
        
        Args:
            config: Pipeline configuration
            tenant_id: Tenant identifier
            operation: Operation to run (build, query, etc.)
            **kwargs: Additional operation parameters
        """
        with TenantContext.tenant_scope(tenant_id):
            pipeline = cls(config, tenant_id)
            
            if operation == 'build':
                return pipeline.build(**kwargs)
            elif operation == 'query':
                return pipeline.query(**kwargs)
            else:
                raise ValueError(f"Unknown operation: {operation}")
