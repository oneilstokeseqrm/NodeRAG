"""
Test suite for multi-tenant data isolation
"""
import pytest
import networkx as nx
import pandas as pd
import numpy as np
import uuid
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.storage.storage_factory_tenant import TenantAwareStorageFactory
from NodeRAG.src.pipeline.graph_pipeline_tenant import TenantAwareGraphPipeline
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter


class TestMultiTenantIsolation:
    """Test multi-tenant data isolation"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment"""
        import os
        os.makedirs('/tmp/test', exist_ok=True)
        
        self.config = {
            'config': {'main_folder': '/tmp/test', 'language': 'en', 'chunk_size': 512},
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        
        StorageFactory.initialize(self.config, backend_mode="file")
        
        # Create test tenants
        self.tenant1 = f"tenant_{uuid.uuid4()}"
        self.tenant2 = f"tenant_{uuid.uuid4()}"
        
        yield
        
        # Cleanup
        TenantContext.clear_current_tenant()
    
    def test_tenant_context_management(self):
        """Test tenant context setting and retrieval"""
        # Initially no tenant
        assert TenantContext.get_current_tenant() is None
        assert TenantContext.get_current_tenant_or_default() == 'default'
        
        # Set tenant 1
        TenantContext.set_current_tenant(self.tenant1, {'org': 'TestOrg1'})
        assert TenantContext.get_current_tenant() == self.tenant1
        assert TenantContext.get_tenant_metadata()['org'] == 'TestOrg1'
        
        # Clear and set tenant 2
        TenantContext.clear_current_tenant()
        TenantContext.set_current_tenant(self.tenant2)
        assert TenantContext.get_current_tenant() == self.tenant2
    
    def test_tenant_context_scope(self):
        """Test tenant context manager"""
        original_tenant = TenantContext.get_current_tenant()
        
        with TenantContext.tenant_scope(self.tenant1):
            assert TenantContext.get_current_tenant() == self.tenant1
            
            # Nested scope
            with TenantContext.tenant_scope(self.tenant2):
                assert TenantContext.get_current_tenant() == self.tenant2
            
            # Back to tenant1
            assert TenantContext.get_current_tenant() == self.tenant1
        
        # Back to original
        assert TenantContext.get_current_tenant() == original_tenant
    
    def test_tenant_data_isolation(self):
        """Test that tenants cannot access each other's data"""
        adapter = PipelineStorageAdapter()
        
        # Create graphs for each tenant
        graph1 = nx.Graph()
        graph1.add_node("tenant1_node", data="tenant1_data")
        
        graph2 = nx.Graph()
        graph2.add_node("tenant2_node", data="tenant2_data")
        
        # Save graphs with tenant isolation
        with TenantContext.tenant_scope(self.tenant1):
            success1 = adapter.save_pickle(graph1, "/tmp/graph.pkl", "graph", self.tenant1)
            assert success1
        
        with TenantContext.tenant_scope(self.tenant2):
            success2 = adapter.save_pickle(graph2, "/tmp/graph.pkl", "graph", self.tenant2)
            assert success2
        
        # Load and verify isolation
        with TenantContext.tenant_scope(self.tenant1):
            loaded1 = adapter.load_pickle("/tmp/graph.pkl", "graph", self.tenant1)
            assert "tenant1_node" in loaded1.nodes()
            assert "tenant2_node" not in loaded1.nodes()
        
        with TenantContext.tenant_scope(self.tenant2):
            loaded2 = adapter.load_pickle("/tmp/graph.pkl", "graph", self.tenant2)
            assert "tenant2_node" in loaded2.nodes()
            assert "tenant1_node" not in loaded2.nodes()
    
    def test_tenant_namespace_isolation(self):
        """Test namespace isolation for embeddings"""
        # Get namespaces for different tenants
        with TenantContext.tenant_scope(self.tenant1):
            ns1 = TenantContext.get_tenant_namespace('embeddings')
            assert ns1.startswith(self.tenant1)
        
        with TenantContext.tenant_scope(self.tenant2):
            ns2 = TenantContext.get_tenant_namespace('embeddings')
            assert ns2.startswith(self.tenant2)
        
        assert ns1 != ns2
    
    def test_concurrent_tenant_operations(self):
        """Test that concurrent operations maintain tenant isolation"""
        import time
        
        def process_tenant_data(tenant_id, operation_id):
            """Process operation for a specific tenant"""
            with TenantContext.tenant_scope(tenant_id):
                # Verify correct tenant context
                current = TenantContext.get_current_tenant()
                assert current == tenant_id, f"Context mismatch: {current} != {tenant_id}"
                
                # Create tenant-specific graph
                graph = nx.Graph()
                node_id = f"{tenant_id}_node_{operation_id}"
                graph.add_node(node_id, tenant=tenant_id, operation=operation_id, thread=threading.current_thread().name)
                
                adapter = PipelineStorageAdapter()
                path = f"/tmp/test_{tenant_id}.pkl"  # Unique path per tenant
                
                success = adapter.save_pickle(graph, path, "graph", tenant_id)
                if not success:
                    return {'tenant': tenant_id, 'operation': operation_id, 'success': False, 'error': 'Save failed'}
                
                # Only verify final state for first operation of each tenant
                if operation_id == 0:
                    time.sleep(0.1)
                    
                    loaded = adapter.load_pickle(path, "graph", tenant_id)
                    if loaded:
                        # Verify it's a valid graph (not corrupted)
                        if not isinstance(loaded, nx.Graph):
                            return {'tenant': tenant_id, 'success': False, 'error': 'Corrupted graph'}
                        
                        for node in loaded.nodes():
                            if 'tenant' in loaded.nodes[node]:
                                node_tenant = loaded.nodes[node]['tenant']
                                if node_tenant != tenant_id:
                                    return {'tenant': tenant_id, 'success': False, 
                                           'error': f'Cross-tenant contamination: found {node_tenant}'}
                        
                        return {'tenant': tenant_id, 'operation': operation_id, 'success': True}
                    else:
                        return {'tenant': tenant_id, 'success': False, 'error': 'Load failed'}
                
                return {'tenant': tenant_id, 'operation': operation_id, 'success': True}
        
        # Test with DIFFERENT tenants for true isolation
        test_tenants = [f"tenant_concurrent_{uuid.uuid4()}" for _ in range(4)]
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i, tenant_id in enumerate(test_tenants):
                future = executor.submit(process_tenant_data, tenant_id, 0)
                futures.append(future)
            
            for future in as_completed(futures):
                result = future.result()
                assert result['success'], f"Failed: {result.get('error', 'Unknown error')}"
    
    def test_tenant_access_validation(self):
        """Test access validation between tenants"""
        with TenantContext.tenant_scope(self.tenant1):
            # Can access own resources
            assert TenantContext.validate_tenant_access(self.tenant1)
            
            # Cannot access other tenant's resources
            assert not TenantContext.validate_tenant_access(self.tenant2)
    
    def test_tenant_aware_pipeline(self):
        """Test tenant-aware pipeline operations"""
        from NodeRAG.config.Node_config import NodeConfig
        
        node_config = NodeConfig(self.config)
        
        # Create pipeline for tenant1
        with TenantContext.tenant_scope(self.tenant1):
            pipeline1 = TenantAwareGraphPipeline(node_config, self.tenant1)
            assert pipeline1.tenant_id == self.tenant1
            
            # Create and save graph
            pipeline1.G = nx.Graph()
            pipeline1.G.add_node("t1_node", data="tenant1")
            pipeline1.save_graph()
        
        # Create pipeline for tenant2
        with TenantContext.tenant_scope(self.tenant2):
            pipeline2 = TenantAwareGraphPipeline(node_config, self.tenant2)
            assert pipeline2.tenant_id == self.tenant2
            
            # Create and save different graph
            pipeline2.G = nx.Graph()
            pipeline2.G.add_node("t2_node", data="tenant2")
            pipeline2.save_graph()
        
        # Verify isolation when loading
        with TenantContext.tenant_scope(self.tenant1):
            pipeline1_reload = TenantAwareGraphPipeline(node_config, self.tenant1)
            graph1 = pipeline1_reload.load_graph()
            assert "t1_node" in graph1.nodes() if graph1 else False
            assert "t2_node" not in graph1.nodes() if graph1 else True
