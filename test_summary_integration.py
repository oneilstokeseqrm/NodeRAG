#!/usr/bin/env python3
"""Integration test for summary generation with Neo4j storage"""

import os
import sys
import networkx as nx
from pathlib import Path
import json
import tempfile

sys.path.insert(0, str(Path(__file__).parent))

from NodeRAG.config import NodeConfig
from NodeRAG.src.pipeline.summary_generation import SummaryGeneration
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.standards.eq_metadata import EQMetadata

def test_summary_neo4j_integration():
    """Test summary generation with Neo4j storage"""
    
    print("=" * 60)
    print("Testing Summary Generation Neo4j Storage")
    print("=" * 60)
    
    tenant_id = "test_tenant_406"
    TenantContext.set_current_tenant(tenant_id)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dict = {
            'config': {
                'main_folder': tmpdir,
                'graph_path': f"{tmpdir}/graph.pkl",
                'summary_path': f"{tmpdir}/summary.jsonl",
                'high_level_elements_path': f"{tmpdir}/he.parquet",
                'high_level_elements_titles_path': f"{tmpdir}/he_titles.parquet",
                'embedding': f"{tmpdir}/embedding.parquet",
                'indices_path': f"{tmpdir}/indices.json"
            },
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'text-embedding-3-small'},
            'language': 'en'
        }
        
        Path(f"{tmpdir}/cache").mkdir(parents=True, exist_ok=True)
        
        config = NodeConfig(config_dict)
        
        StorageFactory.initialize(config_dict, backend_mode='cloud')
        
        test_graph = nx.Graph()
        
        test_graph.add_node('entity_a1', type='entity', weight=2,
                          tenant_id='tenant_a', account_id='acc_a',
                          interaction_id='int_a1', interaction_type='chat',
                          timestamp='2025-01-01T00:00:00Z', user_id='user_a',
                          source_system='slack')
        
        test_graph.add_node('entity_b1', type='entity', weight=3,
                          tenant_id='tenant_b', account_id='acc_b',
                          interaction_id='int_b1', interaction_type='email',
                          timestamp='2025-01-01T00:00:00Z', user_id='user_b',
                          source_system='gmail')
        
        test_graph.add_edge('entity_a1', 'entity_b1', weight=1)
        
        pipeline = SummaryGeneration(config)
        pipeline.G = test_graph
        
        print("\nTesting cross-tenant metadata extraction...")
        metadata = pipeline._extract_metadata_from_community(['entity_a1', 'entity_b1'])
        
        assert metadata.tenant_id == 'AGGREGATED', f"Expected AGGREGATED, got {metadata.tenant_id}"
        print(f"✅ Cross-tenant summary uses AGGREGATED: {metadata.tenant_id}")
        
        print("\nTesting single-tenant metadata extraction...")
        metadata_single = pipeline._extract_metadata_from_community(['entity_a1'])
        
        assert metadata_single.tenant_id == 'tenant_a', f"Expected tenant_a, got {metadata_single.tenant_id}"
        print(f"✅ Single-tenant preserves metadata: {metadata_single.tenant_id}")
        
        print("\nTesting graph storage to Neo4j...")
        pipeline.store_graph()
        print("✅ Graph stored successfully")
        
        from unittest.mock import MagicMock
        he = MagicMock()
        he.hash_id = 'he_test_1'
        he.title_hash_id = 'he_test_1_title'
        he.context = 'Test high-level element for cross-tenant summary'
        he.title = 'Cross-Tenant Summary'
        he.human_readable_id = 'HLE_TEST_001'
        he.embedding = [0.1] * 3072
        
        test_graph.add_node('he_test_1', type='high_level_element',
                          tenant_id='AGGREGATED', account_id='AGGREGATED',
                          interaction_id='AGGREGATED', interaction_type='summary',
                          timestamp='2025-01-01T00:00:00Z', user_id='system',
                          source_system='internal')
        test_graph.add_node('he_test_1_title', type='high_level_element_title',
                          tenant_id='AGGREGATED', account_id='AGGREGATED')
        
        pipeline.high_level_elements = [he]
        
        print("\nTesting high-level elements storage...")
        pipeline.store_high_level_elements()
        print("✅ High-level elements stored successfully")
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
        
        TenantContext.clear_current_tenant()
        StorageFactory.cleanup()

if __name__ == "__main__":
    test_summary_neo4j_integration()
