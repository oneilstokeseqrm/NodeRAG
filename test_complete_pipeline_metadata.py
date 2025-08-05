"""Test complete pipeline with metadata propagation through all components"""
import sys
sys.path.append('.')

import asyncio
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.test_utils.config_helper import create_test_nodeconfig, cleanup_test_output

async def test_complete_pipeline():
    """Test metadata flows through entire graph pipeline"""
    print("=== Testing Complete Pipeline Metadata Flow ===\n")
    
    test_metadata = EQMetadata(
        tenant_id='complete_test_tenant',
        account_id='acc_12345678-1234-4567-8901-123456789012',
        interaction_id='int_12345678-1234-4567-8901-123456789012',
        interaction_type='email',
        text='Complete pipeline test',
        timestamp='2024-01-01T12:00:00Z',
        user_id='complete@example.com',
        source_system='gmail'
    )
    
    decomposition_data = {
        'text_hash_id': 'complete_test_hash',
        'text_id': 'complete_test_001',
        'metadata': test_metadata.to_dict(),
        'response': {
            'Output': [{
                'semantic_unit': {
                    'context': 'Apple Inc announced acquisition of Beats Electronics for $3 billion'
                },
                'entities': [
                    {'name': 'Apple Inc', 'type': 'organization'},
                    {'name': 'Beats Electronics', 'type': 'organization'},
                    {'name': '$3 billion', 'type': 'money'}
                ],
                'relationships': [
                    'Apple Inc, acquired, Beats Electronics',
                    'Apple Inc, paid, $3 billion'
                ]
            }]
        }
    }
    
    config = create_test_nodeconfig()
    pipeline = Graph_pipeline(config)
    
    await pipeline.graph_tasks(decomposition_data)
    
    print("Checking semantic units...")
    semantic_units_ok = True
    for su in pipeline.semantic_units:
        node_data = pipeline.G.nodes[su.hash_id]
        if node_data.get('tenant_id') != test_metadata.tenant_id:
            print(f"❌ Semantic unit missing tenant_id")
            semantic_units_ok = False
            break
    
    if semantic_units_ok:
        print(f"✅ All {len(pipeline.semantic_units)} semantic units have metadata")
    
    print("\nChecking entities...")
    entities_ok = True
    for entity in pipeline.entities:
        node_data = pipeline.G.nodes[entity.hash_id]
        if node_data.get('tenant_id') != test_metadata.tenant_id:
            print(f"❌ Entity missing tenant_id")
            entities_ok = False
            break
    
    if entities_ok:
        print(f"✅ All {len(pipeline.entities)} entities have metadata")
    
    print("\nChecking relationships...")
    relationships_ok = True
    node_count = 0
    edge_count = 0
    
    for node_id, node_data in pipeline.G.nodes(data=True):
        if node_data.get('text_hash_id') == 'complete_test_hash' and node_data.get('type') in ['entity', 'relationship']:
            node_count += 1
            if node_data.get('tenant_id') != test_metadata.tenant_id:
                print(f"❌ Relationship node missing tenant_id")
                relationships_ok = False
                break
    
    for edge in pipeline.G.edges(data=True):
        if edge[2].get('text_hash_id') == 'complete_test_hash':
            edge_count += 1
            if edge[2].get('tenant_id') != test_metadata.tenant_id:
                print(f"❌ Relationship edge missing tenant_id")
                relationships_ok = False
                break
    
    if relationships_ok and (node_count > 0 or edge_count > 0):
        print(f"✅ All relationship nodes ({node_count}) and edges ({edge_count}) have metadata")
    
    all_ok = semantic_units_ok and entities_ok and relationships_ok
    if all_ok:
        print("\n✅ Complete pipeline metadata propagation verified!")
        print(f"   Total nodes: {pipeline.G.number_of_nodes()}")
        print(f"   Total edges: {pipeline.G.number_of_edges()}")
    else:
        print("\n❌ Some components missing metadata")
    
    return all_ok

if __name__ == "__main__":
    try:
        success = asyncio.run(test_complete_pipeline())
        exit(0 if success else 1)
    finally:
        cleanup_test_output()
