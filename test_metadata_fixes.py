"""Test that metadata fixes work correctly"""
import sys
sys.path.append('.')

import json
import asyncio
import os
import pandas as pd
import networkx as nx
from NodeRAG.src.pipeline.Insert_text import Insert_text
from NodeRAG.src.pipeline.attribute_generation import Attribution_generation_pipeline
from NodeRAG.test_utils.config_helper import create_test_nodeconfig
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.storage import storage

def test_insert_text_fix():
    """Test that text units get metadata when added as nodes"""
    print("=== Testing Insert_text Fix ===\n")
    
    test_metadata = EQMetadata(
        tenant_id='test_tenant',
        account_id='acc_12345678-1234-4567-8901-123456789012',
        interaction_id='int_12345678-1234-4567-8901-123456789012',
        interaction_type='email',
        text='Test text for insert',
        timestamp='2024-01-01T10:00:00Z',
        user_id='test@example.com',
        source_system='gmail'
    )
    
    config = create_test_nodeconfig()
    
    os.makedirs(os.path.dirname(config.text_decomposition_path), exist_ok=True)
    os.makedirs(os.path.dirname(config.graph_path), exist_ok=True)
    os.makedirs(os.path.dirname(config.semantic_units_path), exist_ok=True)
    
    test_data = {
        'text_hash_id': 'test_text_hash_123',
        'text_id': 'test_001',
        'metadata': test_metadata.to_dict(),
        'response': {'Output': []}
    }
    
    with open(config.text_decomposition_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(test_data) + '\n')
    
    G = nx.Graph()
    G.add_node('semantic_unit_123', type='semantic_unit', weight=1)
    
    storage(G).save_pickle(config.graph_path)
    
    semantic_units_df = pd.DataFrame([{
        'hash_id': 'semantic_unit_123',
        'text_hash_id': 'test_text_hash_123',
        'insert': None
    }])
    storage(semantic_units_df).save_parquet(config.semantic_units_path)
    
    try:
        insert_pipeline = Insert_text(config)
        insert_pipeline.insert_text()
        
        if insert_pipeline.G.has_node('test_text_hash_123'):
            node_data = insert_pipeline.G.nodes['test_text_hash_123']
            print(f"Text unit node created with fields: {list(node_data.keys())}")
            
            required_fields = ['tenant_id', 'account_id', 'interaction_id', 
                              'interaction_type', 'timestamp', 'user_id', 'source_system']
            
            missing = [f for f in required_fields if f not in node_data]
            if missing:
                print(f"❌ Missing metadata fields: {missing}")
                return False
            else:
                print("✅ All metadata fields present!")
                print(f"   tenant_id: {node_data['tenant_id']}")
                return True
        else:
            print("❌ Text unit node not created")
            return False
    except Exception as e:
        print(f"❌ Insert_text test failed with error: {e}")
        return False

async def test_attribute_generation_fix():
    """Test that attributes inherit metadata from entities"""
    print("\n=== Testing Attribute Generation Fix ===\n")
    
    config = create_test_nodeconfig()
    
    os.makedirs(os.path.dirname(config.entities_path), exist_ok=True)
    os.makedirs(os.path.dirname(config.relationship_path), exist_ok=True)
    os.makedirs(os.path.dirname(config.semantic_units_path), exist_ok=True)
    
    G = nx.Graph()
    entity_node = 'entity_hash_456'
    
    G.add_node(entity_node, 
        type='entity',
        weight=5,
        context='Test Company Inc',
        tenant_id='test_tenant',
        account_id='acc_12345678-1234-4567-8901-123456789012',
        interaction_id='int_12345678-1234-4567-8901-123456789012',
        interaction_type='email',
        timestamp='2024-01-01T10:00:00Z',
        user_id='test@example.com',
        source_system='gmail'
    )
    
    semantic_node = 'semantic_unit_789'
    G.add_node(semantic_node, type='semantic_unit', weight=1, context='Test semantic unit')
    G.add_edge(entity_node, semantic_node, type='semantic', weight=1)
    
    storage(G).save_pickle(config.graph_path)
    
    entities_data = pd.DataFrame([{
        'hash_id': entity_node,
        'context': 'Test Company Inc',
        'type': 'entity',
        'weight': 5
    }])
    storage(entities_data).save_parquet(config.entities_path)
    
    relationships_data = pd.DataFrame([{
        'hash_id': 'rel_123',
        'context': 'Test relationship',
        'type': 'relationship',
        'weight': 1
    }])
    storage(relationships_data).save_parquet(config.relationship_path)
    
    semantic_units_data = pd.DataFrame([{
        'hash_id': semantic_node,
        'context': 'Test semantic unit',
        'type': 'semantic_unit',
        'weight': 1
    }])
    storage(semantic_units_data).save_parquet(config.semantic_units_path)
    
    try:
        attr_pipeline = Attribution_generation_pipeline(config)
        attr_pipeline.important_nodes = [entity_node]
        
        async def mock_api_client(request):
            return "Test Company Inc is a leading technology company."
        
        attr_pipeline.API_client = mock_api_client
        
        await attr_pipeline.generate_attribution(entity_node)
        
        if attr_pipeline.attributes:
            attr = attr_pipeline.attributes[0]
            if attr_pipeline.G.has_node(attr.hash_id):
                node_data = attr_pipeline.G.nodes[attr.hash_id]
                print(f"Attribute node created with fields: {list(node_data.keys())}")
                
                required_fields = ['tenant_id', 'account_id', 'interaction_id', 
                                  'interaction_type', 'timestamp', 'user_id', 'source_system']
                
                missing = [f for f in required_fields if f not in node_data]
                if missing:
                    print(f"❌ Missing metadata fields: {missing}")
                    return False
                else:
                    print("✅ All metadata fields present!")
                    print(f"   tenant_id: {node_data['tenant_id']}")
                    print(f"   Inherited from entity: {entity_node}")
                    return True
            else:
                print("❌ Attribute node not found in graph")
                return False
        else:
            print("❌ No attributes created")
            return False
    except Exception as e:
        print(f"❌ Attribute generation test failed with error: {e}")
        return False

async def main():
    """Run all tests"""
    print("Testing Metadata Fixes\n")
    
    insert_success = test_insert_text_fix()
    
    attr_success = await test_attribute_generation_fix()
    
    print("\n=== Test Summary ===")
    print(f"Insert_text fix: {'✅ PASSED' if insert_success else '❌ FAILED'}")
    print(f"Attribute generation fix: {'✅ PASSED' if attr_success else '❌ FAILED'}")
    
    if insert_success and attr_success:
        print("\n✅ All fixes working correctly!")
        return True
    else:
        print("\n❌ Some fixes need attention")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
