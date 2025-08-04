import json
import sys
sys.path.append('.')

import networkx as nx
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline

def test_pipeline_metadata_flow():
    """Test complete pipeline flow with metadata"""
    print("=== Testing End-to-End Metadata Flow ===\n")
    
    test_metadata = EQMetadata(
        tenant_id='pipeline_test_tenant',
        account_id='acc_12345678-1234-4567-8901-123456789012',
        interaction_id='int_12345678-1234-4567-8901-123456789012',
        interaction_type='email',
        text='Full email content here',
        timestamp='2024-01-01T12:00:00Z',
        user_id='pipeline_test@example.com',
        source_system='gmail'
    )
    
    decomposition_data = {
        'text_hash_id': 'test_text_hash_789',
        'text_id': 'test_text_001',
        'metadata': test_metadata.to_dict(),
        'response': {
            'Output': [{
                'entities': [{'name': 'TEST COMPANY', 'type': 'organization'}],
                'relationships': [],
                'semantic_unit': {'context': 'Customer needs help with billing'}
            }]
        }
    }
    
    pipeline = Graph_pipeline.__new__(Graph_pipeline)
    pipeline.G = nx.Graph()
    pipeline.semantic_units = []
    pipeline.entities = []
    pipeline.relationship = []
    pipeline.relationship_lookup = {}
    pipeline.relationship_nodes = []
    
    class MockConfig:
        def __init__(self):
            self.tracker = MockTracker()
    
    class MockTracker:
        def update(self):
            pass
    
    pipeline.config = MockConfig()
    
    try:
        import asyncio
        metadata_dict = decomposition_data.get('metadata')
        print(f"DEBUG: metadata_dict type: {type(metadata_dict)}")
        print(f"DEBUG: metadata_dict content: {metadata_dict}")
        
        if isinstance(metadata_dict, str):
            print("WARNING: metadata_dict is a string, not a dict!")
            import json
            try:
                metadata_dict = json.loads(metadata_dict)
                print("Successfully parsed string to dict")
            except:
                print("Failed to parse string as JSON")
        
        test_metadata_obj = EQMetadata.from_dict(metadata_dict)
        print(f"DEBUG: from_dict result type: {type(test_metadata_obj)}")
        print(f"DEBUG: has validate method: {hasattr(test_metadata_obj, 'validate')}")
        
        asyncio.run(pipeline.graph_tasks(decomposition_data))
        print("✅ PASS: Pipeline processed data with metadata")
        
        if len(pipeline.semantic_units) > 0:
            su = pipeline.semantic_units[0]
            node_data = pipeline.G.nodes[su.hash_id]
            
            checks = {
                'tenant_id': node_data.get('tenant_id') == test_metadata.tenant_id,
                'account_id': node_data.get('account_id') == test_metadata.account_id,
                'user_id': node_data.get('user_id') == test_metadata.user_id,
                'no_text_field': 'text' not in node_data
            }
            
            all_passed = all(checks.values())
            if all_passed:
                print("✅ PASS: All metadata fields correctly propagated")
            else:
                print(f"❌ FAIL: Some metadata fields missing: {checks}")
                return False
        else:
            print("❌ FAIL: No semantic units created")
            return False
            
    except Exception as e:
        print(f"❌ FAIL: Pipeline processing failed: {e}")
        return False
    
    bad_data = {
        'text_hash_id': 'test_text_hash_bad',
        'response': decomposition_data['response']
    }
    
    try:
        asyncio.run(pipeline.graph_tasks(bad_data))
        print("❌ FAIL: Pipeline accepted data without metadata")
        return False
    except ValueError as e:
        if "metadata is REQUIRED" in str(e):
            print("✅ PASS: Pipeline correctly rejected missing metadata")
        else:
            print(f"❌ FAIL: Wrong error for missing metadata: {e}")
            return False
    
    empty_metadata_data = {
        'text_hash_id': 'test_text_hash_empty',
        'response': decomposition_data['response'],
        'metadata': {}  # Empty dict
    }
    
    try:
        asyncio.run(pipeline.graph_tasks(empty_metadata_data))
        print("❌ FAIL: Pipeline accepted empty metadata dict")
        return False
    except ValueError as e:
        print("✅ PASS: Pipeline correctly rejected empty metadata")
    
    return True

def test_data_loading_validation():
    """Test that load_data validates metadata presence"""
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump({'text_hash_id': 'test', 'response': {}}, f)
        f.write('\n')
        temp_path = f.name
    
    try:
        class MockConfig:
            text_decomposition_path = temp_path
        
        pipeline = Graph_pipeline.__new__(Graph_pipeline)
        pipeline.config = MockConfig()
        
        try:
            pipeline.load_data()
            print("❌ FAIL: load_data accepted data without metadata")
            return False
        except ValueError as e:
            if "Missing metadata" in str(e):
                print("✅ PASS: load_data correctly rejected missing metadata")
                return True
            else:
                print(f"❌ FAIL: Wrong error from load_data: {e}")
                return False
    finally:
        os.unlink(temp_path)

if __name__ == "__main__":
    test1 = test_pipeline_metadata_flow()
    test2 = test_data_loading_validation()
    
    if test1 and test2:
        print("\n✅ All pipeline metadata flow tests passed!")
        print("Metadata is now properly enforced throughout the pipeline.")
    else:
        print("\n❌ Some tests failed - metadata enforcement incomplete")
