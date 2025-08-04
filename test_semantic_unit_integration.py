import sys
sys.path.append('.')

import networkx as nx
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline
from NodeRAG.standards.eq_metadata import EQMetadata

def test_metadata_required():
    """Test that metadata is required, not optional"""
    pipeline = Graph_pipeline.__new__(Graph_pipeline)
    pipeline.G = nx.Graph()
    pipeline.semantic_units = []
    
    try:
        pipeline.add_semantic_unit(
            {'context': 'test'},
            'text_123'
        )
        print("❌ FAIL: Method accepted call without metadata")
        return False
    except TypeError as e:
        print("✅ PASS: Method correctly requires metadata parameter")
    
    try:
        pipeline.add_semantic_unit(
            {'context': 'test'},
            'text_123',
            None  # Explicit None - should fail
        )
        print("❌ FAIL: Method accepted None metadata")
        return False
    except ValueError as e:
        if "REQUIRED" in str(e):
            print("✅ PASS: Method correctly rejects None metadata")
        else:
            print(f"❌ FAIL: Wrong error message: {e}")
            return False
    
    valid_metadata = EQMetadata(
        tenant_id='test_tenant',
        account_id='acc_12345678-1234-4567-8901-123456789012',
        interaction_id='int_12345678-1234-4567-8901-123456789012',
        interaction_type='email',
        text='Full text here',
        timestamp='2024-01-01T10:00:00Z',
        user_id='test@example.com',
        source_system='gmail'
    )
    
    try:
        result = pipeline.add_semantic_unit(
            {'context': 'Valid test'},
            'text_456',
            valid_metadata
        )
        print("✅ PASS: Method accepts valid metadata")
        
        if pipeline.G.has_edge('text_456', result):
            print("✅ PASS: Edge created from text to semantic unit")
        else:
            print("❌ FAIL: Edge not created")
            return False
            
    except Exception as e:
        print(f"❌ FAIL: Valid call failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("=== Semantic Unit Metadata Integration Test ===\n")
    
    if test_metadata_required():
        print("\n✅ All critical fixes verified")
    else:
        print("\n❌ Some fixes still needed")
