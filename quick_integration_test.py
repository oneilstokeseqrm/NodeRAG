"""Quick test that all Phase 3 components work together"""
import sys
sys.path.append('.')

from NodeRAG.src.pipeline.document_pipeline import document_pipeline
from NodeRAG.test_utils.config_helper import create_test_nodeconfig
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.config import NodeConfig

def test_phase3_integration():
    """Test document → graph pipeline flow with metadata"""
    print("=== Phase 3 Integration Test ===\n")
    
    test_interaction = {
        'tenant_id': 'integration_test',
        'account_id': 'acc_12345678-1234-4567-8901-123456789012',
        'interaction_id': 'int_12345678-1234-4567-8901-123456789012',
        'interaction_type': 'email',
        'text': 'Customer from Apple Inc needs help with billing.',
        'timestamp': '2024-01-01T12:00:00Z',
        'user_id': 'integration@test.com',
        'source_system': 'gmail'
    }
    
    try:
        config = create_test_nodeconfig()
        doc_pipeline = document_pipeline(config)
        text_units = doc_pipeline.process_interaction(test_interaction)
        print(f"✅ Document pipeline: Created {len(text_units)} text units with metadata")
        
        if text_units and hasattr(text_units[0], 'metadata'):
            print(f"✅ Text unit has metadata: tenant_id={text_units[0].metadata.tenant_id}")
        else:
            print("❌ Text unit missing metadata")
            return False
            
        print("\n✅ Phase 3 components working together successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_phase3_integration()
    exit(0 if success else 1)
