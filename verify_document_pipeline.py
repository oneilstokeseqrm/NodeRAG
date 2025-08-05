"""Verify document pipeline works with proper config"""
import sys
sys.path.append('.')

from NodeRAG.test_utils.config_helper import create_test_nodeconfig, cleanup_test_output
from NodeRAG.standards.eq_metadata import EQMetadata

print("=== Verifying Document Pipeline with Fixed Config ===\n")

try:
    config = create_test_nodeconfig()
    print("✅ NodeConfig created successfully")
    
    from NodeRAG.src.pipeline.document_pipeline import document_pipeline
    doc_pipeline = document_pipeline(config)
    print("✅ Document pipeline initialized")
    
    test_interaction = {
        'tenant_id': 'pipeline_test_tenant',
        'account_id': 'acc_12345678-1234-4567-8901-123456789012',
        'interaction_id': 'int_12345678-1234-4567-8901-123456789012',
        'interaction_type': 'email',
        'text': 'Customer from Apple Inc needs help with billing issue. Please process refund.',
        'timestamp': '2024-01-01T12:00:00Z',
        'user_id': 'test@example.com',
        'source_system': 'gmail'
    }
    
    print("\nProcessing test interaction...")
    text_units = doc_pipeline.process_interaction(test_interaction)
    
    if text_units:
        print(f"✅ Created {len(text_units)} text units")
        
        first_unit = text_units[0]
        if hasattr(first_unit, 'metadata'):
            metadata = first_unit.metadata
            print(f"✅ Metadata present on text units")
            print(f"   tenant_id: {metadata.tenant_id}")
            print(f"   account_id: {metadata.account_id}")
            print(f"   user_id: {metadata.user_id}")
            
            validation_errors = metadata.validate()
            if not validation_errors:
                print("✅ Metadata validation passed")
            else:
                print(f"❌ Metadata validation errors: {validation_errors}")
        else:
            print("❌ No metadata found on text units")
    else:
        print("❌ No text units created")
    
    print("\n✅ Document pipeline verification complete!")
    
except Exception as e:
    print(f"❌ Document pipeline verification failed: {e}")
    import traceback
    traceback.print_exc()

finally:
    cleanup_test_output()
