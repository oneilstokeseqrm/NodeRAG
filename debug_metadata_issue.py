import sys
sys.path.append('.')

from NodeRAG.standards.eq_metadata import EQMetadata

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

print('=== Metadata Debug ===')
print('Metadata type:', type(test_metadata))
print('to_dict type:', type(test_metadata.to_dict()))
print('from_dict type:', type(EQMetadata.from_dict(test_metadata.to_dict())))
print('validate method exists:', hasattr(test_metadata, 'validate'))
print('validate result:', test_metadata.validate())

metadata_dict = test_metadata.to_dict()
print('\nmetadata_dict contents:', metadata_dict)

if isinstance(metadata_dict, str):
    print("WARNING: metadata_dict is a string, not a dict!")
    import json
    try:
        metadata_dict = json.loads(metadata_dict)
        print("Successfully parsed string to dict")
    except:
        print("Failed to parse string as JSON")

try:
    metadata_from_dict = EQMetadata.from_dict(metadata_dict)
    print('from_dict success, type:', type(metadata_from_dict))
    print('validate method exists on from_dict:', hasattr(metadata_from_dict, 'validate'))
    validation_errors = metadata_from_dict.validate()
    print('validation_errors:', validation_errors)
except Exception as e:
    print('from_dict failed:', e)
    import traceback
    traceback.print_exc()
