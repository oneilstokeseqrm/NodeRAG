"""Integration tests for ID generation with EQ metadata standard"""
import pytest
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.utils.id_generation import NodeIDGenerator, MetadataTracker
from NodeRAG.config.eq_config import EQConfig

def test_id_generation_with_real_metadata():
    """Test ID generation integrates with real EQMetadata"""
    metadata = EQMetadata(
        tenant_id="tenant_acme",
        interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
        interaction_type="email",
        text="Integration test content",
        account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
        source_system="outlook"
    )
    
    doc_id = NodeIDGenerator.generate_document_id(metadata.to_dict())
    assert doc_id.startswith("doc_")
    assert len(doc_id) == 20
    
    sem_id = NodeIDGenerator.generate_semantic_unit_id(
        text=metadata.text,
        tenant_id=metadata.tenant_id,
        doc_id=doc_id,
        chunk_index=0
    )
    assert sem_id.startswith("sem_")

def test_id_generation_with_config_integration():
    """Test ID generation works with EQConfig"""
    import tempfile
    import os
    import shutil
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        config_data = {
            'config': {
                'main_folder': temp_dir,
                'language': 'English',
                'chunk_size': 1048
            },
            'model_config': {
                'service_provider': 'openai',
                'model_name': 'gpt-4o-mini',
                'temperature': 0,
                'max_tokens': 10000,
                'rate_limit': 40
            },
            'embedding_config': {
                'service_provider': 'openai_embedding',
                'embedding_model_name': 'text-embedding-3-small',
                'rate_limit': 20
            },
            'eq_config': {
                'metadata': {'validate_on_set': True}
            }
        }
        eq_config = EQConfig(config_data)
        
        metadata_dict = {
            'tenant_id': 'tenant_test',
            'interaction_id': 'int_550e8400-e29b-41d4-a716-446655440000',
            'interaction_type': 'call',
            'text': 'Test call content',
            'account_id': 'acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8',
            'timestamp': '2024-01-15T10:30:00Z',
            'user_id': 'usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8',
            'source_system': 'internal'
        }
        
        eq_config.current_metadata = metadata_dict
        
        doc_id = NodeIDGenerator.generate_document_id(eq_config.current_metadata)
        assert NodeIDGenerator.validate_id_format(doc_id)
    finally:
        shutil.rmtree(temp_dir)

def test_lineage_tracking_integration():
    """Test lineage tracking with real node creation flow"""
    tracker = MetadataTracker()
    
    metadata = EQMetadata(
        tenant_id="tenant_acme",
        interaction_id="int_123",
        interaction_type="email",
        text="Test document",
        account_id="acc_456",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_789",
        source_system="outlook"
    )
    
    doc_id = NodeIDGenerator.generate_document_id(metadata.to_dict())
    tracker.record_node_creation(doc_id, 'document', [], metadata.to_dict())
    
    sem_id = NodeIDGenerator.generate_semantic_unit_id(
        text=metadata.text,
        tenant_id=metadata.tenant_id,
        doc_id=doc_id,
        chunk_index=0
    )
    tracker.record_node_creation(sem_id, 'semantic_unit', [doc_id], metadata.to_dict())
    
    source_docs = tracker.find_source_documents(sem_id)
    assert doc_id in source_docs

def test_metadata_validation_integration():
    """Test that ID generation works with validated metadata"""
    metadata = EQMetadata(
        tenant_id="tenant_acme",
        interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
        interaction_type="email",
        text="Test validation content",
        account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
        source_system="gmail"
    )
    
    errors = metadata.validate()
    assert len(errors) == 0
    
    entity_id = NodeIDGenerator.generate_entity_id(
        entity_name="Test Entity",
        entity_type="PERSON",
        tenant_id=metadata.tenant_id
    )
    
    assert entity_id.startswith("ent_")
    assert NodeIDGenerator.validate_id_format(entity_id)

def test_tenant_isolation_integration():
    """Test tenant isolation works with real metadata"""
    metadata1 = EQMetadata(
        tenant_id="tenant_acme",
        interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
        interaction_type="email",
        text="Same content",
        account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
        source_system="outlook"
    )
    
    metadata2 = EQMetadata(
        tenant_id="tenant_beta",
        interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
        interaction_type="email",
        text="Same content",
        account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
        timestamp="2024-01-15T10:30:00Z",
        user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
        source_system="outlook"
    )
    
    entity1_id = NodeIDGenerator.generate_entity_id(
        entity_name="John Smith",
        entity_type="PERSON",
        tenant_id=metadata1.tenant_id
    )
    
    entity2_id = NodeIDGenerator.generate_entity_id(
        entity_name="John Smith",
        entity_type="PERSON",
        tenant_id=metadata2.tenant_id
    )
    
    assert entity1_id != entity2_id

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
