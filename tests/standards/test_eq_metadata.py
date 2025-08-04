import pytest
import os
from datetime import datetime, timezone
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.standards.metadata_propagation import MetadataPropagationRules


class TestEQMetadata:
    """Test cases for EQMetadata dataclass and validation"""
    
    def test_valid_metadata_creation(self):
        """Test creation and validation of valid metadata"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="This is a test email content about invoice questions",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        errors = metadata.validate()
        assert len(errors) == 0, f"Valid metadata should have no errors: {errors}"
        
        data_dict = metadata.to_dict()
        assert data_dict['tenant_id'] == "tenant_acme"
        assert data_dict['interaction_type'] == "email"
        assert 'created_at' in data_dict
    
    def test_empty_required_fields_validation(self):
        """Test that empty required fields are caught"""
        metadata = EQMetadata(
            tenant_id="",  # Empty field
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        errors = metadata.validate()
        assert len(errors) >= 1
        assert any("tenant_id cannot be empty" in error for error in errors)
    
    def test_whitespace_only_fields_validation(self):
        """Test that whitespace-only fields are caught"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="   ",  # Whitespace only
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        errors = metadata.validate()
        assert len(errors) >= 1
        assert any("text cannot be empty" in error for error in errors)
    
    def test_invalid_uuid_format_validation(self):
        """Test UUID format validation"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_invalid-uuid-format",  # Invalid UUID
            interaction_type="email",
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        errors = metadata.validate()
        assert len(errors) >= 1
        assert any("interaction_id must be UUID v4 format" in error for error in errors)
    
    def test_missing_uuid_prefix_validation(self):
        """Test that UUIDs without proper prefixes are caught"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="550e8400-e29b-41d4-a716-446655440000",  # Missing int_ prefix
            interaction_type="email",
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        errors = metadata.validate()
        assert len(errors) >= 1
        assert any("interaction_id must be UUID v4 format with 'int_' prefix" in error for error in errors)
    
    def test_invalid_timestamp_format_validation(self):
        """Test timestamp format validation"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15 10:30:00",  # Invalid format (no Z, wrong format)
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        errors = metadata.validate()
        assert len(errors) >= 1
        assert any("timestamp must be ISO8601 format" in error for error in errors)
    
    def test_invalid_interaction_type_validation(self):
        """Test interaction_type enum validation"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="invalid_type",  # Invalid enum value
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        errors = metadata.validate()
        assert len(errors) >= 1
        assert any("interaction_type must be one of:" in error for error in errors)
    
    def test_invalid_source_system_validation(self):
        """Test source_system enum validation"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="invalid_system"  # Invalid enum value
        )
        
        errors = metadata.validate()
        assert len(errors) >= 1
        assert any("source_system must be one of:" in error for error in errors)
    
    def test_all_valid_interaction_types(self):
        """Test all valid interaction types"""
        valid_types = ['call', 'chat', 'email', 'voice_memo', 'custom_notes']
        
        for interaction_type in valid_types:
            metadata = EQMetadata(
                tenant_id="tenant_acme",
                interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
                interaction_type=interaction_type,
                text="Test content",
                account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
                timestamp="2024-01-15T10:30:00Z",
                user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
                source_system="outlook"
            )
            
            errors = metadata.validate()
            type_errors = [e for e in errors if "interaction_type" in e]
            assert len(type_errors) == 0, f"Valid interaction_type '{interaction_type}' should not have errors"
    
    def test_all_valid_source_systems(self):
        """Test all valid source systems"""
        valid_systems = ['internal', 'voice_memo', 'custom', 'outlook', 'gmail']
        
        for source_system in valid_systems:
            metadata = EQMetadata(
                tenant_id="tenant_acme",
                interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
                interaction_type="email",
                text="Test content",
                account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
                timestamp="2024-01-15T10:30:00Z",
                user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
                source_system=source_system
            )
            
            errors = metadata.validate()
            system_errors = [e for e in errors if "source_system" in e]
            assert len(system_errors) == 0, f"Valid source_system '{source_system}' should not have errors"
    
    def test_from_dict_creation(self):
        """Test creating EQMetadata from dictionary"""
        data = {
            'tenant_id': "tenant_acme",
            'interaction_id': "int_550e8400-e29b-41d4-a716-446655440000",
            'interaction_type': "email",
            'text': "Test content",
            'account_id': "acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            'timestamp': "2024-01-15T10:30:00Z",
            'user_id': "usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            'source_system': "outlook"
        }
        
        metadata = EQMetadata.from_dict(data)
        assert metadata.tenant_id == "tenant_acme"
        assert metadata.interaction_type == "email"
        
        errors = metadata.validate()
        assert len(errors) == 0
    
    def test_copy_with_node_info(self):
        """Test copying metadata with NodeRAG-generated fields"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
        
        node_metadata = metadata.copy_with_node_info(
            node_hash_id="abc123def456",
            node_type="entity"
        )
        
        assert node_metadata.node_hash_id == "abc123def456"
        assert node_metadata.node_type == "entity"
        assert node_metadata.tenant_id == "tenant_acme"  # Original fields preserved

    def test_flexible_user_id_formats(self):
        """Test that user_id accepts various string formats"""
        test_cases = [
            ("user@example.com", True, "Email format"),
            ("EMP123456", True, "Employee ID format"),
            ("12345", True, "Numeric string"),
            ("salesforce_123", True, "External system ID"),
            ("usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8", True, "UUID format (backward compatibility)"),
            ("", False, "Empty string"),
            ("   ", False, "Whitespace only"),
        ]
        
        for user_id, should_pass, description in test_cases:
            metadata = EQMetadata(
                tenant_id="tenant_acme",
                interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
                interaction_type="email",
                text="Test content",
                account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
                timestamp="2024-01-15T10:30:00Z",
                user_id=user_id,
                source_system="outlook"
            )
            
            errors = metadata.validate()
            user_id_errors = [e for e in errors if "user_id" in e]
            passed = len(user_id_errors) == 0
            
            if should_pass:
                assert passed, f"{description}: user_id '{user_id}' should be valid but got errors: {user_id_errors}"
            else:
                assert not passed, f"{description}: user_id '{user_id}' should be invalid but passed validation"

    def test_user_id_none_validation(self):
        """Test that None user_id is properly rejected"""
        metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="Test content",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id=None,
            source_system="outlook"
        )
        
        errors = metadata.validate()
        user_id_errors = [e for e in errors if "user_id" in e]
        assert len(user_id_errors) >= 1
        assert any("user_id must be a non-empty string" in error for error in user_id_errors)


class TestMetadataPropagation:
    """Test cases for metadata propagation rules"""
    
    def setup_method(self):
        """Set up test data for each test"""
        self.source_metadata = EQMetadata(
            tenant_id="tenant_acme",
            interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
            interaction_type="email",
            text="This is test email content about customer support",
            account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            timestamp="2024-01-15T10:30:00Z",
            user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
            source_system="outlook"
        )
    
    def test_semantic_unit_propagation(self):
        """Test metadata propagation to semantic units"""
        result = MetadataPropagationRules.propagate_to_semantic_unit(self.source_metadata)
        
        assert result['tenant_id'] == "tenant_acme"
        assert result['text'] == "This is test email content about customer support"
        assert result['interaction_id'] == "int_550e8400-e29b-41d4-a716-446655440000"
        assert 'created_at' in result
    
    def test_entity_propagation(self):
        """Test metadata propagation to entities"""
        result = MetadataPropagationRules.propagate_to_entity(self.source_metadata)
        
        assert result['tenant_id'] == "tenant_acme"
        assert result['interaction_id'] == "int_550e8400-e29b-41d4-a716-446655440000"
        assert 'text' not in result  # Text should be removed
    
    def test_relationship_propagation(self):
        """Test metadata propagation to relationships"""
        result = MetadataPropagationRules.propagate_to_relationship(self.source_metadata)
        
        assert result['tenant_id'] == "tenant_acme"
        assert result['interaction_id'] == "int_550e8400-e29b-41d4-a716-446655440000"
        assert 'text' not in result  # Text should be removed
    
    def test_attribute_propagation_single_entity(self):
        """Test metadata propagation to attributes from single entity"""
        entity_metadata = self.source_metadata.to_dict()
        entity_metadata.pop('text', None)  # Remove text as entities don't have it
        
        result = MetadataPropagationRules.propagate_to_attribute([entity_metadata])
        
        assert result['tenant_id'] == "tenant_acme"
        assert result['interaction_id'] == "int_550e8400-e29b-41d4-a716-446655440000"
        assert result['user_id'] == "usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8"
    
    def test_attribute_propagation_multiple_entities(self):
        """Test metadata propagation to attributes from multiple entities"""
        entity1_metadata = self.source_metadata.to_dict()
        entity1_metadata.pop('text', None)
        
        entity2_metadata = entity1_metadata.copy()
        entity2_metadata['interaction_id'] = "int_550e8400-e29b-41d4-a716-446655440001"
        entity2_metadata['user_id'] = "usr_6ba7b812-9dad-41d4-80b4-00c04fd430c9"
        entity2_metadata['timestamp'] = "2024-01-16T10:30:00Z"  # Later timestamp
        
        result = MetadataPropagationRules.propagate_to_attribute([entity1_metadata, entity2_metadata])
        
        assert result['tenant_id'] == "tenant_acme"
        assert 'interaction_ids' in result
        assert len(result['interaction_ids']) == 2
        assert 'user_ids' in result
        assert len(result['user_ids']) == 2
        assert result['timestamp'] == "2024-01-15T10:30:00Z"
    
    def test_community_propagation(self):
        """Test metadata propagation to communities"""
        member1 = {
            'tenant_id': "tenant_acme",
            'account_id': "acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            'interaction_id': "int_550e8400-e29b-41d4-a716-446655440000",
            'user_id': "usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8"
        }
        
        member2 = {
            'tenant_id': "tenant_acme",
            'account_id': "acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
            'interaction_ids': ["int_550e8400-e29b-41d4-a716-446655440001", "int_550e8400-e29b-41d4-a716-446655440002"],
            'user_ids': ["usr_6ba7b812-9dad-11d1-80b4-00c04fd430c9", "usr_6ba7b812-9dad-41d4-80b4-00c04fd430ca"]
        }
        
        result = MetadataPropagationRules.propagate_to_community([member1, member2])
        
        assert result['tenant_id'] == "tenant_acme"
        assert result['account_id'] == "acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8"
        
        assert 'interaction_ids' in result
        assert len(result['interaction_ids']) == 3  # 1 from member1 + 2 from member2
        assert 'user_ids' in result
        assert len(result['user_ids']) == 3  # 1 from member1 + 2 from member2
    
    def test_empty_entity_list_error(self):
        """Test that empty entity list raises error"""
        with pytest.raises(ValueError, match="Cannot create attribute without entity metadata"):
            MetadataPropagationRules.propagate_to_attribute([])
    
    def test_empty_community_list_error(self):
        """Test that empty community list raises error"""
        with pytest.raises(ValueError, match="Cannot create community without member metadata"):
            MetadataPropagationRules.propagate_to_community([])
    
    def test_validation_rules_semantic_unit(self):
        """Test validation rules for semantic units"""
        metadata = self.source_metadata.to_dict()
        errors = MetadataPropagationRules.validate_propagation_rules(metadata, 'semantic_unit')
        assert len(errors) == 0
        
        metadata.pop('text')
        errors = MetadataPropagationRules.validate_propagation_rules(metadata, 'semantic_unit')
        assert len(errors) >= 1
        assert any("semantic_unit must have text" in error for error in errors)
    
    def test_validation_rules_entity(self):
        """Test validation rules for entities"""
        metadata = self.source_metadata.to_dict()
        metadata.pop('text')  # Entities shouldn't have text
        
        errors = MetadataPropagationRules.validate_propagation_rules(metadata, 'entity')
        assert len(errors) == 0
        
        metadata['text'] = "Some text"
        errors = MetadataPropagationRules.validate_propagation_rules(metadata, 'entity')
        assert len(errors) >= 1
        assert any("entity should not contain text field" in error for error in errors)
    
    def test_validation_rules_community(self):
        """Test validation rules for communities"""
        metadata = {
            'tenant_id': "tenant_acme",
            'interaction_ids': ["int_550e8400-e29b-41d4-a716-446655440000"],
            'user_ids': ["usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8"]
        }
        
        errors = MetadataPropagationRules.validate_propagation_rules(metadata, 'community')
        assert len(errors) == 0
        
        metadata.pop('interaction_ids')
        errors = MetadataPropagationRules.validate_propagation_rules(metadata, 'community')
        assert len(errors) >= 1
        assert any("community must have interaction_id or interaction_ids" in error for error in errors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
