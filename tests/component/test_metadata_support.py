import pytest
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from NodeRAG.src.component.unit import Unit_base
from NodeRAG.src.component.entity import Entity
from NodeRAG.src.component.document import document
from NodeRAG.src.component.semantic_unit import Semantic_unit
from NodeRAG.src.component.relationship import Relationship
from NodeRAG.src.component.attribute import Attribute
from NodeRAG.src.component.text_unit import Text_unit
from NodeRAG.standards.eq_metadata import EQMetadata

@pytest.fixture
def valid_metadata():
    """Create valid EQMetadata for testing"""
    return EQMetadata(
        tenant_id="tenant_12345678-1234-4567-8901-123456789012",
        interaction_id="int_12345678-1234-4567-8901-123456789012",
        interaction_type="email",
        text="Test interaction content",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="outlook"
    )

@pytest.fixture
def invalid_metadata():
    """Create invalid EQMetadata for testing"""
    return EQMetadata(
        tenant_id="",  # Empty tenant_id
        interaction_id="bad_format",  # Wrong format
        interaction_type="invalid_type",
        text="Test",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp="2024-01-01",  # Wrong format
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="outlook"
    )

class TestComponentMetadata:
    """Test metadata support in components"""
    
    def test_entity_with_valid_metadata(self, valid_metadata):
        """Test Entity accepts valid metadata"""
        entity = Entity("Apple Inc", metadata=valid_metadata)
        
        assert entity.metadata == valid_metadata
        assert entity.tenant_id == "tenant_12345678-1234-4567-8901-123456789012"
        assert entity.raw_context == "Apple Inc"
        assert entity.hash_id  # Should generate ID normally
    
    def test_entity_with_invalid_metadata(self, invalid_metadata):
        """Test Entity rejects invalid metadata"""
        with pytest.raises(ValueError) as exc_info:
            Entity("Apple Inc", metadata=invalid_metadata)
        
        assert "Invalid metadata" in str(exc_info.value)
    
    def test_entity_without_metadata(self):
        """Test Entity works without metadata (backward compatibility)"""
        entity = Entity("Apple Inc")
        
        assert entity.metadata is None
        assert entity.tenant_id is None
        assert entity.hash_id  # Should still generate ID
    
    def test_semantic_unit_with_metadata(self, valid_metadata):
        """Test Semantic_unit with metadata"""
        sem_unit = Semantic_unit(
            raw_context="Important business concept",
            metadata=valid_metadata,
            text_hash_id="test_hash"
        )
        
        assert sem_unit.metadata == valid_metadata
        assert sem_unit.tenant_id == valid_metadata.tenant_id
        assert sem_unit.text_hash_id == "test_hash"
    
    def test_text_unit_with_metadata(self, valid_metadata):
        """Test Text_unit with metadata"""
        text_unit = Text_unit(
            raw_context="Sample text content",
            metadata=valid_metadata
        )
        
        assert text_unit.metadata == valid_metadata
        assert text_unit.tenant_id == valid_metadata.tenant_id
        assert text_unit.raw_context == "Sample text content"
    
    def test_relationship_with_metadata(self, valid_metadata):
        """Test Relationship with metadata"""
        relationship = Relationship(
            relationship_tuple=["Apple Inc", "employs", "John Doe"],
            metadata=valid_metadata
        )
        
        assert relationship.metadata == valid_metadata
        assert relationship.tenant_id == valid_metadata.tenant_id
        assert relationship.source.metadata == valid_metadata
        assert relationship.target.metadata == valid_metadata
    
    def test_attribute_with_metadata(self, valid_metadata):
        """Test Attribute with metadata"""
        attribute = Attribute(
            raw_context="Company size: Large",
            node="Apple Inc",
            metadata=valid_metadata
        )
        
        assert attribute.metadata == valid_metadata
        assert attribute.tenant_id == valid_metadata.tenant_id
        assert attribute.node == "Apple Inc"
    
    def test_document_without_metadata(self):
        """Test Document works without metadata (backward compatibility)"""
        doc = document(raw_context="This is a test document.")
        
        assert doc.metadata is None
        assert doc.tenant_id is None
        assert doc.hash_id  # Should still generate ID
    
    def test_all_components_inherit_from_unit_base(self):
        """Test all components properly inherit from Unit_base"""
        components = [Entity, document, Semantic_unit, Relationship, Attribute, Text_unit]
        
        for component_class in components:
            assert issubclass(component_class, Unit_base)
    
    def test_metadata_validation_in_base_class(self, valid_metadata, invalid_metadata):
        """Test metadata validation works through base class"""
        entity = Entity("Test")
        
        entity.metadata = valid_metadata
        assert entity.metadata == valid_metadata
        
        with pytest.raises(ValueError) as exc_info:
            entity.metadata = invalid_metadata
        assert "Invalid metadata" in str(exc_info.value)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
