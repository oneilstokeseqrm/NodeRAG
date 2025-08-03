import pytest
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.src.component import Entity, document, Semantic_unit, Relationship
from NodeRAG.utils.text_spliter import SemanticTextSplitter

def create_test_metadata(tenant_suffix="001"):
    """Create valid test metadata"""
    return EQMetadata(
        tenant_id=f"tenant_12345678-1234-4567-8901-123456789{tenant_suffix}",
        interaction_id=f"int_12345678-1234-4567-8901-123456789{tenant_suffix}",
        interaction_type="email",
        text="Integration test content",
        account_id=f"acc_12345678-1234-4567-8901-123456789{tenant_suffix}",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id=f"usr_12345678-1234-4567-8901-123456789{tenant_suffix}",
        source_system="outlook"
    )

class TestMetadataIntegration:
    """Integration tests for metadata propagation"""
    
    def test_document_to_text_unit_propagation(self):
        """Verify metadata propagates from document to text units"""
        metadata = create_test_metadata()
        
        doc = document(
            raw_context="This is a comprehensive test document that contains multiple sentences and paragraphs. The purpose of this document is to test metadata propagation from the parent document to all child text units that are created during the splitting process. Each text unit should inherit the complete metadata from the parent document, including tenant information, interaction details, and all other required fields. This ensures proper data lineage and tenant isolation throughout the system.",
            metadata=metadata,
            splitter=SemanticTextSplitter(chunk_size=100, model_name="gpt-3.5-turbo")
        )
        
        doc.split()
        
        assert len(doc.text_units) > 1, "Document should split into multiple units"
        
        for text_unit in doc.text_units:
            assert text_unit.metadata == metadata
            assert text_unit.tenant_id == metadata.tenant_id
            print(f"✓ Text unit has tenant_id: {text_unit.tenant_id}")
    
    def test_relationship_to_entity_propagation(self):
        """Verify metadata propagates from relationship to entities"""
        metadata = create_test_metadata()
        
        rel = Relationship(
            relationship_tuple=["Apple Inc", "employs", "John Doe"],
            metadata=metadata
        )
        
        assert rel.source.metadata == metadata
        assert rel.target.metadata == metadata
        assert rel.source.tenant_id == metadata.tenant_id
        assert rel.target.tenant_id == metadata.tenant_id
        print(f"✓ Source entity tenant_id: {rel.source.tenant_id}")
        print(f"✓ Target entity tenant_id: {rel.target.tenant_id}")
    
    def test_multi_tenant_isolation(self):
        """Verify different tenants get different metadata"""
        metadata1 = create_test_metadata("001")
        metadata2 = create_test_metadata("002")
        
        entity1 = Entity("Apple Inc", metadata=metadata1)
        entity2 = Entity("Apple Inc", metadata=metadata2)
        
        assert entity1.hash_id == entity2.hash_id  # Same content = same ID
        assert entity1.tenant_id != entity2.tenant_id  # Different tenants
        
        print(f"✓ Entity 1 tenant: {entity1.tenant_id}")
        print(f"✓ Entity 2 tenant: {entity2.tenant_id}")
        print(f"✓ Both have same hash_id: {entity1.hash_id}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
