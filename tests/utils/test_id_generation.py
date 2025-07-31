import pytest
from NodeRAG.utils.id_generation import NodeIDGenerator, MetadataTracker

class TestNodeIDGenerator:
    """Test ID generation utilities"""
    
    def test_semantic_unit_id_generation(self):
        """Test semantic unit ID generation"""
        id1 = NodeIDGenerator.generate_semantic_unit_id(
            text="This is test content",
            tenant_id="tenant_acme",
            doc_id="doc_123",
            chunk_index=0
        )
        
        id2 = NodeIDGenerator.generate_semantic_unit_id(
            text="This is test content",
            tenant_id="tenant_acme",
            doc_id="doc_123",
            chunk_index=0
        )
        
        assert id1 == id2
        assert id1.startswith("sem_")
        assert len(id1) == 20  # sem_ + 16 chars
    
    def test_entity_id_deduplication(self):
        """Test entity ID enables deduplication"""
        id1 = NodeIDGenerator.generate_entity_id(
            entity_name="John Smith",
            entity_type="PERSON",
            tenant_id="tenant_acme"
        )
        
        id2 = NodeIDGenerator.generate_entity_id(
            entity_name="john smith",  # Different case
            entity_type="person",       # Different case
            tenant_id="tenant_acme"
        )
        
        assert id1 == id2  # Should be same due to normalization
    
    def test_entity_tenant_isolation(self):
        """Test entities are isolated by tenant"""
        id1 = NodeIDGenerator.generate_entity_id(
            entity_name="John Smith",
            entity_type="PERSON",
            tenant_id="tenant_acme"
        )
        
        id2 = NodeIDGenerator.generate_entity_id(
            entity_name="John Smith",
            entity_type="PERSON",
            tenant_id="tenant_beta"
        )
        
        assert id1 != id2  # Different tenants = different IDs
    
    def test_relationship_id_consistency(self):
        """Test relationship IDs are consistent regardless of direction"""
        id1 = NodeIDGenerator.generate_relationship_id(
            source_entity_id="ent_abc123",
            target_entity_id="ent_def456",
            relationship_type="WORKS_FOR",
            tenant_id="tenant_acme"
        )
        
        id2 = NodeIDGenerator.generate_relationship_id(
            source_entity_id="ent_def456",
            target_entity_id="ent_abc123",
            relationship_type="WORKS_FOR",
            tenant_id="tenant_acme"
        )
        
        assert id1 == id2  # Should be same due to sorting
    
    def test_community_id_generation(self):
        """Test community ID generation"""
        members = ["ent_123", "ent_456", "ent_789"]
        
        id1 = NodeIDGenerator.generate_community_id(
            member_entity_ids=members,
            tenant_id="tenant_acme",
            community_level=0
        )
        
        id2 = NodeIDGenerator.generate_community_id(
            member_entity_ids=["ent_789", "ent_123", "ent_456"],
            tenant_id="tenant_acme",
            community_level=0
        )
        
        assert id1 == id2  # Should be same due to sorting
    
    def test_id_format_validation(self):
        """Test ID format validation"""
        valid_ids = [
            "doc_1234567890abcdef",
            "sem_fedcba0987654321",
            "ent_0123456789abcdef",
            "rel_abcdef0123456789",
            "attr_0f1e2d3c4b5a6978",
            "comm_9876543210fedcba"
        ]
        
        for node_id in valid_ids:
            assert NodeIDGenerator.validate_id_format(node_id) is True
        
        invalid_ids = [
            "invalid_format",
            "doc_short",
            "doc_way_too_long_hash_value_here",
            "unknown_1234567890abcdef",
            "doc_UPPERCASE0NOT0HEX",
            ""
        ]
        
        for node_id in invalid_ids:
            assert NodeIDGenerator.validate_id_format(node_id) is False

class TestMetadataTracker:
    """Test metadata lineage tracking"""
    
    def test_lineage_tracking(self):
        """Test tracking node lineage"""
        tracker = MetadataTracker()
        
        doc_metadata = {
            'tenant_id': 'tenant_acme',
            'interaction_id': 'int_123'
        }
        tracker.record_node_creation(
            node_id='doc_001',
            node_type='document',
            source_ids=[],
            metadata=doc_metadata
        )
        
        tracker.record_node_creation(
            node_id='sem_001',
            node_type='semantic_unit',
            source_ids=['doc_001'],
            metadata=doc_metadata
        )
        
        entity_metadata = doc_metadata.copy()
        entity_metadata.pop('text', None)
        tracker.record_node_creation(
            node_id='ent_001',
            node_type='entity',
            source_ids=['sem_001'],
            metadata=entity_metadata
        )
        
        lineage = tracker.get_lineage_tree('ent_001')
        assert lineage['type'] == 'entity'
        assert 'sem_001' in lineage['ancestors']
        assert 'doc_001' in lineage['ancestors']['sem_001']['ancestors']
        
        doc_ids = tracker.find_source_documents('ent_001')
        assert doc_ids == ['doc_001']

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
