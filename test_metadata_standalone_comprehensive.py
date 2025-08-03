#!/usr/bin/env python3
"""Standalone comprehensive metadata test without full NodeRAG imports"""

import sys
import os
sys.path.insert(0, '.')

from datetime import datetime, timezone

from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.src.component.unit import Unit_base
from NodeRAG.src.component.entity import Entity
from NodeRAG.src.component.document import document
from NodeRAG.src.component.semantic_unit import Semantic_unit
from NodeRAG.src.component.text_unit import Text_unit
from NodeRAG.src.component.attribute import Attribute

def create_valid_metadata():
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

def create_invalid_metadata():
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

def test_entity_with_valid_metadata():
    """Test Entity accepts valid metadata"""
    print("Testing Entity with valid metadata...")
    metadata = create_valid_metadata()
    entity = Entity("Apple Inc", metadata=metadata)
    
    assert entity.metadata == metadata, "Entity metadata not set correctly"
    assert entity.tenant_id == "tenant_12345678-1234-4567-8901-123456789012", "Entity tenant_id not accessible"
    assert entity.raw_context == "Apple Inc", "Entity raw_context not preserved"
    assert entity.hash_id, "Entity hash_id not generated"
    print("✅ Entity with valid metadata - PASSED")

def test_entity_with_invalid_metadata():
    """Test Entity rejects invalid metadata"""
    print("Testing Entity with invalid metadata...")
    invalid_metadata = create_invalid_metadata()
    
    try:
        entity = Entity("Apple Inc", metadata=invalid_metadata)
        print("❌ Entity with invalid metadata - FAILED (should have raised ValueError)")
        return False
    except ValueError as e:
        if "Invalid metadata" in str(e):
            print("✅ Entity with invalid metadata - PASSED (correctly rejected)")
            return True
        else:
            print(f"❌ Entity with invalid metadata - FAILED (wrong error: {e})")
            return False

def test_entity_without_metadata():
    """Test Entity works without metadata (backward compatibility)"""
    print("Testing Entity without metadata...")
    entity = Entity("Apple Inc")
    
    assert entity.metadata is None, "Entity metadata should be None"
    assert entity.tenant_id is None, "Entity tenant_id should be None"
    assert entity.hash_id, "Entity hash_id should still generate"
    print("✅ Entity without metadata - PASSED")

def test_semantic_unit_with_metadata():
    """Test Semantic_unit with metadata"""
    print("Testing Semantic_unit with metadata...")
    metadata = create_valid_metadata()
    sem_unit = Semantic_unit(
        raw_context="Important business concept",
        metadata=metadata,
        text_hash_id="test_hash"
    )
    
    assert sem_unit.metadata == metadata, "Semantic_unit metadata not set"
    assert sem_unit.tenant_id == metadata.tenant_id, "Semantic_unit tenant_id not accessible"
    assert sem_unit.text_hash_id == "test_hash", "Semantic_unit text_hash_id not preserved"
    print("✅ Semantic_unit with metadata - PASSED")

def test_text_unit_with_metadata():
    """Test Text_unit with metadata"""
    print("Testing Text_unit with metadata...")
    metadata = create_valid_metadata()
    text_unit = Text_unit(
        raw_context="Test text content",
        metadata=metadata
    )
    
    assert text_unit.metadata == metadata, "Text_unit metadata not set"
    assert text_unit.tenant_id == metadata.tenant_id, "Text_unit tenant_id not accessible"
    assert text_unit.hash_id, "Text_unit hash_id not generated"
    print("✅ Text_unit with metadata - PASSED")

def test_attribute_with_metadata():
    """Test Attribute with metadata"""
    print("Testing Attribute with metadata...")
    metadata = create_valid_metadata()
    attribute = Attribute(
        raw_context="Test attribute",
        node="test_node",
        metadata=metadata
    )
    
    assert attribute.metadata == metadata, "Attribute metadata not set"
    assert attribute.tenant_id == metadata.tenant_id, "Attribute tenant_id not accessible"
    assert attribute.node == "test_node", "Attribute node not preserved"
    print("✅ Attribute with metadata - PASSED")

def test_all_components_inherit_from_unit_base():
    """Test all components inherit from Unit_base"""
    print("Testing component inheritance...")
    components = [Entity, document, Semantic_unit, Text_unit, Attribute]
    
    for comp_class in components:
        assert issubclass(comp_class, Unit_base), f"{comp_class.__name__} does not inherit from Unit_base"
    
    print("✅ All components inherit from Unit_base - PASSED")

def test_metadata_validation_in_base_class():
    """Test metadata validation in base class"""
    print("Testing metadata validation in base class...")
    
    entity = Entity("Test")
    
    valid_metadata = create_valid_metadata()
    entity.metadata = valid_metadata
    assert entity.metadata == valid_metadata, "Valid metadata not set correctly"
    
    invalid_metadata = create_invalid_metadata()
    try:
        entity.metadata = invalid_metadata
        print("❌ Metadata validation - FAILED (should have raised ValueError)")
        return False
    except ValueError as e:
        if "Invalid metadata" in str(e):
            print("✅ Metadata validation in base class - PASSED")
            return True
        else:
            print(f"❌ Metadata validation - FAILED (wrong error: {e})")
            return False

def run_all_tests():
    """Run all metadata tests"""
    print("=" * 60)
    print("COMPREHENSIVE METADATA TESTING")
    print("=" * 60)
    
    tests = [
        test_entity_with_valid_metadata,
        test_entity_with_invalid_metadata,
        test_entity_without_metadata,
        test_semantic_unit_with_metadata,
        test_text_unit_with_metadata,
        test_attribute_with_metadata,
        test_all_components_inherit_from_unit_base,
        test_metadata_validation_in_base_class
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            result = test()
            if result is not False:  # None or True means passed
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ {test.__name__} - FAILED with exception: {e}")
            failed += 1
        print()
    
    print("=" * 60)
    print(f"TEST SUMMARY: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return passed, failed

if __name__ == "__main__":
    passed, failed = run_all_tests()
    
    if failed == 0:
        print("🎉 ALL TESTS PASSED!")
        sys.exit(0)
    else:
        print(f"💥 {failed} TESTS FAILED!")
        sys.exit(1)
