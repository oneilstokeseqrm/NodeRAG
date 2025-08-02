# Component Metadata Migration Guide

## Overview
All NodeRAG components now support EQMetadata for multi-tenant data association. This update enables every component to carry the required 8 metadata fields throughout the system while maintaining existing ID generation logic.

## Changes Made

### Base Class Updates
**File: `NodeRAG/src/component/unit.py`**
- Added `metadata` property with getter and setter
- Added metadata validation using `EQMetadata.validate()`
- Added `tenant_id` convenience property
- Maintains backward compatibility with existing code

```python
@property
def metadata(self) -> Optional[EQMetadata]:
    """Get EQ metadata for this unit"""
    return self._metadata

@metadata.setter
def metadata(self, value: EQMetadata):
    """Set EQ metadata with validation"""
    if value is not None:
        errors = value.validate()
        if errors:
            raise ValueError(f"Invalid metadata: {errors}")
    self._metadata = value
```

### Component Constructor Updates
All 7 components now accept optional `metadata` parameter:

1. **Entity** (`NodeRAG/src/component/entity.py`)
2. **document** (`NodeRAG/src/component/document.py`)
3. **Semantic_unit** (`NodeRAG/src/component/semantic_unit.py`)
4. **Relationship** (`NodeRAG/src/component/relationship.py`)
5. **Attribute** (`NodeRAG/src/component/attribute.py`)
6. **Community_summary** (`NodeRAG/src/component/community.py`)
7. **Text_unit** (`NodeRAG/src/component/text_unit.py`)

Each component follows the same pattern:
```python
def __init__(self, ..., metadata: Optional[EQMetadata] = None):
    super().__init__()
    # ... existing initialization ...
    
    if metadata:
        self.metadata = metadata
```

### Bug Fix
**Fixed Community_summary genid() type error:**
- Issue: `genid()` expected a list but was receiving string or None
- Solution: Added type checking and conversion in `hash_id` property

```python
@property
def hash_id(self):
    if not self._hash_id:
        if isinstance(self.community_node, str):
            self._hash_id = genid([self.community_node], "sha256")
        elif self.community_node is None:
            self._hash_id = genid([""], "sha256")
        else:
            self._hash_id = genid(self.community_node, "sha256")
    return self._hash_id
```

### Metadata Propagation
**Document component automatically propagates metadata to child text units:**
```python
def split(self) -> None:
    if not self._processed_context:
        self._processed_context = True
        texts = self.splitter.split(self.raw_context)
        # Propagate metadata to text units
        self.text_units = [Text_unit(text, metadata=self.metadata) for text in texts]
```

**Relationship component propagates metadata to Entity objects:**
```python
if relationship_tuple:
    self.source = Entity(relationship_tuple[0], metadata=metadata, text_hash_id=text_hash_id)
    self.target = Entity(relationship_tuple[2], metadata=metadata, text_hash_id=text_hash_id)
```

## Backward Compatibility
Components continue to work without metadata:

```python
# Old way (still works)
entity = Entity("Apple Inc")
assert entity.metadata is None
assert entity.tenant_id is None

# New way (with metadata)
entity = Entity("Apple Inc", metadata=eq_metadata)
assert entity.metadata == eq_metadata
assert entity.tenant_id == eq_metadata.tenant_id
```

## Usage Examples

### Creating Components with Metadata
```python
from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.src.component import Entity, document

# Create metadata
metadata = EQMetadata(
    tenant_id="tenant_12345678-1234-4567-8901-123456789012",
    interaction_id="int_12345678-1234-4567-8901-123456789012",
    interaction_type="email",
    text="Business interaction content",
    account_id="acc_12345678-1234-4567-8901-123456789012",
    timestamp="2024-01-01T12:00:00Z",
    user_id="usr_12345678-1234-4567-8901-123456789012",
    source_system="outlook"
)

# Create components with metadata
entity = Entity("Apple Inc", metadata=metadata)
doc = document("Document content", metadata=metadata)

# Access metadata
print(f"Entity tenant: {entity.tenant_id}")
print(f"Document tenant: {doc.tenant_id}")
```

### Metadata Validation
```python
# Invalid metadata will raise ValueError
try:
    invalid_metadata = EQMetadata(tenant_id="", ...)  # Empty tenant_id
    entity = Entity("Test", metadata=invalid_metadata)
except ValueError as e:
    print(f"Validation error: {e}")
```

## Migration Steps

### For Existing Code
1. **No immediate changes required** - existing code continues to work
2. **Gradual migration** - add metadata parameter when creating new components
3. **Pipeline updates** - Task 3.2 will update pipelines to pass metadata

### For New Development
1. Always pass `metadata` parameter when creating components
2. Ensure metadata validation is handled appropriately
3. Verify metadata propagation in document splitting scenarios

## Testing
Comprehensive test suite available at `tests/component/test_metadata_support.py`:

```bash
# Run metadata tests
python -m pytest tests/component/test_metadata_support.py -v

# Run all tests to ensure backward compatibility
python -m pytest tests/ -v
```

## What's Next
- **Task 3.2**: Update pipelines to pass metadata during component instantiation
- **Task 3.3**: Implement metadata-aware storage operations
- **Task 3.4**: Add metadata to graph operations
- **Task 3.5**: Implement tenant-aware retrieval filtering

## Technical Notes
- ID generation logic (`genid()`) remains unchanged
- Metadata is stored separately from component IDs
- All components inherit metadata functionality from `Unit_base`
- Metadata validation occurs at assignment time
- Thread-safe metadata operations (no shared state)

## Troubleshooting

### Common Issues
1. **Import errors**: Ensure `EQMetadata` is imported from `NodeRAG.standards.eq_metadata`
2. **Validation errors**: Check that all 8 required metadata fields are provided
3. **Type errors**: Ensure metadata parameter is `Optional[EQMetadata]` type

### Verification Commands
```bash
# Check all components have metadata parameter
grep -n "metadata: Optional\[EQMetadata\]" NodeRAG/src/component/*.py

# Verify Unit_base has metadata property
grep -A 10 "class Unit_base" NodeRAG/src/component/unit.py

# Test imports
python -c "from NodeRAG.src.component import Entity; print('Import successful')"
```
