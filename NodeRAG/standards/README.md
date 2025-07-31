# EQ Metadata Standard

This directory contains the metadata standard for the NodeRAG EQ integration.

## Overview

All nodes and edges in the NodeRAG graph must include 8 required metadata fields plus NodeRAG-generated fields. This standard ensures consistent metadata handling throughout the entire system.

### Required Fields (8 fields)

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `tenant_id` | String | Tenant identifier | `"tenant_acme"` |
| `interaction_id` | UUID v4 | Unique interaction identifier with prefix | `"int_6ba7b810-9dad-11d1-80b4-00c04fd430c8"` |
| `interaction_type` | Enum | Type of customer interaction | `"email"` |
| `text` | String | Full interaction content/transcript | `"Subject: Invoice Question..."` |
| `account_id` | UUID v4 | Account identifier with prefix | `"acc_6ba7b811-9dad-11d1-80b4-00c04fd430c8"` |
| `timestamp` | ISO8601 | Interaction timestamp in UTC | `"2024-01-15T10:30:00Z"` |
| `user_id` | UUID v4 | User/agent identifier with prefix | `"usr_6ba7b812-9dad-11d1-80b4-00c04fd430c8"` |
| `source_system` | Enum | System that captured the interaction | `"outlook"` |

### NodeRAG-Generated Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `node_hash_id` | String | SHA-256 hash from NodeRAG | `"abc123def456..."` |
| `node_type` | String | Node type in graph | `"entity"`, `"semantic_unit"`, etc. |
| `created_at` | ISO8601 | Metadata creation timestamp | `"2024-01-15T10:30:00.123456+00:00"` |

## Field Validation Rules

### ID Standards
- **UUID v4 Format**: All IDs (except tenant_id) must follow UUID v4 format
- **Prefixed UUIDs**: IDs use prefixes for easier identification:
  - `int_` for interaction_id
  - `acc_` for account_id  
  - `usr_` for user_id
- **Tenant IDs**: Human-readable format for testing clarity (e.g., `tenant_acme`)

### Enum Values
- **interaction_type**: `call`, `chat`, `email`, `voice_memo`, `custom_notes`
- **source_system**: `internal`, `voice_memo`, `custom`, `outlook`, `gmail`

### Timestamp Format
- **ISO8601 Format**: Must end with 'Z' for UTC timezone
- **Example**: `"2024-01-15T10:30:00Z"`

## Usage

### Basic Usage

```python
from NodeRAG.standards.eq_metadata import EQMetadata

# Create metadata instance
metadata = EQMetadata(
    tenant_id="tenant_acme",
    interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
    interaction_type="email",
    text="Subject: Invoice Question\n\nHi, I have a question about my recent invoice...",
    account_id="acc_6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    timestamp="2024-01-15T10:30:00Z",
    user_id="usr_6ba7b812-9dad-11d1-80b4-00c04fd430c8",
    source_system="outlook"
)

# Validate metadata
errors = metadata.validate()
if errors:
    print(f"Validation errors: {errors}")
else:
    print("Metadata is valid!")

# Convert to dictionary for storage
data_dict = metadata.to_dict()

# Create from dictionary
metadata_from_dict = EQMetadata.from_dict(data_dict)

# Add NodeRAG-generated fields
node_metadata = metadata.copy_with_node_info(
    node_hash_id="abc123def456",
    node_type="entity"
)
```

### Validation Example

```python
# This will fail validation
invalid_metadata = EQMetadata(
    tenant_id="",  # Empty field
    interaction_id="invalid-uuid",  # Invalid UUID format
    interaction_type="invalid_type",  # Invalid enum
    text="Test content",
    account_id="acc_6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    timestamp="2024-01-15 10:30:00",  # Invalid timestamp format
    user_id="usr_6ba7b812-9dad-11d1-80b4-00c04fd430c8",
    source_system="invalid_system"  # Invalid enum
)

errors = invalid_metadata.validate()
# errors will contain:
# - "tenant_id cannot be empty"
# - "interaction_id must be UUID v4 format with 'int_' prefix"
# - "interaction_type must be one of: call, chat, email, voice_memo, custom_notes"
# - "timestamp must be ISO8601 format (YYYY-MM-DDTHH:MM:SSZ)"
# - "source_system must be one of: internal, voice_memo, custom, outlook, gmail"
```

## Propagation Rules

The `metadata_propagation.py` module defines how metadata flows through different node types in the NodeRAG pipeline.

### Usage

```python
from NodeRAG.standards.metadata_propagation import MetadataPropagationRules

# Propagate to semantic units (inherits all fields)
semantic_metadata = MetadataPropagationRules.propagate_to_semantic_unit(source_metadata)

# Propagate to entities (removes text field)
entity_metadata = MetadataPropagationRules.propagate_to_entity(source_metadata)

# Propagate to relationships (removes text field)
relationship_metadata = MetadataPropagationRules.propagate_to_relationship(source_metadata)

# Propagate to attributes (aggregates from multiple entities)
attribute_metadata = MetadataPropagationRules.propagate_to_attribute([entity1, entity2])

# Propagate to communities (aggregates from multiple members)
community_metadata = MetadataPropagationRules.propagate_to_community([member1, member2])
```

### Propagation Rules Summary

| Node Type | Text Field | ID Fields | Aggregation |
|-----------|------------|-----------|-------------|
| **Semantic Unit** | ✅ Preserved | ✅ Individual | None |
| **Entity** | ❌ Removed | ✅ Individual | None |
| **Relationship** | ❌ Removed | ✅ Individual | None |
| **Attribute** | ❌ Removed | ✅ Individual or Lists | From multiple entities |
| **Community** | ❌ Removed | ✅ Lists only | From multiple members |

### Validation

```python
# Validate propagation rules
errors = MetadataPropagationRules.validate_propagation_rules(metadata_dict, 'entity')
if errors:
    print(f"Propagation validation errors: {errors}")
```

## Testing

Run the test suite to validate the metadata standard:

```bash
# Run all tests
pytest tests/standards/test_eq_metadata.py -v

# Run with coverage
pytest tests/standards/test_eq_metadata.py -v --cov=NodeRAG.standards

# Generate HTML test report
pytest tests/standards/test_eq_metadata.py -v --html=metadata_standard_validation.html
```

## Integration with NodeRAG

This metadata standard is designed to integrate with the existing NodeRAG pipeline:

1. **Document Processing**: Original documents are enriched with EQ metadata
2. **Graph Construction**: All nodes inherit appropriate metadata fields
3. **Storage**: Metadata is preserved in all storage formats (parquet, pickle, etc.)
4. **Retrieval**: Metadata enables tenant-aware and context-aware search

## Next Steps

This metadata standard provides the foundation for:
- **Task 1.2**: EQ Configuration Module integration
- **Task 3.1**: Node creation with metadata propagation
- **Future Tasks**: Storage adapters and pipeline integration

## Files in this Package

- `eq_metadata.py`: Core EQMetadata dataclass with validation
- `metadata_propagation.py`: Rules for metadata flow through node types
- `__init__.py`: Package exports
- `README.md`: This documentation
- `../tests/standards/test_eq_metadata.py`: Comprehensive test suite
