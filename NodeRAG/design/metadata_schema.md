# EQ Metadata Schema Design

## Overview

This document defines how metadata flows through the NodeRAG graph structure, ensuring complete traceability from source interactions to high-level communities.

## Node Types and Metadata

### 1. Document Level
**Initial Ingestion Point**
- Contains all 8 required metadata fields
- Source of metadata for all derived nodes

### 2. Semantic Units
**Purpose**: Chunks of text with semantic meaning
**Metadata**: 
- Inherits ALL fields from document
- Adds: `chunk_index`, `start_char`, `end_char`
- Node ID: Deterministic hash of (text + metadata)

### 3. Entities
**Purpose**: Named entities extracted from text
**Metadata**:
- Inherits all fields EXCEPT `text` 
- Adds: `entity_type`, `entity_name`, `confidence_score`
- Node ID: Deterministic hash of (entity_name + entity_type + tenant_id)

### 4. Relationships
**Purpose**: Connections between entities
**Metadata**:
- Inherits from source semantic units
- Removes `text` field
- Adds: `relationship_type`, `source_entity_id`, `target_entity_id`
- Node ID: Deterministic hash of (source + target + type + tenant_id)

### 5. Attributes
**Purpose**: Properties of entities aggregated across mentions
**Metadata**:
- Aggregates from multiple entity mentions
- Fields become lists: `interaction_ids[]`, `user_ids[]`
- Keeps earliest `timestamp`
- Node ID: Deterministic hash of (entity_id + attribute_name + tenant_id)

### 6. Communities
**Purpose**: High-level groupings of related entities
**Metadata**:
- Only preserves `tenant_id` (required)
- Optional `account_id` if all members share it
- Aggregates all `interaction_ids[]` and `user_ids[]` from members
- Node ID: Deterministic hash of (member_ids + tenant_id)

## Metadata Flow Diagram

```
Document (All 8 fields)
    ├── Semantic Unit 1 (All fields + chunk info)
    │   ├── Entity A (No text field)
    │   ├── Entity B (No text field)
    │   └── Relationship A→B (No text field)
    ├── Semantic Unit 2 (All fields + chunk info)
    │   ├── Entity A (No text field) ← Duplicate detection via ID
    │   └── Entity C (No text field)
    └── Semantic Unit 3 (All fields + chunk info)
        └── Entity B (No text field) ← Duplicate detection via ID

Aggregation Layer:
    Entity A (multiple mentions) → Attribute (Lists of IDs)
    Entity B (multiple mentions) → Attribute (Lists of IDs)
    
Community Layer:
    Community 1 (tenant_id + aggregated IDs from all members)
```

## ID Generation Rules

### Deterministic IDs
All node IDs are generated using SHA-256 hashes to ensure:
1. Same content + metadata = same ID (deduplication)
2. Tenant isolation (tenant_id in hash)
3. Reproducible across runs

### ID Components by Node Type

| Node Type | Hash Components |
|-----------|----------------|
| Semantic Unit | text + tenant_id + doc_id + chunk_index |
| Entity | entity_name + entity_type + tenant_id |
| Relationship | source_id + target_id + rel_type + tenant_id |
| Attribute | entity_id + attribute_name + tenant_id |
| Community | sorted(member_ids) + tenant_id |

## Metadata Validation Points

1. **Document Ingestion**: Validate all 8 fields present
2. **Node Creation**: Validate metadata matches node type rules
3. **Aggregation**: Validate list fields are properly merged
4. **Query Time**: Filter by tenant_id for isolation

## Cross-Reference Tracking

Each node maintains references to its source:
- Semantic Units → Document ID
- Entities → Semantic Unit IDs
- Relationships → Semantic Unit ID
- Attributes → Entity IDs
- Communities → Member Entity IDs

This enables full lineage tracking from any node back to source interactions.
