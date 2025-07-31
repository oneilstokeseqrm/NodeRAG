"""
Example usage of ID generation in NodeRAG pipeline
"""
from NodeRAG.utils.id_generation import NodeIDGenerator, MetadataTracker
from NodeRAG.standards.eq_metadata import EQMetadata

metadata = EQMetadata(
    tenant_id="tenant_acme",
    interaction_id="int_550e8400-e29b-41d4-a716-446655440000",
    interaction_type="email",
    text="John Smith from Acme Corp called about the invoice.",
    account_id="acc_6ba7b810-9dad-41d4-80b4-00c04fd430c8",
    timestamp="2024-01-15T10:30:00Z",
    user_id="usr_6ba7b812-9dad-41d4-80b4-00c04fd430c8",
    source_system="outlook"
)

doc_id = NodeIDGenerator.generate_document_id(metadata.to_dict())
print(f"Document ID: {doc_id}")

sem_id = NodeIDGenerator.generate_semantic_unit_id(
    text=metadata.text,
    tenant_id=metadata.tenant_id,
    doc_id=doc_id,
    chunk_index=0
)
print(f"Semantic Unit ID: {sem_id}")

entity1_id = NodeIDGenerator.generate_entity_id(
    entity_name="John Smith",
    entity_type="PERSON",
    tenant_id=metadata.tenant_id
)
print(f"Entity 1 ID: {entity1_id}")

entity2_id = NodeIDGenerator.generate_entity_id(
    entity_name="Acme Corp",
    entity_type="ORGANIZATION",
    tenant_id=metadata.tenant_id
)
print(f"Entity 2 ID: {entity2_id}")

rel_id = NodeIDGenerator.generate_relationship_id(
    source_entity_id=entity1_id,
    target_entity_id=entity2_id,
    relationship_type="WORKS_FOR",
    tenant_id=metadata.tenant_id
)
print(f"Relationship ID: {rel_id}")

tracker = MetadataTracker()
tracker.record_node_creation(doc_id, 'document', [], metadata.to_dict())
tracker.record_node_creation(sem_id, 'semantic_unit', [doc_id], metadata.to_dict())
tracker.record_node_creation(entity1_id, 'entity', [sem_id], metadata.to_dict())

print("\nLineage for entity:", entity1_id)
lineage = tracker.get_lineage_tree(entity1_id)
print(f"  Type: {lineage['type']}")
print(f"  Sources: {lineage['sources']}")
print(f"  Tenant: {lineage['metadata']['tenant_id']}")
