"""
ID generation utilities for NodeRAG nodes with EQ metadata
"""
import hashlib
from typing import List, Dict, Any, Optional
import json

class NodeIDGenerator:
    """Generate deterministic IDs for NodeRAG nodes"""
    
    @staticmethod
    def _compute_hash(components: List[str]) -> str:
        """Compute SHA-256 hash from components"""
        if len(components) > 1:
            sorted_components = [components[0]] + sorted(components[1:])
        else:
            sorted_components = components
            
        combined = "|".join(str(c) for c in sorted_components)
        return hashlib.sha256(combined.encode()).hexdigest()
    
    @staticmethod
    def generate_semantic_unit_id(text: str, tenant_id: str, doc_id: str, chunk_index: int) -> str:
        """Generate ID for semantic unit"""
        components = [text, tenant_id, doc_id, str(chunk_index)]
        return f"sem_{NodeIDGenerator._compute_hash(components)[:16]}"
    
    @staticmethod
    def generate_entity_id(entity_name: str, entity_type: str, tenant_id: str) -> str:
        """Generate ID for entity - enables deduplication across documents"""
        normalized_name = entity_name.strip().lower()
        components = [normalized_name, entity_type.lower(), tenant_id]
        return f"ent_{NodeIDGenerator._compute_hash(components)[:16]}"
    
    @staticmethod
    def generate_relationship_id(source_entity_id: str, target_entity_id: str, 
                                relationship_type: str, tenant_id: str) -> str:
        """Generate ID for relationship"""
        entity_ids = sorted([source_entity_id, target_entity_id])
        components = [entity_ids[0], entity_ids[1], relationship_type.lower(), tenant_id]
        return f"rel_{NodeIDGenerator._compute_hash(components)[:16]}"
    
    @staticmethod
    def generate_attribute_id(entity_id: str, attribute_name: str, tenant_id: str) -> str:
        """Generate ID for entity attribute"""
        components = [entity_id, attribute_name.lower(), tenant_id]
        return f"attr_{NodeIDGenerator._compute_hash(components)[:16]}"
    
    @staticmethod
    def generate_community_id(member_entity_ids: List[str], tenant_id: str, 
                             community_level: int = 0) -> str:
        """Generate ID for community"""
        sorted_members = sorted(member_entity_ids)
        components = [",".join(sorted_members), tenant_id, str(community_level)]
        return f"comm_{NodeIDGenerator._compute_hash(components)[:16]}"
    
    @staticmethod
    def generate_document_id(metadata: Dict[str, Any]) -> str:
        """Generate ID for document based on metadata"""
        components = [
            metadata['interaction_id'],
            metadata['tenant_id'],
            metadata['timestamp']
        ]
        return f"doc_{NodeIDGenerator._compute_hash(components)[:16]}"
    
    @staticmethod
    def validate_id_format(node_id: str) -> bool:
        """Validate that a node ID follows the expected format"""
        if not node_id or not isinstance(node_id, str):
            return False
        
        parts = node_id.split('_')
        if len(parts) != 2:
            return False
        
        prefix, hash_part = parts
        valid_prefixes = ['doc', 'sem', 'ent', 'rel', 'attr', 'comm']
        
        return (prefix in valid_prefixes and 
                len(hash_part) == 16 and 
                all(c in '0123456789abcdef' for c in hash_part))


class MetadataTracker:
    """Track metadata lineage through the graph"""
    
    def __init__(self):
        self.lineage: Dict[str, Dict[str, Any]] = {}
    
    def record_node_creation(self, node_id: str, node_type: str, 
                           source_ids: List[str], metadata: Dict[str, Any]):
        """Record node creation with source tracking"""
        self.lineage[node_id] = {
            'type': node_type,
            'sources': source_ids,
            'metadata': metadata,
            'created_at': metadata.get('created_at')
        }
    
    def get_lineage_tree(self, node_id: str) -> Dict[str, Any]:
        """Get complete lineage tree for a node"""
        if node_id not in self.lineage:
            return {}
        
        node_info = self.lineage[node_id].copy()
        node_info['ancestors'] = {}
        
        for source_id in node_info.get('sources', []):
            if source_id in self.lineage:
                node_info['ancestors'][source_id] = self.get_lineage_tree(source_id)
        
        return node_info
    
    def find_source_documents(self, node_id: str) -> List[str]:
        """Find all source document IDs for a given node"""
        if node_id not in self.lineage:
            return []
        
        node_info = self.lineage[node_id]
        if node_info['type'] == 'document':
            return [node_id]
        
        doc_ids = []
        for source_id in node_info.get('sources', []):
            doc_ids.extend(self.find_source_documents(source_id))
        
        return list(set(doc_ids))  # Remove duplicates
