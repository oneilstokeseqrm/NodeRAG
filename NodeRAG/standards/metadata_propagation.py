"""
Metadata Propagation Rules for NodeRAG EQ Integration

This module defines how metadata flows through the NodeRAG pipeline.
"""

from typing import Dict, List, Optional, Any
from .eq_metadata import EQMetadata


class MetadataPropagationRules:
    """Rules for how metadata propagates through different node types"""
    
    ALWAYS_PROPAGATE = [
        'tenant_id',
        'account_id', 
        'interaction_id',
        'source_system',
        'timestamp',
        'user_id',
        'interaction_type'
    ]
    
    AGGREGATE_FIELDS = {
        'interaction_id': 'collect_unique',  # Collect all unique interaction_ids
        'user_id': 'collect_unique',         # Collect all unique user_ids
        'timestamp': 'use_earliest'          # Use earliest timestamp
    }
    
    @staticmethod
    def propagate_to_semantic_unit(source_metadata: EQMetadata) -> Dict[str, Any]:
        """Rules for propagating metadata to semantic units"""
        return source_metadata.to_dict()
    
    @staticmethod
    def propagate_to_entity(source_metadata: EQMetadata) -> Dict[str, Any]:
        """Rules for propagating metadata to entities"""
        result = source_metadata.to_dict()
        result.pop('text', None)  # Remove text field for entities
        return result
    
    @staticmethod
    def propagate_to_relationship(source_metadata: EQMetadata) -> Dict[str, Any]:
        """Rules for propagating metadata to relationships"""
        result = source_metadata.to_dict()
        result.pop('text', None)
        return result
    
    @staticmethod
    def propagate_to_attribute(entity_metadata_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Rules for propagating metadata to attributes (from multiple entities)"""
        if not entity_metadata_list:
            raise ValueError("Cannot create attribute without entity metadata")
        
        result = entity_metadata_list[0].copy()
        
        all_interaction_ids = set()
        all_user_ids = set()
        earliest_timestamp = result.get('timestamp', '')
        
        for metadata in entity_metadata_list:
            if metadata.get('interaction_id'):
                all_interaction_ids.add(metadata['interaction_id'])
            if metadata.get('user_id'):
                all_user_ids.add(metadata['user_id'])
            if metadata.get('timestamp', '') and metadata['timestamp'] < earliest_timestamp:
                earliest_timestamp = metadata['timestamp']
        
        if len(all_interaction_ids) > 1:
            result['interaction_ids'] = list(all_interaction_ids)
            result.pop('interaction_id', None)
        
        if len(all_user_ids) > 1:
            result['user_ids'] = list(all_user_ids)
            result.pop('user_id', None)
        
        if earliest_timestamp:
            result['timestamp'] = earliest_timestamp
        
        return result
    
    @staticmethod
    def propagate_to_community(member_metadata_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Rules for propagating metadata to communities/high-level elements"""
        if not member_metadata_list:
            raise ValueError("Cannot create community without member metadata")
        
        result = {
            'tenant_id': member_metadata_list[0]['tenant_id']
        }
        
        account_ids = set(m.get('account_id') for m in member_metadata_list if m.get('account_id'))
        if len(account_ids) == 1:
            result['account_id'] = account_ids.pop()
        
        all_interaction_ids = set()
        all_user_ids = set()
        
        for metadata in member_metadata_list:
            if isinstance(metadata.get('interaction_ids'), list):
                all_interaction_ids.update(metadata['interaction_ids'])
            elif metadata.get('interaction_id'):
                all_interaction_ids.add(metadata['interaction_id'])
                
            if isinstance(metadata.get('user_ids'), list):
                all_user_ids.update(metadata['user_ids'])
            elif metadata.get('user_id'):
                all_user_ids.add(metadata['user_id'])
        
        if all_interaction_ids:
            result['interaction_ids'] = list(all_interaction_ids)
        if all_user_ids:
            result['user_ids'] = list(all_user_ids)
        
        return result
    
    @staticmethod
    def validate_propagation_rules(metadata: Dict[str, Any], node_type: str) -> List[str]:
        """Validate that propagated metadata follows the rules for the given node type"""
        errors = []
        
        if not metadata.get('tenant_id'):
            errors.append(f"{node_type} must have tenant_id")
        
        if node_type == 'semantic_unit':
            required_fields = MetadataPropagationRules.ALWAYS_PROPAGATE + ['text']
            for field in required_fields:
                if not metadata.get(field):
                    errors.append(f"semantic_unit must have {field}")
        
        elif node_type in ['entity', 'relationship']:
            if metadata.get('text'):
                errors.append(f"{node_type} should not contain text field")
        
        elif node_type == 'community':
            has_interaction_id = metadata.get('interaction_id') or metadata.get('interaction_ids')
            has_user_id = metadata.get('user_id') or metadata.get('user_ids')
            
            if not has_interaction_id:
                errors.append("community must have interaction_id or interaction_ids")
            if not has_user_id:
                errors.append("community must have user_id or user_ids")
        
        return errors
