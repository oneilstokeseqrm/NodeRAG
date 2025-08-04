from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import re
from uuid import UUID


@dataclass
class EQMetadata:
    """Standard metadata for all EQ nodes and edges in NodeRAG"""
    tenant_id: str
    interaction_id: str  
    interaction_type: str
    text: str
    account_id: str
    timestamp: str  # ISO8601 format
    user_id: str
    source_system: str
    
    node_hash_id: Optional[str] = None  # SHA-256 from NodeRAG
    node_type: Optional[str] = None  # 'entity', 'semantic_unit', etc.
    
    created_at: Optional[str] = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def validate(self) -> List[str]:
        """Validate all required fields are non-empty and properly formatted"""
        errors = []
        
        required_fields = [
            'tenant_id', 'interaction_id', 'interaction_type', 
            'text', 'account_id', 'timestamp', 'user_id', 'source_system'
        ]
        
        for field_name in required_fields:
            value = getattr(self, field_name)
            if field_name == 'user_id':
                continue  # Handle user_id separately with flexible validation
            if not value or (isinstance(value, str) and not value.strip()):
                errors.append(f"{field_name} cannot be empty")
        
        if self.interaction_id and not self._validate_uuid_format(self.interaction_id, 'int_'):
            errors.append(f"interaction_id must be UUID v4 format with 'int_' prefix")
            
        if self.account_id and not self._validate_uuid_format(self.account_id, 'acc_'):
            errors.append(f"account_id must be UUID v4 format with 'acc_' prefix")
            
        if not self.user_id or not isinstance(self.user_id, str) or not self.user_id.strip():
            errors.append(f"user_id must be a non-empty string")
        
        if self.timestamp and not self._validate_iso8601(self.timestamp):
            errors.append(f"timestamp must be ISO8601 format (YYYY-MM-DDTHH:MM:SSZ)")
        
        valid_interaction_types = ['call', 'chat', 'email', 'voice_memo', 'custom_notes']
        if self.interaction_type and self.interaction_type not in valid_interaction_types:
            errors.append(f"interaction_type must be one of: {', '.join(valid_interaction_types)}")
        
        valid_source_systems = ['internal', 'voice_memo', 'custom', 'outlook', 'gmail']
        if self.source_system and self.source_system not in valid_source_systems:
            errors.append(f"source_system must be one of: {', '.join(valid_source_systems)}")
        
        return errors
    
    def _validate_uuid_format(self, value: str, expected_prefix: str = '') -> bool:
        """Validate UUID v4 format with optional prefix"""
        if expected_prefix and not value.startswith(expected_prefix):
            return False
        
        uuid_part = value[len(expected_prefix):] if expected_prefix else value
        pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        return bool(re.match(pattern, uuid_part, re.IGNORECASE))
    
    def _validate_iso8601(self, timestamp: str) -> bool:
        """Validate ISO8601 timestamp format"""
        try:
            if not timestamp.endswith('Z'):
                return False
            datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return True
        except:
            return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/serialization"""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EQMetadata':
        """Create EQMetadata instance from dictionary"""
        return cls(**data)
    
    def copy_with_node_info(self, node_hash_id: str, node_type: str) -> 'EQMetadata':
        """Create a copy with NodeRAG-generated fields populated"""
        data = self.to_dict()
        data['node_hash_id'] = node_hash_id
        data['node_type'] = node_type
        return self.from_dict(data)
