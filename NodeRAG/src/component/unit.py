from abc import ABC, abstractmethod
from typing import Optional
from ...standards.eq_metadata import EQMetadata

class Unit_base(ABC):
    """Base class for all NodeRAG units with EQ metadata support"""
    
    def __init__(self):
        self._metadata: Optional[EQMetadata] = None
    
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
    
    @property
    def tenant_id(self) -> Optional[str]:
        """Convenience property for tenant_id"""
        return self._metadata.tenant_id if self._metadata else None
    
    @property
    @abstractmethod
    def hash_id(self):
        pass
    
    @property
    @abstractmethod
    def human_readable_id(self):
        pass
        
    def call_action(self, action: str, *args, **kwargs) -> None:
        method = getattr(self, action, None)
        
        if callable(method):
            method(*args, **kwargs)
        else:
            raise ValueError(f"Action {action} not found")
        

