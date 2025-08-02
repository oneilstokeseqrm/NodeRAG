from typing import Optional
from .unit import Unit_base
from ...storage import genid
from ...utils.readable_index import entity_index
from ...standards.eq_metadata import EQMetadata

entity_index_counter = entity_index()

class Entity(Unit_base):
    
    def __init__(self, raw_context: str, metadata: Optional[EQMetadata] = None, 
                 text_hash_id: str = None):
        super().__init__()
        self.raw_context = raw_context
        self.text_hash_id = text_hash_id
        self._hash_id = None
        self._human_readable_id = None
        
        if metadata:
            self.metadata = metadata
    @property
    def hash_id(self):
        if not self._hash_id:
            self._hash_id = genid([self.raw_context],"sha256")
        return self._hash_id
    @property
    def human_readable_id(self):
        if not self._human_readable_id:
            self._human_readable_id = entity_index_counter.increment()
        return self._human_readable_id
