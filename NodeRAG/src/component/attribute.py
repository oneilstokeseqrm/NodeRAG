
from typing import Optional
from ...storage import genid
from ...utils.readable_index import attribute_index
from .unit import Unit_base
from ...standards.eq_metadata import EQMetadata

attribute_index_counter = attribute_index()

class Attribute(Unit_base):
    def __init__(self, raw_context: str = None, node: str = None, 
                 metadata: Optional[EQMetadata] = None):
        super().__init__()
        self.node = node
        self.raw_context = raw_context
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
            self._human_readable_id = attribute_index_counter.increment()
        return self._human_readable_id
