"""Test utilities for transaction manager"""
from typing import List, Dict, Any, Optional
import asyncio


class MockFailingAdapter:
    """Mock adapter that fails on demand for testing"""
    def __init__(self, fail_on_operation: Optional[str] = None):
        self.fail_on_operation = fail_on_operation
        self.operations_log = []
        
    async def add_node(self, *args, **kwargs):
        self.operations_log.append(("add_node", args, kwargs))
        if self.fail_on_operation == "add_node":
            raise Exception("Mock add_node failure")
        return True
    
    async def delete_node(self, node_id: str):
        self.operations_log.append(("delete_node", (node_id,), {}))
        return True
    
    async def upsert_vector(self, *args, **kwargs):
        self.operations_log.append(("upsert_vector", args, kwargs))
        if self.fail_on_operation == "upsert_vector":
            raise Exception("Mock upsert_vector failure")
        return True
    
    async def delete_vectors(self, vector_ids: List[str], namespace: str):
        self.operations_log.append(("delete_vectors", (vector_ids, namespace), {}))
        return True
