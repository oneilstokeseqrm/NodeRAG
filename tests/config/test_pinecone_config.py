"""Test configuration for Pinecone"""
import os
from typing import Dict, Any

def get_test_pinecone_config() -> Dict[str, Any]:
    """Get Pinecone configuration for testing"""
    return {
        "api_key": os.getenv("PINECONE_API_KEY", "pcsk_3EYvVL_Dxe7oL1cZsn1syqfNfHqAJVKKTMAcJPW3NnZjLmiNeD5aP7VVYuAzLzHWnsLccp"),
        "index_name": "noderag"
    }
