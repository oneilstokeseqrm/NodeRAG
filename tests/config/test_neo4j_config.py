"""Test configuration for Neo4j"""
import os
from typing import Dict, Any

def get_test_neo4j_config() -> Dict[str, Any]:
    """Get Neo4j configuration for testing"""
    return {
        "uri": os.getenv("NEO4J_URI", "neo4j+s://b875880c.databases.neo4j.io"),
        "user": os.getenv("NEO4J_USERNAME", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "GjiwqwFNiYJnIIZwFcnInbdH2a61f40mJnPolkUd1u4"),
        "database": os.getenv("NEO4J_DATABASE", "neo4j")
    }
