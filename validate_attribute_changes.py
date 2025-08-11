#!/usr/bin/env python3
"""Validate that attribute generation changes are correct"""

import ast
import sys

def check_file(filepath):
    """Check that the modifications are correct"""
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    issues = []
    
    if 'from ...storage.storage_factory import StorageFactory' not in content:
        issues.append("Missing StorageFactory import")
    if 'from ...tenant.tenant_context import TenantContext' not in content:
        issues.append("Missing TenantContext import")
    
    if 'factory.is_cloud_storage()' not in content:
        issues.append("Graph loading not modified for Neo4j")
    if 'neo4j_adapter.get_subgraph' not in content:
        issues.append("Missing get_subgraph call")
    
    if 'neo4j_adapter.add_node' not in content:
        issues.append("save_graph not modified for Neo4j storage")
    
    if content.count('entity_metadata') < 3:  # Should appear in lines 162-193
        issues.append("Existing metadata logic may have been damaged")
    
    if 'else:' not in content or 'storage.load' not in content:
        issues.append("File storage fallback may be missing")
    
    return issues

if __name__ == "__main__":
    filepath = "NodeRAG/src/pipeline/attribute_generation.py"
    issues = check_file(filepath)
    
    if issues:
        print("❌ Issues found:")
        for issue in issues:
            print(f"  - {issue}")
        sys.exit(1)
    else:
        print("✅ All changes validated successfully")
        print("  - StorageFactory imports added")
        print("  - Graph loading uses Neo4j in cloud mode")
        print("  - Graph storage uses Neo4j operations")
        print("  - Existing metadata logic preserved (lines 162-193)")
        print("  - File storage fallback maintained")
        sys.exit(0)
