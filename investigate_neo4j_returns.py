#!/usr/bin/env python3
"""Investigate Neo4j return type mismatches"""

import os
import sys
import asyncio
import json
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from NodeRAG.storage.neo4j_adapter import Neo4jAdapter
from NodeRAG.standards.eq_metadata import EQMetadata

async def test_clear_tenant_data_return_type():
    """Test the clear_tenant_data method return type issue"""
    
    results = {
        "method_signature": None,
        "actual_return": None,
        "expected_return": None,
        "test_expectation": None,
        "type_mismatch": False
    }
    
    adapter = Neo4jAdapter()
    
    metadata = EQMetadata(
        tenant_id="tenant_12345678-1234-4567-8901-123456789012",
        interaction_id="int_12345678-1234-4567-8901-123456789012",
        interaction_type="email",
        text="Return type test",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp="2024-01-01T12:00:00Z",
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="test"
    )
    
    try:
        await adapter.connect()
        
        test_node = {
            "id": "return_type_test_node",
            "type": "Entity",
            "content": "Test node for return type investigation",
            **metadata.to_dict()
        }
        
        await adapter.create_node(test_node)
        
        clear_result = await adapter.clear_tenant_data(metadata.tenant_id)
        
        results["actual_return"] = {
            "value": str(clear_result),
            "type": type(clear_result).__name__,
            "is_boolean": isinstance(clear_result, bool),
            "is_tuple": isinstance(clear_result, tuple),
            "is_int": isinstance(clear_result, int)
        }
        
        results["expected_return"] = {
            "type": "bool",
            "value": "True",
            "description": "Boolean success indicator"
        }
        
        results["test_expectation"] = {
            "assertion": "assert success is True",
            "expects_boolean": True,
            "expects_truthy": True
        }
        
        if not isinstance(clear_result, bool):
            results["type_mismatch"] = True
            results["mismatch_details"] = {
                "expected": "bool",
                "actual": type(clear_result).__name__,
                "test_will_fail": True,
                "reason": f"Test expects boolean True, got {type(clear_result).__name__}: {clear_result}"
            }
        
    except Exception as e:
        results["error"] = str(e)
        results["type_mismatch"] = True
        results["mismatch_details"] = {
            "error_during_test": str(e),
            "test_will_fail": True
        }
    finally:
        await adapter.close()
    
    return results

async def test_other_adapter_methods():
    """Test other adapter methods for return type consistency"""
    
    results = {
        "create_node": None,
        "get_node": None,
        "query": None,
        "add_nodes_batch": None,
        "statistics": None
    }
    
    adapter = Neo4jAdapter()
    
    metadata = EQMetadata(
        tenant_id="tenant_12345678-1234-4567-8901-123456789012",
        interaction_id="int_12345678-1234-4567-8901-123456789012",
        interaction_type="email",
        text="Method return test",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp="2024-01-01T12:00:00Z",
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="test"
    )
    
    try:
        await adapter.connect()
        
        node_data = {
            "id": "method_test_node",
            "type": "Entity",
            "content": "Method return type test",
            **metadata.to_dict()
        }
        
        create_result = await adapter.create_node(node_data)
        results["create_node"] = {
            "type": type(create_result).__name__,
            "value": str(create_result)[:100],
            "is_dict": isinstance(create_result, dict),
            "has_id": "id" in create_result if isinstance(create_result, dict) else False
        }
        
        if isinstance(create_result, dict) and "id" in create_result:
            node_id = create_result["id"]
        else:
            node_id = "method_test_node"
            
        get_result = await adapter.get_node(node_id)
        results["get_node"] = {
            "type": type(get_result).__name__,
            "is_dict": isinstance(get_result, dict),
            "is_none": get_result is None,
            "keys": list(get_result.keys()) if isinstance(get_result, dict) else None
        }
        
        query = "MATCH (n) WHERE n.tenant_id = $tenant_id RETURN n LIMIT 1"
        params = {"tenant_id": metadata.tenant_id}
        
        query_result = await adapter.query(query, params)
        results["query"] = {
            "type": type(query_result).__name__,
            "is_list": isinstance(query_result, list),
            "length": len(query_result) if isinstance(query_result, list) else None,
            "first_item_type": type(query_result[0]).__name__ if query_result and isinstance(query_result, list) else None
        }
        
        batch_nodes = [
            {
                "id": "batch_test_1",
                "type": "Entity",
                "content": "Batch test 1",
                **metadata.to_dict()
            },
            {
                "id": "batch_test_2", 
                "type": "Entity",
                "content": "Batch test 2",
                **metadata.to_dict()
            }
        ]
        
        batch_result = await adapter.add_nodes_batch(batch_nodes)
        results["add_nodes_batch"] = {
            "type": type(batch_result).__name__,
            "is_list": isinstance(batch_result, list),
            "is_dict": isinstance(batch_result, dict),
            "length": len(batch_result) if hasattr(batch_result, '__len__') else None
        }
        
        stats_result = await adapter.statistics()
        results["statistics"] = {
            "type": type(stats_result).__name__,
            "is_dict": isinstance(stats_result, dict),
            "keys": list(stats_result.keys()) if isinstance(stats_result, dict) else None
        }
        
        await adapter.delete_node("method_test_node")
        await adapter.delete_node("batch_test_1")
        await adapter.delete_node("batch_test_2")
        
    except Exception as e:
        results["error"] = str(e)
    finally:
        await adapter.close()
    
    return results

async def analyze_expected_vs_actual():
    """Compare expected return types with actual implementation"""
    
    expectations = {
        "clear_tenant_data": {
            "expected_type": "bool",
            "expected_value": "True for success, False for failure",
            "test_assertion": "assert success is True",
            "caller_usage": "Boolean success indicator"
        },
        "create_node": {
            "expected_type": "dict",
            "expected_structure": {"id": "str", "properties": "dict"},
            "caller_usage": "Node data with generated ID"
        },
        "get_node": {
            "expected_type": "dict or None",
            "expected_structure": {"id": "str", "labels": "list", "properties": "dict"},
            "caller_usage": "Node data or None if not found"
        },
        "query": {
            "expected_type": "list",
            "expected_structure": "list of records/dicts",
            "caller_usage": "Query results as list"
        },
        "add_nodes_batch": {
            "expected_type": "list",
            "expected_structure": "list of created node data",
            "caller_usage": "Batch operation results"
        }
    }
    
    clear_results = await test_clear_tenant_data_return_type()
    method_results = await test_other_adapter_methods()
    
    comparison = {
        "expectations": expectations,
        "actual_results": {
            "clear_tenant_data": clear_results,
            "other_methods": method_results
        },
        "mismatches": []
    }
    
    if clear_results.get("type_mismatch"):
        comparison["mismatches"].append({
            "method": "clear_tenant_data",
            "expected": expectations["clear_tenant_data"]["expected_type"],
            "actual": clear_results.get("actual_return", {}).get("type"),
            "impact": "Test failure - assertion expects boolean True"
        })
    
    return comparison

if __name__ == "__main__":
    print("=== Neo4j Return Type Investigation ===\n")
    
    loop = asyncio.get_event_loop()
    
    print("Testing clear_tenant_data return type...")
    clear_results = loop.run_until_complete(test_clear_tenant_data_return_type())
    
    print("\nTesting other adapter methods...")
    method_results = loop.run_until_complete(test_other_adapter_methods())
    
    print("\nComparing expected vs actual...")
    comparison = loop.run_until_complete(analyze_expected_vs_actual())
    
    investigation = {
        "clear_tenant_data_analysis": clear_results,
        "method_return_analysis": method_results,
        "expectation_comparison": comparison,
        "root_cause_findings": {
            "type_mismatches_found": [],
            "likely_issues": [],
            "confidence_level": "HIGH"
        }
    }
    
    if clear_results.get("type_mismatch"):
        investigation["root_cause_findings"]["type_mismatches_found"].append({
            "method": "clear_tenant_data",
            "issue": "Returns non-boolean type when test expects boolean",
            "details": clear_results.get("mismatch_details", {})
        })
        investigation["root_cause_findings"]["likely_issues"].append(
            "clear_tenant_data method implementation returns wrong type"
        )
    
    for method, result in method_results.items():
        if result and result.get("type") == "tuple" and method != "statistics":
            investigation["root_cause_findings"]["type_mismatches_found"].append({
                "method": method,
                "issue": "Unexpected tuple return type",
                "type": result.get("type")
            })
    
    with open("neo4j_return_type_investigation.json", "w") as f:
        json.dump(investigation, f, indent=2)
    
    print("\nInvestigation complete. See neo4j_return_type_investigation.json")
