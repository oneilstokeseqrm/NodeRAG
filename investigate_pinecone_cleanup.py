#!/usr/bin/env python3
"""Investigate Pinecone namespace cleanup and connection issues"""

import os
import sys
import asyncio
import json
import time
from typing import Optional, List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from NodeRAG.storage.pinecone_adapter import PineconeAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
import numpy as np

async def test_namespace_lifecycle():
    """Test complete namespace lifecycle: create, list, use, delete"""
    
    results = {
        "namespace_creation": None,
        "namespace_listing": None,
        "namespace_usage": None,
        "namespace_deletion": None,
        "cleanup_issues": []
    }
    
    adapter = PineconeAdapter()
    test_namespace = "test_lifecycle_namespace"
    
    metadata = EQMetadata(
        tenant_id="tenant_12345678-1234-4567-8901-123456789012",
        interaction_id="int_12345678-1234-4567-8901-123456789012",
        interaction_type="email",
        text="Namespace lifecycle test",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp="2024-01-01T12:00:00Z",
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="test"
    )
    
    try:
        vector_data = {
            "id": "lifecycle_test_vector",
            "values": np.random.rand(3072).tolist(),  # Use 3072 dimensions
            "metadata": metadata.to_dict()
        }
        
        upsert_result = await adapter.upsert_vectors([vector_data], namespace=test_namespace)
        results["namespace_creation"] = {
            "success": upsert_result is not None,
            "result_type": type(upsert_result).__name__,
            "error": None
        }
        
        try:
            namespaces = await adapter.list_namespaces()
            results["namespace_listing"] = {
                "success": True,
                "namespace_found": test_namespace in namespaces if namespaces else False,
                "total_namespaces": len(namespaces) if namespaces else 0,
                "namespaces": namespaces[:5] if namespaces else []  # First 5 for brevity
            }
        except Exception as e:
            results["namespace_listing"] = {
                "success": False,
                "error": str(e)
            }
        
        try:
            search_vector = np.random.rand(3072).tolist()
            search_results = await adapter.search_vectors(
                query_vector=search_vector,
                top_k=1,
                namespace=test_namespace
            )
            results["namespace_usage"] = {
                "success": True,
                "results_found": len(search_results) if search_results else 0,
                "result_type": type(search_results).__name__
            }
        except Exception as e:
            results["namespace_usage"] = {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }
            if "404" in str(e) or "not found" in str(e).lower():
                results["cleanup_issues"].append("Namespace not found during search - possible cleanup timing issue")
        
        try:
            delete_result = await adapter.delete_namespace(test_namespace)
            results["namespace_deletion"] = {
                "success": True,
                "result": delete_result,
                "result_type": type(delete_result).__name__
            }
        except Exception as e:
            results["namespace_deletion"] = {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }
            if "404" in str(e) or "not found" in str(e).lower():
                results["cleanup_issues"].append("404 error during namespace deletion - namespace may not exist")
        
    except Exception as e:
        results["general_error"] = str(e)
        results["cleanup_issues"].append(f"General Pinecone error: {str(e)}")
    
    return results

async def test_concurrent_namespace_operations():
    """Test concurrent namespace operations that might cause 404 errors"""
    
    results = {
        "concurrent_creates": None,
        "concurrent_deletes": None,
        "race_conditions": []
    }
    
    adapter = PineconeAdapter()
    
    async def create_and_delete_namespace(namespace_suffix: str):
        """Create and immediately delete a namespace"""
        namespace = f"test_concurrent_{namespace_suffix}"
        
        try:
            vector_data = {
                "id": f"test_vector_{namespace_suffix}",
                "values": np.random.rand(3072).tolist(),
                "metadata": {"test": "concurrent"}
            }
            await adapter.upsert_vectors([vector_data], namespace=namespace)
            
            await asyncio.sleep(0.1)
            
            await adapter.delete_namespace(namespace)
            return {"success": True, "namespace": namespace}
            
        except Exception as e:
            return {"success": False, "namespace": namespace, "error": str(e)}
    
    try:
        tasks = [create_and_delete_namespace(str(i)) for i in range(3)]
        concurrent_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        results["concurrent_creates"] = {
            "total_operations": len(concurrent_results),
            "successful": sum(1 for r in concurrent_results if isinstance(r, dict) and r.get("success")),
            "failed": sum(1 for r in concurrent_results if isinstance(r, dict) and not r.get("success")),
            "exceptions": sum(1 for r in concurrent_results if isinstance(r, Exception))
        }
        
        for result in concurrent_results:
            if isinstance(result, dict) and not result.get("success"):
                error = result.get("error", "")
                if "404" in error or "not found" in error.lower():
                    results["race_conditions"].append(f"404 error in concurrent operation: {error}")
                elif "already exists" in error.lower():
                    results["race_conditions"].append(f"Namespace collision: {error}")
        
    except Exception as e:
        results["general_error"] = str(e)
    
    return results

async def test_dimension_validation():
    """Test dimension validation that might cause upsert failures"""
    
    results = {
        "correct_dimensions": None,
        "incorrect_dimensions": None,
        "dimension_errors": []
    }
    
    adapter = PineconeAdapter()
    test_namespace = "test_dimensions"
    
    try:
        correct_vector = {
            "id": "correct_dim_test",
            "values": np.random.rand(3072).tolist(),
            "metadata": {"test": "correct_dimensions"}
        }
        
        correct_result = await adapter.upsert_vectors([correct_vector], namespace=test_namespace)
        results["correct_dimensions"] = {
            "success": correct_result is not None,
            "dimension_count": len(correct_vector["values"])
        }
        
        incorrect_vector = {
            "id": "incorrect_dim_test",
            "values": np.random.rand(1536).tolist(),
            "metadata": {"test": "incorrect_dimensions"}
        }
        
        try:
            incorrect_result = await adapter.upsert_vectors([incorrect_vector], namespace=test_namespace)
            results["incorrect_dimensions"] = {
                "success": True,  # Shouldn't succeed
                "unexpected": True,
                "dimension_count": len(incorrect_vector["values"])
            }
        except Exception as e:
            results["incorrect_dimensions"] = {
                "success": False,
                "expected_failure": True,
                "error": str(e),
                "dimension_count": len(incorrect_vector["values"])
            }
            if "dimension" in str(e).lower():
                results["dimension_errors"].append(f"Dimension validation error: {str(e)}")
        
        await adapter.delete_namespace(test_namespace)
        
    except Exception as e:
        results["general_error"] = str(e)
    
    return results

if __name__ == "__main__":
    print("=== Pinecone Cleanup Investigation ===\n")
    
    loop = asyncio.get_event_loop()
    
    print("Testing namespace lifecycle...")
    lifecycle_results = loop.run_until_complete(test_namespace_lifecycle())
    
    print("\nTesting concurrent operations...")
    concurrent_results = loop.run_until_complete(test_concurrent_namespace_operations())
    
    print("\nTesting dimension validation...")
    dimension_results = loop.run_until_complete(test_dimension_validation())
    
    investigation = {
        "namespace_lifecycle": lifecycle_results,
        "concurrent_operations": concurrent_results,
        "dimension_validation": dimension_results,
        "root_cause_analysis": {
            "namespace_404_errors": [],
            "dimension_issues": [],
            "timing_issues": [],
            "likely_causes": []
        }
    }
    
    if lifecycle_results.get("cleanup_issues"):
        investigation["root_cause_analysis"]["namespace_404_errors"].extend(lifecycle_results["cleanup_issues"])
    
    if concurrent_results.get("race_conditions"):
        investigation["root_cause_analysis"]["timing_issues"].extend(concurrent_results["race_conditions"])
    
    if dimension_results.get("dimension_errors"):
        investigation["root_cause_analysis"]["dimension_issues"].extend(dimension_results["dimension_errors"])
    
    if investigation["root_cause_analysis"]["namespace_404_errors"]:
        investigation["root_cause_analysis"]["likely_causes"].append("Namespace cleanup timing issues")
    
    if investigation["root_cause_analysis"]["dimension_issues"]:
        investigation["root_cause_analysis"]["likely_causes"].append("Dimension mismatch errors")
    
    if investigation["root_cause_analysis"]["timing_issues"]:
        investigation["root_cause_analysis"]["likely_causes"].append("Race conditions in concurrent operations")
    
    with open("pinecone_cleanup_investigation.json", "w") as f:
        json.dump(investigation, f, indent=2)
    
    print("\nInvestigation complete. See pinecone_cleanup_investigation.json")
