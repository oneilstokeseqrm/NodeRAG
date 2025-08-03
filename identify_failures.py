#!/usr/bin/env python3
"""Systematically identify and categorize the 5 critical test failures"""

import subprocess
import json
import re
from typing import Dict, List, Any

def run_test_and_capture_output(test_path: str) -> Dict[str, Any]:
    """Run a specific test and capture detailed output"""
    try:
        result = subprocess.run(
            ["python", "-m", "pytest", test_path, "-v", "--tb=short"],
            capture_output=True,
            text=True,
            cwd="/home/ubuntu/repos/NodeRAG",
            timeout=30  # 30 second timeout to prevent hanging
        )
    except subprocess.TimeoutExpired:
        return {
            "test_path": test_path,
            "return_code": -1,
            "passed": False,
            "stdout": "",
            "stderr": "Test timed out after 30 seconds",
            "error_summary": "Test timeout - likely connection or hanging issue"
        }
    
    return {
        "test_path": test_path,
        "return_code": result.returncode,
        "passed": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "error_summary": extract_error_summary(result.stdout + result.stderr)
    }

def extract_error_summary(output: str) -> str:
    """Extract key error information from test output"""
    patterns = [
        r"assert .+ is .+",
        r"RuntimeError: .+",
        r"TypeError: .+", 
        r"ValueError: .+",
        r"404.*not found",
        r"namespace.*not.*found",
        r"tuple.*unpack",
        r"expected.*got",
        r"AssertionError: .+"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, output, re.IGNORECASE)
        if match:
            return match.group(0)
    
    return "No specific error pattern found"

def categorize_failure(test_path: str, error_summary: str) -> str:
    """Categorize failure by component and type"""
    if "pinecone" in test_path.lower():
        if "404" in error_summary or "namespace" in error_summary.lower():
            return "pinecone_namespace_cleanup"
        else:
            return "pinecone_other"
    elif "neo4j" in test_path.lower():
        if "tuple" in error_summary.lower() or "assert" in error_summary.lower():
            return "neo4j_return_type"
        else:
            return "neo4j_other"
    elif "transaction" in test_path.lower():
        if "rollback" in error_summary.lower() or "consistency" in error_summary.lower():
            return "transaction_consistency"
        else:
            return "transaction_other"
    else:
        return "other"

def main():
    """Identify and categorize all failing tests"""
    
    test_paths = [
        "tests/storage/test_neo4j_adapter.py::TestNeo4jAdapter::test_clear_tenant_data",
        "tests/storage/test_neo4j_adapter.py::TestNeo4jAdapter::test_add_nodes_batch",
        "tests/storage/test_neo4j_adapter.py::TestNeo4jAdapter::test_statistics",
        "tests/storage/test_neo4j_adapter.py::TestNeo4jAdapter::test_connection",
        
        "tests/storage/test_pinecone_adapter.py::TestPineconeAdapter::test_namespace_cleanup",
        "tests/storage/test_pinecone_adapter.py::TestPineconeAdapter::test_delete_vectors",
        "tests/storage/test_pinecone_adapter.py::TestPineconeAdapter::test_search_with_filters",
        "tests/storage/test_pinecone_adapter.py::TestPineconeAdapter::test_upsert_vectors",
        
        "tests/storage/test_transaction_manager.py::TestTransactionManager::test_rollback_with_mock_adapters",
        "tests/storage/test_transaction_manager.py::TestTransactionManager::test_batch_operations_success",
        "tests/storage/test_transaction_manager.py::TestTransactionManager::test_concurrent_transactions",
        
        "tests/storage/test_transaction_integration.py::TestTransactionIntegration::test_successful_node_and_embedding_creation",
        "tests/storage/test_transaction_integration.py::TestTransactionIntegration::test_batch_transaction_success",
        "tests/storage/test_transaction_integration.py::TestTransactionIntegration::test_rollback_on_pinecone_failure",
        "tests/storage/test_transaction_integration.py::TestTransactionIntegration::test_consistency_after_multiple_transactions",
        
        "tests/storage/test_transaction_fixes.py::test_data_consistency_maintained",
        
        "tests/integration/test_pinecone_integration.py",
        "tests/integration/test_neo4j_integration.py"
    ]
    
    results = []
    failures_by_category = {
        "pinecone_namespace_cleanup": [],
        "pinecone_other": [],
        "neo4j_return_type": [],
        "neo4j_other": [],
        "transaction_consistency": [],
        "transaction_integration": [],
        "transaction_other": [],
        "other": []
    }
    
    print("Testing individual test methods to identify failures...")
    
    for test_path in test_paths:
        print(f"Testing: {test_path}")
        result = run_test_and_capture_output(test_path)
        results.append(result)
        
        if not result["passed"]:
            if "transaction_integration" in test_path:
                category = "transaction_integration"
            elif "transaction_fixes" in test_path and "consistency" in test_path:
                category = "transaction_consistency"
            else:
                category = categorize_failure(test_path, result["error_summary"])
            
            failures_by_category[category].append(result)
            print(f"  FAILED: {result['error_summary']}")
        else:
            print(f"  PASSED")
    
    total_failures = sum(len(failures) for failures in failures_by_category.values())
    
    investigation_summary = {
        "total_tests_checked": len(test_paths),
        "total_failures_found": total_failures,
        "failures_by_category": {
            category: len(failures) 
            for category, failures in failures_by_category.items()
        },
        "detailed_failures": failures_by_category,
        "all_test_results": results,
        "exact_failing_tests": [
            result["test_path"] for result in results if not result["passed"]
        ]
    }
    
    with open("test_failures_categorized.json", "w") as f:
        json.dump(investigation_summary, f, indent=2)
    
    print(f"\nInvestigation complete:")
    print(f"Total failures found: {total_failures}")
    print(f"Pinecone namespace cleanup: {len(failures_by_category['pinecone_namespace_cleanup'])}")
    print(f"Pinecone other: {len(failures_by_category['pinecone_other'])}")
    print(f"Neo4j return type: {len(failures_by_category['neo4j_return_type'])}")
    print(f"Neo4j other: {len(failures_by_category['neo4j_other'])}")
    print(f"Transaction consistency: {len(failures_by_category['transaction_consistency'])}")
    print(f"Transaction integration: {len(failures_by_category['transaction_integration'])}")
    print(f"Transaction other: {len(failures_by_category['transaction_other'])}")
    print(f"Other: {len(failures_by_category['other'])}")
    print("\nExact failing tests:")
    for test in investigation_summary["exact_failing_tests"]:
        print(f"  - {test}")
    print("\nSee test_failures_categorized.json for detailed results")

if __name__ == "__main__":
    main()
