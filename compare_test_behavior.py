#!/usr/bin/env python3
"""Compare test behavior with and without metadata changes"""

import subprocess
import json

def run_test_on_branch(branch_name, test_command):
    """Run test on specific branch and capture output"""
    print(f"\nTesting on branch: {branch_name}")
    
    subprocess.run(["git", "checkout", branch_name], capture_output=True)
    
    result = subprocess.run(
        test_command.split(),
        capture_output=True,
        text=True
    )
    
    return {
        "branch": branch_name,
        "return_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "passed": result.returncode == 0
    }

def main():
    test_command = "python -m pytest tests/storage/test_neo4j_adapter.py::TestNeo4jAdapter::test_connection -v"
    
    main_result = run_test_on_branch("main", test_command)
    
    subprocess.run(["git", "checkout", "c355762"], capture_output=True)
    previous_result = run_test_on_branch("c355762", test_command)
    
    subprocess.run(["git", "checkout", "main"], capture_output=True)
    
    comparison = {
        "test_command": test_command,
        "main_branch": {
            "passed": main_result["passed"],
            "error_snippet": main_result["stderr"][-500:] if not main_result["passed"] else "N/A"
        },
        "previous_commit": {
            "passed": previous_result["passed"],
            "error_snippet": previous_result["stderr"][-500:] if not previous_result["passed"] else "N/A"
        },
        "regression": main_result["passed"] != previous_result["passed"]
    }
    
    with open("test_comparison_report.json", "w") as f:
        json.dump(comparison, f, indent=2)
    
    print("\nComparison complete. See test_comparison_report.json")
    print(f"Regression detected: {comparison['regression']}")

if __name__ == "__main__":
    main()
