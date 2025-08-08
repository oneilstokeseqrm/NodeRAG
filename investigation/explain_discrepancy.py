#!/usr/bin/env python3
"""
Explain discrepancy between claimed race condition and passing tests
"""
import subprocess
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def explain_discrepancy():
    print("INVESTIGATING DISCREPANCY")
    print("="*60)
    print("Claimed: 'critical race condition...nodes get mixed up between tenants'")
    print("Report shows: '✅ All concurrent operations maintained isolation'")
    print("="*60)
    
    print("\nRe-running the validation test that supposedly passed...")
    
    try:
        from validation.validate_main_branch import validate_concurrent_operations
        result = validate_concurrent_operations()
        print(f"Validation concurrent operations result: {result}")
    except Exception as e:
        print(f"Error running validation: {e}")
    
    print("\n" + "="*60)
    print("EXPLANATION NEEDED:")
    print("="*60)
    print("1. If the validation passed, where is the race condition?")
    print("2. If there's a race condition, why did the validation pass?")
    print("3. Show the EXACT error or mixed data that proves the race condition")
    
    print("\n" + "="*60)
    print("CHECKING PYTEST TEST THAT ALLEGEDLY FAILED:")
    print("="*60)
    
    try:
        result = subprocess.run([
            'python', '-m', 'pytest', 
            'tests/test_multi_tenant_isolation.py::TestMultiTenantIsolation::test_concurrent_tenant_operations',
            '-v', '--tb=short'
        ], capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        print("PYTEST OUTPUT:")
        print(result.stdout)
        if result.stderr:
            print("PYTEST STDERR:")
            print(result.stderr)
        print(f"PYTEST RETURN CODE: {result.returncode}")
        
        if result.returncode == 0:
            print("\n✅ PYTEST TEST ACTUALLY PASSES!")
            print("This suggests the race condition claim was FALSE ALARM")
        else:
            print("\n❌ PYTEST TEST FAILS - Race condition may be real")
            
    except Exception as e:
        print(f"Error running pytest: {e}")

if __name__ == "__main__":
    explain_discrepancy()
