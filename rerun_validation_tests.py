"""Re-run all Phase 3 validation tests with fixed config"""
import subprocess
import sys
import os
from datetime import datetime

print("=== Re-running Phase 3 Validation Tests ===")
print(f"Started: {datetime.now().isoformat()}\n")

os.chdir('/home/ubuntu/repos/NodeRAG')

tests = [
    ('Semantic Unit Metadata Tests', 'test_semantic_unit_metadata.py'),
    ('Integration Tests', 'test_semantic_unit_integration.py'), 
    ('Pipeline Flow Tests', 'test_pipeline_metadata_flow.py'),
    ('Entity Metadata Verification', 'verify_entity_metadata.py'),
    ('Document Pipeline Verification', 'verify_document_pipeline.py'),
    ('Quick Integration Test', 'quick_integration_test.py'),
]

results = []
all_passed = True

for test_name, test_file in tests:
    print(f"\n=== Running {test_name} ===")
    
    try:
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print(f"✅ {test_name} - PASSED")
            results.append((test_name, 'PASSED', None))
        else:
            print(f"❌ {test_name} - FAILED")
            print(f"Error output:\n{result.stderr}")
            results.append((test_name, 'FAILED', result.stderr))
            all_passed = False
            
    except subprocess.TimeoutExpired:
        print(f"⚠️  {test_name} - TIMEOUT")
        results.append((test_name, 'TIMEOUT', 'Test exceeded 60 seconds'))
        all_passed = False
    except Exception as e:
        print(f"❌ {test_name} - ERROR: {e}")
        results.append((test_name, 'ERROR', str(e)))
        all_passed = False

print("\n" + "="*50)
print("VALIDATION SUMMARY")
print("="*50)

for test_name, status, error in results:
    print(f"{test_name}: {status}")
    if error and len(error) < 200:
        print(f"  Error: {error}")

print(f"\nOverall Status: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
print(f"Completed: {datetime.now().isoformat()}")

with open('validation_rerun_summary.txt', 'w') as f:
    f.write(f"Phase 3 Validation Re-run Summary\n")
    f.write(f"Generated: {datetime.now().isoformat()}\n\n")
    
    f.write("Test Results:\n")
    for test_name, status, error in results:
        f.write(f"- {test_name}: {status}\n")
        if error:
            f.write(f"  Error: {error[:200]}...\n" if len(error) > 200 else f"  Error: {error}\n")
    
    f.write(f"\nOverall Status: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}\n")
    
    if all_passed:
        f.write("\nRecommendation: Proceed to Task 3.3c (relationships)\n")
    else:
        f.write("\nRecommendation: Investigate and fix remaining failures\n")

print(f"\nSummary saved to: validation_rerun_summary.txt")

exit(0 if all_passed else 1)
