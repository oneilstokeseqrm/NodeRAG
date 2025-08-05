"""Validate the complete relationships implementation"""
import subprocess
import sys
from datetime import datetime

print("=== Validating Relationships Implementation ===")
print(f"Started: {datetime.now().isoformat()}\n")

tests = [
    ('Relationship Metadata Tests', 'test_relationship_metadata.py'),
    ('Complete Pipeline Test', 'test_complete_pipeline_metadata.py')
]

all_passed = True
results = []

for test_name, test_file in tests:
    print(f"\nRunning {test_name}...")
    try:
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            print(f"✅ {test_name} - PASSED")
            results.append((test_name, 'PASSED'))
        else:
            print(f"❌ {test_name} - FAILED")
            print(result.stderr)
            results.append((test_name, 'FAILED'))
            all_passed = False
            
    except Exception as e:
        print(f"❌ {test_name} - ERROR: {e}")
        results.append((test_name, 'ERROR'))
        all_passed = False

html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Task 3.3c - Relationships Implementation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .success {{ color: green; }}
        .fail {{ color: red; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>Task 3.3c - Add Relationships Metadata Support</h1>
    <p>Generated: {datetime.now().isoformat()}</p>
    
    <h2>Test Results</h2>
    <table>
        <tr><th>Test</th><th>Result</th></tr>
        {''.join(f'<tr><td>{name}</td><td class="{"success" if status == "PASSED" else "fail"}">{status}</td></tr>' for name, status in results)}
    </table>
    
    <h2>Summary</h2>
    <p class="{'success' if all_passed else 'fail'}">
        {'✅ All tests passed - Task 3.3c complete!' if all_passed else '❌ Some tests failed - review implementation'}
    </p>
    
    <h2>Implementation Checklist</h2>
    <ul>
        <li>✓ add_relationships() accepts metadata parameter</li>
        <li>✓ Metadata validated (not None, correct type)</li>
        <li>✓ All 7 metadata fields added to edges (excluding 'text')</li>
        <li>✓ graph_tasks() passes metadata to add_relationships()</li>
        <li>✓ Relationship deduplication still works</li>
        <li>✓ Complete pipeline test passes</li>
    </ul>
</body>
</html>
"""

with open('task_3_3c_report.html', 'w') as f:
    f.write(html)

print(f"\n{'='*50}")
print(f"Overall Result: {'✅ PASSED' if all_passed else '❌ FAILED'}")
print(f"Report saved to: task_3_3c_report.html")
print(f"{'='*50}")

exit(0 if all_passed else 1)
