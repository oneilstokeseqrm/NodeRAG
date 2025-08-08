#!/usr/bin/env python3
"""
Generate HTML validation report
"""
import subprocess
import json
from datetime import datetime

def generate_validation_report():
    """Generate comprehensive validation report"""
    
    result = subprocess.run(
        ["python", "validation/validate_main_branch.py"],
        capture_output=True,
        text=True
    )
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Multi-Tenant Post-Merge Validation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #2c3e50; color: white; padding: 20px; }}
        .success {{ color: green; font-weight: bold; }}
        .failure {{ color: red; font-weight: bold; }}
        .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; }}
        pre {{ background: #f4f4f4; padding: 10px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Multi-Tenant System Post-Merge Validation</h1>
        <p>Generated: {datetime.now().isoformat()}</p>
        <p>Branch: main (after PR #29)</p>
    </div>
    
    <div class="section">
        <h2>Validation Results</h2>
        <pre>{result.stdout}</pre>
    </div>
    
    <div class="section">
        <h2>Status</h2>
        <p class="{'success' if result.returncode == 0 else 'failure'}">
            {'✅ ALL TESTS PASSED - READY FOR PRODUCTION' if result.returncode == 0 else '❌ VALIDATION FAILED - DO NOT DEPLOY'}
        </p>
    </div>
</body>
</html>
    """
    
    with open("validation/validation_report.html", "w") as f:
        f.write(html_content)
    
    print(f"Report generated: validation/validation_report.html")
    return result.returncode == 0

if __name__ == "__main__":
    success = generate_validation_report()
    exit(0 if success else 1)
