#!/usr/bin/env python3
"""
EQ Test Data Validation Script

This script validates the test interaction data for the NodeRAG EQ project.
It checks all 8 required metadata fields and generates validation reports.

Required fields:
- tenant_id: string (test format: tenant_acme, tenant_beta)
- interaction_id: UUID v4 format with 'int_' prefix
- interaction_type: enum (call, chat, email, voice_memo, custom_notes)
- text: non-empty string
- account_id: UUID v4 format with 'acc_' prefix
- timestamp: ISO8601 format
- user_id: UUID v4 format with 'usr_' prefix
- source_system: enum (internal, voice_memo, custom, outlook, gmail)
"""

import json
import os
import re
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
import uuid

REQUIRED_FIELDS = [
    'tenant_id', 'interaction_id', 'interaction_type', 'text',
    'account_id', 'timestamp', 'user_id', 'source_system'
]

VALID_INTERACTION_TYPES = {'call', 'chat', 'email', 'voice_memo', 'custom_notes'}
VALID_SOURCE_SYSTEMS = {'internal', 'voice_memo', 'custom', 'outlook', 'gmail'}
VALID_TENANT_IDS = {'tenant_acme', 'tenant_beta'}

UUID_PATTERN = re.compile(r'^(int_|acc_|usr_)?[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE)

ISO8601_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')

class ValidationResult:
    def __init__(self):
        self.total_files = 0
        self.valid_files = 0
        self.errors = []
        self.warnings = []
        self.statistics = {
            'by_tenant': {},
            'by_interaction_type': {},
            'by_source_system': {},
            'by_account': {}
        }

def validate_uuid_format(value: str, field_name: str) -> List[str]:
    """Validate UUID v4 format with optional prefix."""
    errors = []
    if not UUID_PATTERN.match(value):
        errors.append(f"{field_name} '{value}' is not a valid UUID v4 format")
    return errors

def validate_timestamp(timestamp: str) -> List[str]:
    """Validate ISO8601 timestamp format."""
    errors = []
    if not ISO8601_PATTERN.match(timestamp):
        errors.append(f"timestamp '{timestamp}' is not valid ISO8601 format (YYYY-MM-DDTHH:MM:SSZ)")
    else:
        try:
            datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except ValueError:
            errors.append(f"timestamp '{timestamp}' is not a valid datetime")
    return errors

def validate_interaction_data(data: Dict[str, Any], filename: str) -> List[str]:
    """Validate a single interaction data object."""
    errors = []
    
    for field in REQUIRED_FIELDS:
        if field not in data:
            errors.append(f"Missing required field: {field}")
        elif not data[field] or (isinstance(data[field], str) and not data[field].strip()):
            errors.append(f"Field '{field}' is empty or contains only whitespace")
    
    if errors:
        return errors  # Don't continue validation if basic structure is wrong
    
    if data['tenant_id'] not in VALID_TENANT_IDS:
        errors.append(f"tenant_id '{data['tenant_id']}' is not valid. Must be one of: {VALID_TENANT_IDS}")
    
    uuid_fields = ['interaction_id', 'account_id', 'user_id']
    for field in uuid_fields:
        errors.extend(validate_uuid_format(data[field], field))
    
    if data['interaction_type'] not in VALID_INTERACTION_TYPES:
        errors.append(f"interaction_type '{data['interaction_type']}' is not valid. Must be one of: {VALID_INTERACTION_TYPES}")
    
    if data['source_system'] not in VALID_SOURCE_SYSTEMS:
        errors.append(f"source_system '{data['source_system']}' is not valid. Must be one of: {VALID_SOURCE_SYSTEMS}")
    
    errors.extend(validate_timestamp(data['timestamp']))
    
    if len(data['text'].strip()) < 10:
        errors.append("text field should contain meaningful content (at least 10 characters)")
    
    return errors

def collect_statistics(data: Dict[str, Any], stats: Dict[str, Dict[str, int]]):
    """Collect statistics from interaction data."""
    tenant_id = data.get('tenant_id', 'unknown')
    interaction_type = data.get('interaction_type', 'unknown')
    source_system = data.get('source_system', 'unknown')
    account_id = data.get('account_id', 'unknown')
    
    stats['by_tenant'][tenant_id] = stats['by_tenant'].get(tenant_id, 0) + 1
    stats['by_interaction_type'][interaction_type] = stats['by_interaction_type'].get(interaction_type, 0) + 1
    stats['by_source_system'][source_system] = stats['by_source_system'].get(source_system, 0) + 1
    stats['by_account'][account_id] = stats['by_account'].get(account_id, 0) + 1

def validate_test_data() -> ValidationResult:
    """Main validation function."""
    result = ValidationResult()
    
    base_path = Path(__file__).parent.parent / 'sample-interactions'
    json_files = list(base_path.rglob('*.json'))
    
    if not json_files:
        result.errors.append("No JSON files found in sample-interactions directory")
        return result
    
    result.total_files = len(json_files)
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            file_errors = validate_interaction_data(data, json_file.name)
            
            if file_errors:
                result.errors.extend([f"{json_file.name}: {error}" for error in file_errors])
            else:
                result.valid_files += 1
                collect_statistics(data, result.statistics)
                
        except json.JSONDecodeError as e:
            result.errors.append(f"{json_file.name}: Invalid JSON format - {str(e)}")
        except Exception as e:
            result.errors.append(f"{json_file.name}: Unexpected error - {str(e)}")
    
    return result

def generate_html_report(result: ValidationResult) -> str:
    """Generate HTML validation report."""
    html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EQ Test Data Validation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; color: #333; border-bottom: 2px solid #007acc; padding-bottom: 10px; margin-bottom: 20px; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px; }}
        .summary-card {{ background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #007acc; }}
        .summary-card h3 {{ margin: 0 0 10px 0; color: #333; }}
        .summary-card .value {{ font-size: 24px; font-weight: bold; color: #007acc; }}
        .success {{ color: #28a745; }}
        .error {{ color: #dc3545; }}
        .warning {{ color: #ffc107; }}
        .section {{ margin-bottom: 30px; }}
        .section h2 {{ color: #333; border-bottom: 1px solid #ddd; padding-bottom: 5px; }}
        .error-list {{ background: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px; padding: 15px; }}
        .error-list ul {{ margin: 0; padding-left: 20px; }}
        .error-list li {{ margin-bottom: 5px; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
        .stats-table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        .stats-table th, .stats-table td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        .stats-table th {{ background-color: #f8f9fa; font-weight: bold; }}
        .no-errors {{ background: #d4edda; border: 1px solid #c3e6cb; border-radius: 4px; padding: 15px; color: #155724; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>EQ Test Data Validation Report</h1>
            <p>Generated on {timestamp}</p>
        </div>
        
        <div class="summary">
            <div class="summary-card">
                <h3>Total Files</h3>
                <div class="value">{total_files}</div>
            </div>
            <div class="summary-card">
                <h3>Valid Files</h3>
                <div class="value {valid_class}">{valid_files}</div>
            </div>
            <div class="summary-card">
                <h3>Validation Status</h3>
                <div class="value {status_class}">{status}</div>
            </div>
            <div class="summary-card">
                <h3>Error Count</h3>
                <div class="value {error_class}">{error_count}</div>
            </div>
        </div>
        
        <div class="section">
            <h2>Validation Results</h2>
            {validation_content}
        </div>
        
        <div class="section">
            <h2>Data Statistics</h2>
            <div class="stats-grid">
                {statistics_content}
            </div>
        </div>
    </div>
</body>
</html>
    """
    
    status = "PASS" if result.valid_files == result.total_files else "FAIL"
    status_class = "success" if status == "PASS" else "error"
    valid_class = "success" if result.valid_files == result.total_files else "warning"
    error_class = "success" if len(result.errors) == 0 else "error"
    
    if result.errors:
        validation_content = f"""
        <div class="error-list">
            <h3>Validation Errors ({len(result.errors)})</h3>
            <ul>
                {''.join(f'<li>{error}</li>' for error in result.errors)}
            </ul>
        </div>
        """
    else:
        validation_content = """
        <div class="no-errors">
            <h3>✅ All validation checks passed!</h3>
            <p>All test data files contain the required fields with valid formats and values.</p>
        </div>
        """
    
    stats_sections = []
    for category, data in result.statistics.items():
        if data:
            table_rows = ''.join(f'<tr><td>{key}</td><td>{value}</td></tr>' for key, value in data.items())
            stats_sections.append(f"""
            <div>
                <h3>{category.replace('_', ' ').title()}</h3>
                <table class="stats-table">
                    <thead>
                        <tr><th>Category</th><th>Count</th></tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            </div>
            """)
    
    statistics_content = ''.join(stats_sections)
    
    return html_template.format(
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
        total_files=result.total_files,
        valid_files=result.valid_files,
        status=status,
        error_count=len(result.errors),
        valid_class=valid_class,
        status_class=status_class,
        error_class=error_class,
        validation_content=validation_content,
        statistics_content=statistics_content
    )

def generate_csv_statistics(result: ValidationResult) -> str:
    """Generate CSV statistics report."""
    csv_content = []
    csv_content.append("Category,Subcategory,Count")
    
    for category, data in result.statistics.items():
        for subcategory, count in data.items():
            csv_content.append(f"{category},{subcategory},{count}")
    
    return '\n'.join(csv_content)

def main():
    """Main execution function."""
    print("🔍 Starting EQ Test Data Validation...")
    print("=" * 50)
    
    result = validate_test_data()
    
    print(f"📊 Validation Summary:")
    print(f"   Total files processed: {result.total_files}")
    print(f"   Valid files: {result.valid_files}")
    print(f"   Files with errors: {result.total_files - result.valid_files}")
    print(f"   Total errors: {len(result.errors)}")
    
    if result.errors:
        print(f"\n❌ Validation Errors:")
        for error in result.errors:
            print(f"   • {error}")
    else:
        print(f"\n✅ All validation checks passed!")
    
    output_dir = Path(__file__).parent.parent / 'expected-outputs'
    output_dir.mkdir(exist_ok=True)
    
    html_report = generate_html_report(result)
    html_file = output_dir / 'test_data_validation_report.html'
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_report)
    print(f"\n📄 HTML report generated: {html_file}")
    
    csv_stats = generate_csv_statistics(result)
    csv_file = output_dir / 'test_data_statistics.csv'
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write(csv_stats)
    print(f"📊 CSV statistics generated: {csv_file}")
    
    print(f"\n📈 Data Distribution:")
    for category, data in result.statistics.items():
        if data:
            print(f"   {category.replace('_', ' ').title()}:")
            for key, value in data.items():
                print(f"     • {key}: {value}")
    
    print("=" * 50)
    status = "PASSED" if result.valid_files == result.total_files else "FAILED"
    print(f"🎯 Validation Status: {status}")
    
    return 0 if result.valid_files == result.total_files else 1

if __name__ == "__main__":
    exit(main())
