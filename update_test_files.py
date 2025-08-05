"""Update all test files to use correct NodeConfig initialization"""
import os
import re
from pathlib import Path

test_files = [
    'quick_integration_test.py',
    'test_pipeline_metadata_flow.py',
    'verify_entity_metadata.py',
]

incorrect_pattern = re.compile(r'NodeConfig\(\s*\)')

replacement_import = "from NodeRAG.test_utils.config_helper import create_test_nodeconfig"
replacement_usage = "create_test_nodeconfig()"

for test_file in test_files:
    if not Path(test_file).exists():
        print(f"⚠️  Test file not found: {test_file}")
        continue
    
    with open(test_file, 'r') as f:
        content = f.read()
    
    if 'NodeConfig()' in content:
        print(f"\n=== Updating {test_file} ===")
        
        if 'from NodeRAG.test_utils.config_helper import' not in content:
            lines = content.split('\n')
            import_added = False
            
            for i, line in enumerate(lines):
                if line.startswith('from NodeRAG') and not import_added:
                    lines.insert(i + 1, replacement_import)
                    import_added = True
                    break
            
            if not import_added:
                for i, line in enumerate(lines):
                    if "sys.path.append('.')" in line:
                        lines.insert(i + 1, replacement_import)
                        break
            
            content = '\n'.join(lines)
        
        original_count = content.count('NodeConfig()')
        content = content.replace('config = NodeConfig()', f'config = {replacement_usage}')
        content = content.replace('NodeConfig()', replacement_usage)
        
        with open(test_file, 'w') as f:
            f.write(content)
        
        print(f"✅ Updated {original_count} instances of NodeConfig()")
        
        print("Changes made:")
        if 'from NodeRAG.test_utils.config_helper import' in content:
            print("  - Added config helper import")
        print(f"  - Replaced NodeConfig() with {replacement_usage}")
    else:
        print(f"✅ {test_file} - No updates needed")

print("\n=== Update Summary ===")
print("All test files have been updated to use the correct NodeConfig initialization pattern.")
