# NodeConfig Initialization Guide

## Problem Summary
The NodeConfig class requires a configuration dictionary parameter for initialization, but test files were calling `NodeConfig()` without arguments, causing the error:
```
NodeConfig.__new__() missing 1 required positional argument: 'config'
```

## Correct Pattern for Tests

### ❌ INCORRECT (DO NOT USE):
```python
config = NodeConfig()  # Missing required config parameter
```

### ✅ CORRECT USAGE:

#### Option 1: Using Test Helper (Recommended)
```python
from NodeRAG.test_utils.config_helper import create_test_nodeconfig

config = create_test_nodeconfig()
```

#### Option 2: Manual Configuration
```python
from NodeRAG.config import NodeConfig

config_dict = {
    'model_config': {...},
    'embedding_config': {...},
    'config': {
        'main_folder': './test_output',
        # ... other required fields
    },
    'eq_config': {...}
}

config = NodeConfig(config_dict)
```

## Required Config Structure

The NodeConfig expects a nested dictionary with these top-level keys:
- `model_config`: LLM configuration
- `embedding_config`: Embedding model configuration  
- `config`: Core application configuration (must include 'main_folder')
- `eq_config`: EQ-specific metadata and storage configuration

### Critical Required Fields
- `config.main_folder`: Base directory for all outputs (REQUIRED)
- Document pipeline also requires `./test_output/info/document_hash.json` file

## Test Configuration Helper

The `NodeRAG/test_utils/config_helper.py` provides:

1. **`create_test_nodeconfig()`**: Creates properly configured NodeConfig for testing
2. **`load_test_config()`**: Loads configuration from YAML or creates minimal config
3. **`cleanup_test_output()`**: Cleans up test directories after tests
4. **Automatic directory creation**: Creates all required test directories and files

## Production vs Test Configuration

- **Production**: Load from `NodeRAG/config/Node_config.yaml`
- **Testing**: Use `test_utils.config_helper` for isolated test environment

## Files Updated

The following test files were updated to use the correct pattern:
- ✅ quick_integration_test.py - Updated to use create_test_nodeconfig()
- ✅ test_pipeline_metadata_flow.py - Already correct
- ✅ verify_entity_metadata.py - Already correct

## Validation Results

All Phase 3 validation tests now pass:
- ✅ Semantic Unit Metadata Tests
- ✅ Integration Tests  
- ✅ Pipeline Flow Tests
- ✅ Entity Metadata Verification
- ✅ Document Pipeline Verification
- ✅ Quick Integration Test

## Common Errors Fixed

1. **TypeError: NodeConfig.__new__() missing 1 required positional argument: 'config'**
   - ✅ Fixed: Pass config dictionary as parameter

2. **FileNotFoundError: document_hash.json**
   - ✅ Fixed: Config helper creates required files and directories

3. **KeyError: 'document_path'**
   - ✅ Fixed: Document hash file contains proper structure

## Future Reference

Always use the test configuration helper for new tests to ensure:
- Consistent configuration across all tests
- Proper directory structure creation
- Required file initialization
- Easy cleanup after tests
