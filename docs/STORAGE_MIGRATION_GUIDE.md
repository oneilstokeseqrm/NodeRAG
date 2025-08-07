# Graph Pipeline Storage Migration Guide

## Overview
This guide documents the migration of Graph_pipeline from direct file I/O to StorageFactory-mediated storage operations, enabling seamless switching between file storage and cloud storage (Neo4j + Pinecone).

## Migration Status
- ✅ Storage audit completed - identified all storage operations across pipeline files
- ✅ Storage adapter implemented - routes operations to appropriate backend
- ✅ Graph_pipeline_v2 created - extends original with StorageFactory support
- ✅ Backward compatibility maintained - existing file workflows unchanged
- ✅ Tests created for both file and cloud storage modes

## Architecture Changes

### Before Migration
```python
# Direct file operations
storage(self.G).save_pickle(self.config.graph_path)
storage(entities).save_parquet(self.config.entities_path)
```

### After Migration
```python
# StorageFactory-mediated operations
from .storage_adapter import storage_factory_wrapper
storage_factory_wrapper(self.G).save_pickle(self.config.graph_path, component_type='graph')
storage_factory_wrapper(entities).save_parquet(self.config.entities_path, component_type='data')
```

## Key Components

### 1. PipelineStorageAdapter
- **Location**: `NodeRAG/src/pipeline/storage_adapter.py`
- **Purpose**: Routes storage operations to appropriate backend
- **Features**: 
  - Automatic backend detection (file/cloud)
  - Component-type routing (graph → Neo4j, embeddings → Pinecone)
  - Fallback to file storage for compatibility

### 2. Graph_pipeline_v2
- **Location**: `NodeRAG/src/pipeline/graph_pipeline_v2.py`
- **Purpose**: Extended Graph_pipeline with StorageFactory integration
- **Features**:
  - Extends original Graph_pipeline (no breaking changes)
  - Overrides storage methods to use PipelineStorageAdapter
  - Maintains identical API for seamless migration

### 3. StorageFactoryWrapper
- **Purpose**: Maintains existing storage() API while using StorageFactory
- **Features**:
  - Drop-in replacement for storage() calls
  - Adds component_type parameter for proper routing
  - Graceful fallback to original storage if StorageFactory unavailable

## Usage Examples

### File Storage Mode (Default)
```python
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.src.pipeline.graph_pipeline_v2 import Graph_pipeline
from NodeRAG.config.Node_config import NodeConfig

# Initialize with file storage
config = {
    'config': {'main_folder': '/path/to/data', 'language': 'en', 'chunk_size': 512},
    'model_config': {'model_name': 'gpt-4o'},
    'embedding_config': {'model_name': 'gpt-4o'}
}

StorageFactory.initialize(config, backend_mode="file")

# Use pipeline normally - saves to files
node_config = NodeConfig(config)
pipeline = Graph_pipeline(node_config)
pipeline.save_graph()  # Saves to pickle file
```

### Cloud Storage Mode
```python
# Initialize with cloud storage
config = {
    'config': {'main_folder': '/path/to/data', 'language': 'en', 'chunk_size': 512},
    'model_config': {'model_name': 'gpt-4o'},
    'embedding_config': {'model_name': 'gpt-4o'},
    'eq_config': {
        'storage': {
            'neo4j_uri': 'neo4j://localhost:7687',
            'neo4j_user': 'neo4j',
            'neo4j_password': 'password',
            'pinecone_api_key': 'your-api-key',
            'pinecone_index': 'noderag-index'
        }
    }
}

StorageFactory.initialize(config, backend_mode="cloud")

# Same pipeline code works with cloud storage
pipeline = Graph_pipeline(node_config)
pipeline.save_graph()  # Saves to Neo4j
```

### Component Type Routing
```python
from NodeRAG.src.pipeline.storage_adapter import storage_factory_wrapper

# Graph data → Neo4j in cloud mode, pickle in file mode
storage_factory_wrapper(graph).save_pickle(path, component_type='graph')

# Embeddings → Pinecone in cloud mode, parquet in file mode  
storage_factory_wrapper(embeddings).save_parquet(path, component_type='embeddings')

# Other data → Always uses file storage
storage_factory_wrapper(entities).save_parquet(path, component_type='data')
```

## Migration Checklist

### For Existing Code
- [ ] Update imports to use `graph_pipeline_v2` instead of `graph_pipeline`
- [ ] Initialize StorageFactory before creating pipeline instances
- [ ] Test with both file and cloud storage modes
- [ ] Verify data persistence and retrieval works correctly
- [ ] Update production configurations with cloud credentials

### For New Development
- [ ] Use `storage_factory_wrapper()` instead of `storage()` for new code
- [ ] Always specify `component_type` parameter for proper routing
- [ ] Test storage operations with both backend modes
- [ ] Include proper error handling for storage failures

## Environment Variables

Required for cloud storage mode:
```bash
# Neo4j Configuration
Neo4j_Credentials_NEO4J_URI=neo4j://localhost:7687
Neo4j_Credentials_NEO4J_USERNAME=neo4j
Neo4j_Credentials_NEO4J_PASSWORD=your-password

# Pinecone Configuration  
pinecone_API_key=your-pinecone-api-key
Pinecone_Index_Name=noderag-index
```

## Storage Operation Routing

| Component Type | File Mode | Cloud Mode |
|---------------|-----------|------------|
| `graph` | Pickle files | Neo4j database |
| `embeddings` | Parquet files | Pinecone vectors |
| `data` | Parquet files | Parquet files |
| `json` | JSON files | JSON files |

## Performance Considerations

### File Storage
- Direct file I/O with minimal overhead
- Fast for small to medium datasets
- Limited by disk I/O performance

### Cloud Storage
- Network latency for database operations
- Better scalability for large datasets
- Automatic backup and replication
- Multi-tenant isolation capabilities

### Optimization Tips
- Use `lazy_init=True` for faster StorageFactory startup
- Enable connection warmup for production deployments
- Consider caching frequently accessed data
- Monitor storage operation performance

## Troubleshooting

### Common Issues

**StorageFactory not initialized**
```python
# Error: StorageFactory not initialized
# Solution: Initialize before use
StorageFactory.initialize(config, backend_mode="file")
```

**Missing cloud credentials**
```python
# Error: Neo4j/Pinecone connection failed
# Solution: Check environment variables
import os
print(os.getenv('Neo4j_Credentials_NEO4J_URI'))
print(os.getenv('pinecone_API_key'))
```

**Component type routing issues**
```python
# Error: Data saved to wrong backend
# Solution: Specify correct component_type
storage_factory_wrapper(data).save_parquet(path, component_type='embeddings')  # → Pinecone
storage_factory_wrapper(data).save_parquet(path, component_type='data')       # → File
```

### Debug Mode
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed storage operation logging
adapter = PipelineStorageAdapter()
# Check logs for routing decisions and backend operations
```

## Rollback Plan

If issues arise, revert to original Graph_pipeline:
```python
# Original (fallback)
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline

# New (with StorageFactory)  
from NodeRAG.src.pipeline.graph_pipeline_v2 import Graph_pipeline
```

The original files remain unchanged, ensuring safe rollback capability.

## Testing

### Run Migration Tests
```bash
# Test file storage compatibility
pytest tests/test_pipeline_migration.py::TestPipelineMigration::test_file_storage_compatibility -v

# Test cloud storage (requires credentials)
pytest tests/test_pipeline_migration.py::TestPipelineMigration::test_cloud_storage_mode -v

# Run full test suite
pytest tests/test_pipeline_migration.py -v
```

### Manual Testing
```bash
# Run storage audit
python analysis/pipeline_storage_audit.py

# Test file mode
STORAGE_MODE=file python examples/test_pipeline_file.py

# Test cloud mode  
STORAGE_MODE=cloud python examples/test_pipeline_cloud.py
```

## Support

For issues or questions about the migration:

1. **Check StorageFactory initialization status**
   ```python
   status = StorageFactory.get_initialization_status()
   print(status)
   ```

2. **Verify credentials for cloud storage**
   ```bash
   env | grep -E "(Neo4j|pinecone)"
   ```

3. **Review logs for storage adapter operations**
   ```python
   import logging
   logging.getLogger('NodeRAG.src.pipeline.storage_adapter').setLevel(logging.DEBUG)
   ```

4. **Test with minimal example**
   ```python
   from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
   adapter = PipelineStorageAdapter()
   print(f"Backend mode: {adapter.backend_mode}")
   ```

## Next Steps

After successful migration:
- Monitor storage operation performance
- Implement additional cloud storage optimizations
- Consider migrating other pipeline components
- Update documentation and training materials
- Plan for production deployment with cloud storage

---

**Migration completed successfully!** 🎉

All storage operations now route through StorageFactory, enabling seamless switching between file and cloud storage modes while maintaining full backward compatibility.
