# Multi-Tenant Data Isolation Guide

## Overview
NodeRAG now supports complete multi-tenant data isolation, enabling secure SaaS deployments where multiple customers share the same infrastructure while maintaining complete data separation.

## Architecture

### Tenant Context Management
The `TenantContext` class provides thread-local tenant isolation:
- Each request/thread maintains its own tenant context
- Tenant ID validation and format enforcement
- Automatic namespace generation for storage operations

### Storage Isolation
All storage operations are tenant-aware:
- **Neo4j**: Tenant-specific subgraphs with metadata isolation
- **Pinecone**: Namespace-based vector isolation
- **File Storage**: Directory-based tenant separation

## Usage Examples

### Basic Tenant Operations
```python
from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.src.pipeline.graph_pipeline_tenant import TenantAwareGraphPipeline

# Set tenant context
TenantContext.set_current_tenant('customer_123', {
    'org_name': 'Acme Corp',
    'tier': 'premium'
})

# All subsequent operations are tenant-scoped
pipeline = TenantAwareGraphPipeline(config)
pipeline.build()  # Builds graph for customer_123
```

### Using Tenant Scope Context Manager
```python
# Temporary tenant scope
with TenantContext.tenant_scope('customer_456'):
    # All operations here are scoped to customer_456
    pipeline = TenantAwareGraphPipeline(config)
    pipeline.save_graph()
# Automatically reverts to previous tenant context
```

### Concurrent Multi-Tenant Processing
```python
from concurrent.futures import ThreadPoolExecutor

def process_tenant_data(tenant_id, data):
    with TenantContext.tenant_scope(tenant_id):
        pipeline = TenantAwareGraphPipeline(config)
        return pipeline.process(data)

# Process multiple tenants concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    for tenant_id in tenant_list:
        future = executor.submit(process_tenant_data, tenant_id, tenant_data[tenant_id])
        futures.append(future)
```

## API Integration

### Flask Example
```python
from flask import Flask, request, g
from NodeRAG.tenant.tenant_context import TenantContext

app = Flask(__name__)

@app.before_request
def set_tenant_context():
    # Extract tenant from header, JWT, or subdomain
    tenant_id = request.headers.get('X-Tenant-ID')
    if tenant_id:
        TenantContext.set_current_tenant(tenant_id)
        g.tenant_id = tenant_id

@app.after_request
def clear_tenant_context(response):
    TenantContext.clear_current_tenant()
    return response

@app.route('/api/graph/build', methods=['POST'])
def build_graph():
    pipeline = TenantAwareGraphPipeline(config)
    result = pipeline.build(request.json)
    return {'status': 'success', 'tenant': g.tenant_id}
```

### FastAPI Example
```python
from fastapi import FastAPI, Header, Depends
from NodeRAG.tenant.tenant_context import TenantContext

app = FastAPI()

async def get_tenant_id(x_tenant_id: str = Header(None)):
    if x_tenant_id:
        TenantContext.set_current_tenant(x_tenant_id)
    return x_tenant_id

@app.post("/api/graph/build")
async def build_graph(data: dict, tenant_id: str = Depends(get_tenant_id)):
    try:
        pipeline = TenantAwareGraphPipeline(config)
        result = await pipeline.build_async(data)
        return {"status": "success", "tenant": tenant_id}
    finally:
        TenantContext.clear_current_tenant()
```

## Security Considerations

### Tenant ID Validation
- Only alphanumeric characters, hyphens, and underscores allowed
- Automatic validation on context setting
- Format: `^[a-zA-Z0-9_-]+$`

### Access Control
- Tenants can only access their own data
- Cross-tenant access attempts are logged and blocked
- Default tenant has admin access (use carefully)

### Resource Isolation
- Each tenant's data is completely isolated
- No shared storage locations between tenants
- Separate namespaces in cloud storage

## Configuration

### Environment Variables
```bash
# Enable multi-tenant mode
NODERAG_MULTI_TENANT_ENABLED=true

# Default tenant for backwards compatibility
NODERAG_DEFAULT_TENANT=default

# Tenant validation strictness
NODERAG_TENANT_VALIDATION_STRICT=true
```

### Storage Configuration
```python
config = {
    'multi_tenant': {
        'enabled': True,
        'isolation_level': 'strict',  # strict, moderate, relaxed
        'namespace_prefix': 'noderag',
        'tenant_id_header': 'X-Tenant-ID'
    }
}
```

## Migration from Single-Tenant

### Step 1: Update Pipeline Usage
```python
# Before (single-tenant)
from NodeRAG.src.pipeline.graph_pipeline_v2 import Graph_pipeline
pipeline = Graph_pipeline(config)

# After (multi-tenant)
from NodeRAG.src.pipeline.graph_pipeline_tenant import TenantAwareGraphPipeline
pipeline = TenantAwareGraphPipeline(config, tenant_id='default')
```

### Step 2: Add Tenant Context
```python
# Wrap existing code with tenant context
with TenantContext.tenant_scope('default'):
    # Existing pipeline code works unchanged
    pipeline.build()
    pipeline.query(query)
```

### Step 3: Migrate Data
```python
# Script to migrate existing data to tenant structure
def migrate_to_tenant(old_data_path, tenant_id):
    with TenantContext.tenant_scope(tenant_id):
        # Load old data
        old_pipeline = Graph_pipeline(config)
        old_graph = old_pipeline.load_graph()
        
        # Save with tenant isolation
        new_pipeline = TenantAwareGraphPipeline(config)
        new_pipeline.G = old_graph
        new_pipeline.save_graph()
```

## Monitoring & Debugging

### Tenant Activity Logging
```python
import logging
logging.getLogger('NodeRAG.tenant').setLevel(logging.DEBUG)

# Logs will show:
# - Tenant context changes
# - Access validation attempts
# - Cross-tenant access violations
```

### Metrics Collection
```python
from NodeRAG.tenant.tenant_context import TenantContext

# Get tenant metrics
all_tenants = TenantContext.get_all_registered_tenants()
for tenant_id in all_tenants:
    info = TenantContext._global_tenant_registry[tenant_id]
    print(f"Tenant: {tenant_id}")
    print(f"  Created: {info.created_at}")
    print(f"  Last Access: {info.last_accessed}")
    print(f"  Access Count: {info.access_count}")
```

## Best Practices

1. **Always Set Tenant Context**: Never operate without explicit tenant context
2. **Use Context Manager**: Prefer `tenant_scope()` for automatic cleanup
3. **Validate Early**: Set tenant context at request entry point
4. **Log Violations**: Monitor and alert on cross-tenant access attempts
5. **Test Isolation**: Regular testing of tenant isolation boundaries

## Troubleshooting

### Common Issues

**No Tenant Context Error**
```python
# Error: RuntimeError: No tenant context set
# Solution: Set context before operations
TenantContext.set_current_tenant('tenant_id')
```

**Cross-Tenant Access Denied**
```python
# Error: PermissionError: Tenant X cannot access Y resources
# Solution: Ensure correct tenant context
with TenantContext.tenant_scope(correct_tenant_id):
    # Operations here
```

**Thread Context Lost**
```python
# Issue: Tenant context not preserved in new thread
# Solution: Pass tenant_id explicitly to thread
def worker(tenant_id):
    with TenantContext.tenant_scope(tenant_id):
        # Thread operations
```

## Support

For multi-tenant issues:
1. Check tenant context is set correctly
2. Verify storage isolation configuration
3. Review access logs for violations
4. Contact NodeRAG team with tenant ID and session ID
