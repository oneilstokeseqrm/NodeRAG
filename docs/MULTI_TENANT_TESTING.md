# Multi-Tenant Testing Guide

## Automated Testing

All multi-tenant tests now run automatically in CI/CD on every push and PR that affects tenant-related code.

### Test Suites

1. **Multi-Tenant Isolation Tests** (`test_multi_tenant_isolation.py`)
   - Tests data isolation between tenants
   - Validates context management
   - Verifies namespace isolation

2. **Resource Limit Tests** (`test_tenant_resource_limits.py`)
   - Tests maximum tenant limits
   - Validates cleanup of inactive tenants
   - Verifies memory management

3. **Concurrent Operations** (`test_concurrent_tenants.py`)
   - Tests thread safety
   - Validates isolation under load

### Running Tests Locally

```bash
# Set Python path
export PYTHONPATH=/path/to/NodeRAG

# Run all multi-tenant tests
pytest tests/test_multi_tenant_*.py -v

# Run with resource limits
NODERAG_MAX_ACTIVE_TENANTS=10 pytest tests/test_tenant_resource_limits.py -v
```

### Environment Variables

Configure tenant limits via environment:

- `NODERAG_MAX_ACTIVE_TENANTS`: Maximum concurrent tenants (default: 1000)
- `NODERAG_MAX_REGISTRY_SIZE`: Maximum registry entries (default: 5000)
- `NODERAG_TENANT_TTL_HOURS`: Hours before inactive tenant cleanup (default: 24)
- `NODERAG_ENFORCE_TENANT_LIMITS`: Enable/disable limits (default: true)

### Memory Leak Detection

The CI runs a memory leak check that:
1. Creates 100 tenants
2. Forces cleanup
3. Verifies registry size is reasonable

### Performance Benchmarks

Expected performance:
- Tenant context set: < 1ms
- Concurrent operations (10 threads): < 100ms
- Registry cleanup (1000 tenants): < 10ms
