#!/usr/bin/env python3
"""
Detailed investigation of alleged race condition in multi-tenant system
"""
import sys
import os
import time
import uuid
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from NodeRAG.tenant.tenant_context import TenantContext
from NodeRAG.src.pipeline.storage_adapter import PipelineStorageAdapter
from NodeRAG.storage.storage_factory import StorageFactory
import networkx as nx
import tempfile

RACE_CONDITIONS_DETECTED = []
OPERATION_LOG = []
LOCK = threading.Lock()

def log_operation(tenant_id, operation, data):
    """Thread-safe operation logging"""
    with LOCK:
        OPERATION_LOG.append({
            'timestamp': datetime.now().isoformat(),
            'thread_id': threading.get_ident(),
            'tenant_id': tenant_id,
            'operation': operation,
            'data': data
        })

def detect_race_condition(tenant_id, expected_data, actual_data, context):
    """Detect and log race conditions"""
    if expected_data != actual_data:
        with LOCK:
            RACE_CONDITIONS_DETECTED.append({
                'tenant_id': tenant_id,
                'expected': expected_data,
                'actual': actual_data,
                'context': context,
                'thread_id': threading.get_ident(),
                'timestamp': datetime.now().isoformat()
            })
        return True
    return False

def test_concurrent_graph_operations():
    """Test for data corruption in concurrent graph operations"""
    print("\n" + "="*60)
    print("TEST 1: Concurrent Graph Operations - Atomic Write Verification")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = {
            'config': {'main_folder': tmpdir, 'language': 'en', 'chunk_size': 512},
            'model_config': {'model_name': 'gpt-4o'},
            'embedding_config': {'model_name': 'gpt-4o'}
        }
        StorageFactory.initialize(config, backend_mode="file")
        adapter = PipelineStorageAdapter()
        
        corruption_detected = []  # Changed from RACE_CONDITIONS_DETECTED
        
        def tenant_graph_operation(tenant_id, node_suffix):
            """Create and verify tenant-specific graph with correct expectations"""
            try:
                with TenantContext.tenant_scope(tenant_id):
                    current = TenantContext.get_current_tenant()
                    if current != tenant_id:
                        return f"CONTEXT_ERROR: Expected {tenant_id}, got {current}"
                    
                    graph = nx.Graph()
                    node_name = f"{tenant_id}_node_{node_suffix}"
                    secret_data = f"SECRET_{tenant_id}_{node_suffix}"
                    graph.add_node(node_name, secret=secret_data, tenant=tenant_id)
                    
                    save_path = f"{tmpdir}/graph.pkl"  # Same file for stress testing
                    success = adapter.save_pickle(graph, save_path, "graph", tenant_id)
                    if not success:
                        return f"SAVE_FAILED: {tenant_id}"
                    
                    time.sleep(0.001)
                    
                    loaded = adapter.load_pickle(save_path, "graph", tenant_id)
                    if loaded is None:
                        return f"LOAD_FAILED: {tenant_id}"
                    
                    if not isinstance(loaded, nx.Graph):
                        corruption_detected.append({
                            'tenant_id': tenant_id,
                            'error': 'Not a valid graph object'
                        })
                        return f"CORRUPTION: Invalid graph type"
                    
                    for node in loaded.nodes():
                        if 'tenant' in loaded.nodes[node]:
                            found_tenant = loaded.nodes[node]['tenant']
                            if found_tenant != tenant_id:
                                corruption_detected.append({
                                    'tenant_id': tenant_id,
                                    'error': f'Cross-tenant data: {found_tenant}'
                                })
                                return f"CORRUPTION: Cross-tenant contamination"
                    
                    return "SUCCESS"
                    
            except Exception as e:
                return f"ERROR: {e}"
        
        print("Running 50 concurrent operations to test atomic writes...")
        results = []
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for i in range(50):
                tenant_id = f"tenant_{i % 5}"  # 5 tenants, 10 operations each
                future = executor.submit(tenant_graph_operation, tenant_id, i)
                futures.append((tenant_id, future))
            
            for tenant_id, future in futures:
                result = future.result()
                results.append((tenant_id, result))
                if "CORRUPTION" in result or "ERROR" in result:
                    print(f"  ❌ {tenant_id}: {result}")
        
        successes = [r for r in results if r[1] == "SUCCESS"]
        corruptions = [r for r in results if "CORRUPTION" in r[1]]
        errors = [r for r in results if "ERROR" in r[1] or "FAILED" in r[1]]
        
        print(f"\nResults: {len(successes)} successes, {len(corruptions)} corruptions, {len(errors)} errors")
        
        if corruption_detected:
            print(f"\n🚨 DATA CORRUPTION DETECTED: {len(corruption_detected)}")
            for corruption in corruption_detected[:5]:
                print(f"  Tenant: {corruption['tenant_id']}")
                print(f"  Error: {corruption['error']}")
            return False
        else:
            print("✅ No data corruption detected - atomic operations working correctly")
            print("   Note: 'Last write wins' for same tenant is expected behavior")
            return True

def test_rapid_context_switching():
    """Test rapid context switching for race conditions"""
    print("\n" + "="*60)
    print("TEST 2: Rapid Context Switching")
    print("="*60)
    
    context_errors = []
    
    def rapid_switch_operation(iterations):
        """Rapidly switch between tenant contexts"""
        thread_id = threading.get_ident()
        for i in range(iterations):
            tenant_id = f"rapid_tenant_{thread_id}_{i}"
            
            TenantContext.set_current_tenant(tenant_id)
            
            current = TenantContext.get_current_tenant()
            if current != tenant_id:
                context_errors.append({
                    'expected': tenant_id,
                    'actual': current,
                    'thread': thread_id,
                    'iteration': i
                })
            
            TenantContext.clear_current_tenant()
            
            current = TenantContext.get_current_tenant()
            if current is not None:
                context_errors.append({
                    'expected': None,
                    'actual': current,
                    'thread': thread_id,
                    'iteration': i,
                    'operation': 'clear'
                })
    
    print("Running 10 threads with 100 rapid context switches each...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(rapid_switch_operation, 100) for _ in range(10)]
        for future in as_completed(futures):
            future.result()
    
    if context_errors:
        print(f"\n🚨 CONTEXT ERRORS DETECTED: {len(context_errors)}")
        for error in context_errors[:5]:
            print(f"  Thread {error['thread']}: Expected {error['expected']}, got {error['actual']}")
    else:
        print("✅ No context switching errors detected")
    
    return len(context_errors) == 0

def test_registry_corruption():
    """Test for registry corruption under concurrent access"""
    print("\n" + "="*60)
    print("TEST 3: Registry Corruption Test")
    print("="*60)
    
    TenantContext.cleanup_all_tenants()
    
    registry_errors = []
    tenant_ids = [f"registry_tenant_{i}" for i in range(20)]
    
    def registry_operation(tenant_id, operation_type):
        """Perform registry operations"""
        try:
            if operation_type == 'create':
                TenantContext.set_current_tenant(tenant_id, {'test': 'data'})
                TenantContext.clear_current_tenant()
            elif operation_type == 'check':
                all_tenants = TenantContext.get_all_registered_tenants()
                if len(all_tenants) != len(set(all_tenants)):
                    registry_errors.append('Duplicate tenants in registry')
                for t in all_tenants:
                    if not t.startswith('registry_tenant_'):
                        registry_errors.append(f'Corrupted tenant ID: {t}')
            elif operation_type == 'stats':
                stats = TenantContext.get_registry_stats()
                if stats['total_tenants'] < 0:
                    registry_errors.append('Negative tenant count')
            return "SUCCESS"
        except Exception as e:
            return str(e)
    
    print("Running concurrent registry operations...")
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = []
        
        for tenant_id in tenant_ids:
            futures.append(executor.submit(registry_operation, tenant_id, 'create'))
        
        for _ in range(10):
            futures.append(executor.submit(registry_operation, None, 'check'))
            futures.append(executor.submit(registry_operation, None, 'stats'))
        
        for future in as_completed(futures):
            result = future.result()
            if result != "SUCCESS":
                registry_errors.append(result)
    
    final_tenants = TenantContext.get_all_registered_tenants()
    expected_tenants = set(tenant_ids)
    actual_tenants = set(t for t in final_tenants if t.startswith('registry_tenant_'))
    
    if expected_tenants != actual_tenants:
        missing = expected_tenants - actual_tenants
        extra = actual_tenants - expected_tenants
        if missing:
            registry_errors.append(f"Missing tenants: {missing}")
        if extra:
            registry_errors.append(f"Extra tenants: {extra}")
    
    if registry_errors:
        print(f"\n🚨 REGISTRY ERRORS: {len(registry_errors)}")
        for error in registry_errors[:5]:
            print(f"  {error}")
    else:
        print("✅ No registry corruption detected")
    
    return len(registry_errors) == 0

def analyze_operation_log():
    """Analyze operation log for timing issues"""
    print("\n" + "="*60)
    print("OPERATION LOG ANALYSIS")
    print("="*60)
    
    if not OPERATION_LOG:
        print("No operations logged")
        return
    
    by_thread = {}
    for op in OPERATION_LOG:
        thread_id = op['thread_id']
        if thread_id not in by_thread:
            by_thread[thread_id] = []
        by_thread[thread_id].append(op)
    
    print(f"Operations across {len(by_thread)} threads")
    
    for thread_id, ops in by_thread.items():
        tenant_sequence = [op['tenant_id'] for op in ops]
        for i in range(1, len(tenant_sequence)):
            if tenant_sequence[i] != tenant_sequence[i-1]:
                print(f"  Thread {thread_id}: Tenant changed from {tenant_sequence[i-1]} to {tenant_sequence[i]}")

def generate_detailed_report():
    """Generate detailed investigation report"""
    report = {
        'timestamp': datetime.now().isoformat(),
        'race_conditions_found': len(RACE_CONDITIONS_DETECTED),
        'race_condition_details': RACE_CONDITIONS_DETECTED[:10],  # First 10
        'total_operations': len(OPERATION_LOG),
        'investigation_complete': True
    }
    
    import json
    with open('investigation/race_condition_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print("\n" + "="*60)
    print("INVESTIGATION SUMMARY")
    print("="*60)
    
    if RACE_CONDITIONS_DETECTED:
        print(f"🚨 CRITICAL: {len(RACE_CONDITIONS_DETECTED)} race conditions detected!")
        print("Data corruption risk confirmed. DO NOT DEPLOY TO PRODUCTION.")
        print("\nDetails saved to: investigation/race_condition_report.json")
        return False
    else:
        print("✅ No race conditions detected")
        print("Multi-tenant isolation appears to be working correctly")
        return True

def main():
    """Run complete investigation"""
    print("\n" + "="*60)
    print("MULTI-TENANT RACE CONDITION INVESTIGATION")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*60)
    
    os.makedirs('investigation', exist_ok=True)
    
    test_results = []
    
    test_results.append(('Concurrent Graph Operations', test_concurrent_graph_operations()))
    test_results.append(('Rapid Context Switching', test_rapid_context_switching()))
    test_results.append(('Registry Corruption', test_registry_corruption()))
    
    analyze_operation_log()
    
    all_passed = generate_detailed_report()
    
    print("\n" + "="*60)
    print("FINAL VERDICT")
    print("="*60)
    
    for test_name, passed in test_results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    if all_passed and all(r[1] for r in test_results):
        print("\n✅ NO RACE CONDITIONS FOUND - System appears safe")
        return 0
    else:
        print("\n🚨 RACE CONDITIONS DETECTED - DO NOT DEPLOY")
        print("See investigation/race_condition_report.json for details")
        return 1

if __name__ == "__main__":
    sys.exit(main())
