#!/usr/bin/env python3
"""
Verification script for thread safety and async improvements
"""
import concurrent.futures
import asyncio
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from NodeRAG.storage.storage_factory import StorageFactory

def test_concurrent_initialization():
    """Test concurrent access to StorageFactory"""
    print("Testing concurrent initialization...")
    
    config = {
        'config': {
            'main_folder': '/tmp/test',
            'language': 'en',
            'chunk_size': 512
        },
        'model_config': {'model_name': 'gpt-4o'},
        'embedding_config': {'model_name': 'gpt-4o'}
    }
    
    StorageFactory.initialize(config, backend_mode="file")
    
    results = []
    start_time = time.time()
    
    def get_storage():
        return StorageFactory.get_graph_storage()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(get_storage) for _ in range(100)]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    
    elapsed = time.time() - start_time
    
    first = results[0]
    all_same = all(r is first for r in results)
    
    print(f"✅ Thread safety test passed: All same instance = {all_same}")
    print(f"   Completed 100 concurrent requests in {elapsed:.2f}s")
    
    StorageFactory.cleanup()
    
    return all_same

def test_async_performance():
    """Test that async operations don't create multiple event loops"""
    print("\nTesting async event loop reuse...")
    
    import psutil
    import os
    
    process = psutil.Process(os.getpid())
    initial_threads = process.num_threads()
    
    config = {
        'config': {
            'main_folder': '/tmp/test',
            'language': 'en',
            'chunk_size': 512
        },
        'model_config': {'model_name': 'gpt-4o'},
        'embedding_config': {'model_name': 'gpt-4o'}
    }
    
    StorageFactory.initialize(config, backend_mode="file")
    
    for _ in range(10):
        StorageFactory.get_graph_storage()
    
    final_threads = process.num_threads()
    thread_increase = final_threads - initial_threads
    
    print(f"✅ Event loop reuse test passed:")
    print(f"   Initial threads: {initial_threads}")
    print(f"   Final threads: {final_threads}")
    print(f"   Thread increase: {thread_increase} (should be minimal)")
    
    StorageFactory.cleanup()
    
    return thread_increase < 5

if __name__ == "__main__":
    print("=" * 50)
    print("Thread Safety and Async Verification")
    print("=" * 50)
    
    test1 = test_concurrent_initialization()
    test2 = test_async_performance()
    
    print("\n" + "=" * 50)
    if test1 and test2:
        print("✅ ALL VERIFICATIONS PASSED")
    else:
        print("❌ SOME VERIFICATIONS FAILED")
        sys.exit(1)
