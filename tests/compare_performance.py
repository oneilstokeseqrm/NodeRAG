#!/usr/bin/env python3
"""
Compare performance before and after optimizations
"""
import os
import time
import statistics
from typing import Dict, List
import json

from NodeRAG.storage.storage_factory import StorageFactory


def benchmark_initialization(config: Dict, runs: int = 5) -> Dict[str, List[float]]:
    """Benchmark different initialization strategies"""
    results = {
        'eager_init': [],
        'lazy_init': [],
        'lazy_with_preload': [],
        'with_warmup': []
    }
    
    for _ in range(runs):
        # Eager initialization
        StorageFactory.cleanup()
        start = time.time()
        StorageFactory.initialize(config, backend_mode="cloud", lazy_init=False)
        neo4j = StorageFactory.get_graph_storage()
        pinecone = StorageFactory.get_embedding_storage()
        results['eager_init'].append(time.time() - start)
        
        # Lazy initialization
        StorageFactory.cleanup()
        start = time.time()
        StorageFactory.initialize(config, backend_mode="cloud", lazy_init=True)
        init_time = time.time() - start
        neo4j = StorageFactory.get_graph_storage()
        pinecone = StorageFactory.get_embedding_storage()
        results['lazy_init'].append(time.time() - start)
        
        # Lazy with preload
        StorageFactory.cleanup()
        start = time.time()
        StorageFactory.initialize(config, backend_mode="cloud", lazy_init=True)
        StorageFactory.preload_adapters()
        results['lazy_with_preload'].append(time.time() - start)
        
        # With connection warmup
        StorageFactory.cleanup()
        start = time.time()
        StorageFactory.initialize(config, backend_mode="cloud", warmup_connections=True)
        results['with_warmup'].append(time.time() - start)
    
    return results


def benchmark_operations(config: Dict, operations: int = 100) -> Dict[str, float]:
    """Benchmark operation performance"""
    StorageFactory.initialize(config, backend_mode="cloud", warmup_connections=True)
    neo4j = StorageFactory.get_graph_storage()
    
    results = {}
    
    # Benchmark health checks without caching
    times = []
    for _ in range(operations):
        start = time.time()
        health = neo4j.health_check()
        times.append(time.time() - start)
    results['health_check_uncached'] = statistics.mean(times)
    
    # Benchmark health checks with caching
    times = []
    for _ in range(operations):
        start = time.time()
        health = StorageFactory.get_cached_health_check()
        times.append(time.time() - start)
    results['health_check_cached'] = statistics.mean(times)
    
    # Benchmark singleton access
    times = []
    for _ in range(operations):
        start = time.time()
        neo4j = StorageFactory.get_graph_storage()
        times.append(time.time() - start)
    results['get_adapter'] = statistics.mean(times)
    
    return results


def generate_performance_report(init_results: Dict, op_results: Dict) -> str:
    """Generate performance comparison report"""
    
    report = """
PERFORMANCE OPTIMIZATION REPORT
===============================

INITIALIZATION PERFORMANCE (average of 5 runs):
------------------------------------------------
"""
    
    for strategy, times in init_results.items():
        avg = statistics.mean(times)
        std = statistics.stdev(times) if len(times) > 1 else 0
        report += f"{strategy:20s}: {avg:.3f}s (±{std:.3f}s)\n"
    
    # Calculate improvements
    eager_avg = statistics.mean(init_results['eager_init'])
    lazy_avg = statistics.mean(init_results['lazy_init'])
    improvement = ((eager_avg - lazy_avg) / eager_avg) * 100
    
    report += f"\nLazy init improvement: {improvement:.1f}% faster\n"
    
    report += """
OPERATION PERFORMANCE (average of 100 operations):
--------------------------------------------------
"""
    
    for operation, avg_time in op_results.items():
        report += f"{operation:20s}: {avg_time*1000:.2f}ms\n"
    
    # Calculate cache improvement
    if 'health_check_uncached' in op_results and 'health_check_cached' in op_results:
        uncached = op_results['health_check_uncached']
        cached = op_results['health_check_cached']
        cache_improvement = ((uncached - cached) / uncached) * 100
        report += f"\nCache improvement: {cache_improvement:.1f}% faster\n"
    
    report += """
RECOMMENDATIONS:
---------------
1. Use lazy_init=True for faster application startup
2. Call preload_adapters() during application initialization
3. Use get_cached_health_check() for health monitoring
4. Enable warmup_connections=True for production deployments
"""
    
    return report


def main():
    """Run performance comparison"""
    config = {
        'config': {
            'main_folder': '/tmp/perf_test',
            'language': 'en',
            'chunk_size': 512
        },
        'model_config': {'model_name': 'gpt-4o'},
        'embedding_config': {'model_name': 'gpt-4o'},
        'eq_config': {
            'storage': {
                'neo4j_uri': os.getenv('Neo4j_Credentials_NEO4J_URI'),
                'neo4j_user': os.getenv('Neo4j_Credentials_NEO4J_USERNAME', 'neo4j'),
                'neo4j_password': os.getenv('Neo4j_Credentials_NEO4J_PASSWORD'),
                'pinecone_api_key': os.getenv('pinecone_API_key'),
                'pinecone_index': os.getenv('Pinecone_Index_Name', 'noderag')
            }
        }
    }
    
    print("Running initialization benchmarks...")
    init_results = benchmark_initialization(config, runs=5)
    
    print("Running operation benchmarks...")
    op_results = benchmark_operations(config, operations=100)
    
    # Generate report
    report = generate_performance_report(init_results, op_results)
    
    # Save results
    with open('performance_results.json', 'w') as f:
        json.dump({
            'initialization': init_results,
            'operations': op_results
        }, f, indent=2)
    
    with open('performance_report.txt', 'w') as f:
        f.write(report)
    
    print(report)
    print("\nResults saved to performance_results.json and performance_report.txt")
    
    StorageFactory.cleanup()


if __name__ == "__main__":
    main()
