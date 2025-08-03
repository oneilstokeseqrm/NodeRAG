#!/usr/bin/env python3
"""Benchmark metadata overhead on component operations"""

import time
import statistics
import sys
from typing import List
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from NodeRAG.standards.eq_metadata import EQMetadata
from NodeRAG.src.component import Entity
from datetime import datetime, timezone

def create_metadata():
    """Create test metadata"""
    return EQMetadata(
        tenant_id="tenant_12345678-1234-4567-8901-123456789012",
        interaction_id="int_12345678-1234-4567-8901-123456789012",
        interaction_type="email",
        text="Performance test",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="outlook"
    )

def benchmark_entity_creation(iterations: int = 10000):
    """Benchmark entity creation with and without metadata"""
    
    times_without = []
    for i in range(iterations):
        start = time.perf_counter()
        entity = Entity(f"Test Entity {i}")
        _ = entity.hash_id
        end = time.perf_counter()
        times_without.append(end - start)
    
    metadata = create_metadata()
    times_with = []
    for i in range(iterations):
        start = time.perf_counter()
        entity = Entity(f"Test Entity {i}", metadata=metadata)
        _ = entity.hash_id
        end = time.perf_counter()
        times_with.append(end - start)
    
    avg_without = statistics.mean(times_without) * 1000  # Convert to ms
    avg_with = statistics.mean(times_with) * 1000
    overhead = ((avg_with - avg_without) / avg_without) * 100
    
    print(f"Entity Creation Benchmark ({iterations} iterations):")
    print(f"  Without metadata: {avg_without:.3f} ms average")
    print(f"  With metadata:    {avg_with:.3f} ms average")
    print(f"  Overhead:         {overhead:.1f}%")
    
    return {
        "iterations": iterations,
        "avg_without_ms": avg_without,
        "avg_with_ms": avg_with,
        "overhead_percent": overhead
    }

if __name__ == "__main__":
    results = benchmark_entity_creation()
    
    import json
    with open("metadata_performance_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print("\nResults saved to metadata_performance_results.json")
