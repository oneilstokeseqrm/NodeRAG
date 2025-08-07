"""
Extended 10-minute resource leak test for Task 4.0.1d
"""
import os
import time
import gc
import psutil
import threading
import uuid
from datetime import datetime, timedelta, timezone

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.standards.eq_metadata import EQMetadata


def test_extended_resource_leak():
    """Run operations for 10 minutes to detect leaks"""
    
    print("\n=== EXTENDED RESOURCE LEAK TEST (10 minutes) ===")
    
    import os
    os.makedirs('/tmp/leak_test', exist_ok=True)
    config = {
        'config': {
            'main_folder': '/tmp/leak_test',
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
    
    StorageFactory.initialize(config, backend_mode="cloud")
    
    process = psutil.Process()
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=10)
    
    initial_threads = threading.active_count()
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    try:
        initial_handles = len(process.open_files())
    except:
        initial_handles = 0
    
    print(f"Initial state: {initial_threads} threads, {initial_memory:.2f} MB, {initial_handles} handles")
    
    metrics = {
        'iterations': 0,
        'thread_samples': [],
        'memory_samples': [],
        'errors': []
    }
    
    def create_metadata():
        return EQMetadata(
            tenant_id="leak-test",
            account_id=f"acc_{uuid.uuid4()}",
            interaction_id=f"int_{uuid.uuid4()}",
            interaction_type="email",
            text="Resource leak test content",
            timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            user_id="leaktest@test.com",
            source_system="internal"
        )
    
    while datetime.now() < end_time:
        try:
            for _ in range(100):
                neo4j = StorageFactory.get_graph_storage()
                pinecone = StorageFactory.get_embedding_storage()
                
                health = neo4j.health_check()
                
                if metrics['iterations'] % 1000 == 0:
                    metadata = create_metadata()
                    node_id = f"leak_test_{metrics['iterations']}_{uuid.uuid4()}"
                    neo4j.add_node(node_id, "leak_test", metadata, {"iteration": metrics['iterations']})
                
                metrics['iterations'] += 1
            
            gc.collect()
            
            current_threads = threading.active_count()
            current_memory = process.memory_info().rss / 1024 / 1024
            
            metrics['thread_samples'].append(current_threads)
            metrics['memory_samples'].append(current_memory)
            
            if metrics['iterations'] % 6000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds() / 60
                print(f"  {elapsed:.1f} min: {current_threads} threads, {current_memory:.2f} MB, {metrics['iterations']:,} iterations")
            
            time.sleep(0.1)
            
        except Exception as e:
            metrics['errors'].append(str(e))
            print(f"Error during leak test: {e}")
    
    final_threads = threading.active_count()
    final_memory = process.memory_info().rss / 1024 / 1024
    try:
        final_handles = len(process.open_files())
    except:
        final_handles = initial_handles
    
    try:
        neo4j = StorageFactory.get_graph_storage()
        neo4j.clear_tenant_data("leak-test")
        print("✅ Leak test data cleaned up")
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")
    
    StorageFactory.cleanup()
    
    thread_increase = final_threads - initial_threads
    memory_increase = final_memory - initial_memory
    handle_increase = final_handles - initial_handles
    
    results = {
        'duration_minutes': 10,
        'total_iterations': metrics['iterations'],
        'initial_threads': initial_threads,
        'final_threads': final_threads,
        'thread_increase': thread_increase,
        'initial_memory_mb': initial_memory,
        'final_memory_mb': final_memory,
        'memory_increase_mb': memory_increase,
        'initial_handles': initial_handles,
        'final_handles': final_handles,
        'handle_increase': handle_increase,
        'error_count': len(metrics['errors']),
        'leak_detected': thread_increase > 5 or memory_increase > 100,
        'status': 'PASS' if thread_increase <= 5 and memory_increase <= 100 else 'FAIL'
    }
    
    print(f"\n=== RESOURCE LEAK TEST RESULTS ===")
    print(f"Duration: 10 minutes")
    print(f"Total iterations: {metrics['iterations']:,}")
    print(f"Threads: {initial_threads} → {final_threads} (+{thread_increase})")
    print(f"Memory: {initial_memory:.2f} MB → {final_memory:.2f} MB (+{memory_increase:.2f} MB)")
    print(f"Handles: {initial_handles} → {final_handles} (+{handle_increase})")
    print(f"Errors: {len(metrics['errors'])}")
    print(f"Status: {results['status']}")
    
    if results['status'] == 'PASS':
        print("✅ NO RESOURCE LEAKS DETECTED")
    else:
        print("❌ RESOURCE LEAKS DETECTED")
    
    return results


if __name__ == "__main__":
    results = test_extended_resource_leak()
    print(f"\nLeak test complete: {results['status']}")
