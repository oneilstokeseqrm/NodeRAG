"""
Load testing with 1000+ concurrent requests for Task 4.0.1d
"""
import os
import concurrent.futures
import threading
import time
import traceback
import uuid
from statistics import mean, median
import psutil
from datetime import datetime, timezone

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.standards.eq_metadata import EQMetadata


class LoadTester:
    """Load testing for cloud storage"""
    
    def __init__(self):
        self.metrics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'response_times': [],
            'errors': [],
            'thread_counts': [],
            'memory_samples': []
        }
        self.lock = threading.Lock()
        import os
        os.makedirs('/tmp/load_test', exist_ok=True)
        self.config = {
            'config': {
                'main_folder': '/tmp/load_test',
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
    
    def single_operation(self, operation_id: int):
        """Execute a single operation"""
        start = time.time()
        try:
            neo4j = StorageFactory.get_graph_storage()
            pinecone = StorageFactory.get_embedding_storage()
            
            neo4j2 = StorageFactory.get_graph_storage()
            if neo4j is not neo4j2:
                raise Exception(f"Operation {operation_id}: Singleton broken!")
            
            health = neo4j.health_check()
            if health['status'] != 'healthy':
                raise Exception(f"Unhealthy Neo4j: {health}")
            
            metadata = EQMetadata(
                tenant_id="load-test",
                account_id=f"acc_{uuid.uuid4()}",
                interaction_id=f"int_{uuid.uuid4()}",
                interaction_type="email",
                text=f"Load test operation {operation_id}",
                timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                user_id="loadtest@test.com",
                source_system="internal"
            )
            
            node_id = f"load_test_{operation_id}_{uuid.uuid4()}"
            success = neo4j.add_node(node_id, "load_test", metadata, {"operation_id": operation_id})
            
            if not success:
                raise Exception(f"Failed to add node for operation {operation_id}")
            
            with self.lock:
                self.metrics['successful_requests'] += 1
                self.metrics['response_times'].append(time.time() - start)
                
        except Exception as e:
            with self.lock:
                self.metrics['failed_requests'] += 1
                self.metrics['errors'].append({
                    'operation_id': operation_id,
                    'error': str(e),
                    'traceback': traceback.format_exc()
                })
    
    def monitor_resources(self, duration: int):
        """Monitor system resources during test"""
        process = psutil.Process()
        start_time = time.time()
        
        while time.time() - start_time < duration:
            with self.lock:
                self.metrics['thread_counts'].append(threading.active_count())
                self.metrics['memory_samples'].append(
                    process.memory_info().rss / 1024 / 1024  # MB
                )
            time.sleep(0.5)
    
    def run_load_test(self, num_requests: int = 1000, max_workers: int = 50):
        """Execute load test"""
        print(f"\n=== LOAD TEST: {num_requests} requests, {max_workers} workers ===")
        
        StorageFactory.initialize(self.config, backend_mode="cloud")
        
        monitor_thread = threading.Thread(
            target=self.monitor_resources,
            args=(60,)  # Monitor for 60 seconds
        )
        monitor_thread.start()
        
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.single_operation, i) 
                for i in range(num_requests)
            ]
            concurrent.futures.wait(futures)
        
        total_time = time.time() - start_time
        monitor_thread.join()
        
        self.metrics['total_requests'] = num_requests
        self.metrics['total_time'] = total_time
        self.metrics['requests_per_second'] = num_requests / total_time
        
        if self.metrics['response_times']:
            self.metrics['avg_response_time'] = mean(self.metrics['response_times'])
            self.metrics['median_response_time'] = median(self.metrics['response_times'])
            self.metrics['p95_response_time'] = sorted(self.metrics['response_times'])[
                int(len(self.metrics['response_times']) * 0.95)
            ]
        
        self.metrics['max_threads'] = max(self.metrics['thread_counts']) if self.metrics['thread_counts'] else 0
        self.metrics['avg_memory_mb'] = mean(self.metrics['memory_samples']) if self.metrics['memory_samples'] else 0
        
        print(f"\n=== LOAD TEST RESULTS ===")
        print(f"Total requests: {self.metrics['total_requests']}")
        print(f"Successful: {self.metrics['successful_requests']}")
        print(f"Failed: {self.metrics['failed_requests']}")
        print(f"Success rate: {(self.metrics['successful_requests']/self.metrics['total_requests']*100):.1f}%")
        print(f"Total time: {total_time:.2f}s")
        print(f"Requests/second: {self.metrics['requests_per_second']:.1f}")
        print(f"Avg response time: {self.metrics['avg_response_time']*1000:.1f}ms")
        print(f"P95 response time: {self.metrics['p95_response_time']*1000:.1f}ms")
        print(f"Max threads: {self.metrics['max_threads']}")
        print(f"Avg memory: {self.metrics['avg_memory_mb']:.1f} MB")
        
        try:
            neo4j = StorageFactory.get_graph_storage()
            neo4j.clear_tenant_data("load-test")
            print("✅ Load test data cleaned up")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")
        
        StorageFactory.cleanup()
        
        return self.metrics


if __name__ == "__main__":
    tester = LoadTester()
    results = tester.run_load_test(1000, 50)
    print(f"\nLoad test complete: {results['successful_requests']}/{results['total_requests']} successful")
