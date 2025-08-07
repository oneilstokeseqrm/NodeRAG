"""
Example of using optimized StorageFactory with Graph_pipeline
"""
import os
import json
from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.config import NodeConfig
from NodeRAG.src.pipeline.graph_pipeline import Graph_pipeline


def main():
    """Example usage with all optimizations"""
    
    # Original configuration (same as before)
    config = {
        'config': {
            'main_folder': '/tmp/app_data',
            'language': 'en',
            'chunk_size': 512
        },
        'model_config': {
            'model_name': 'gpt-4o',
            'temperature': 0.7
        },
        'embedding_config': {
            'model_name': 'gpt-4o',
            'dimension': 3072
        },
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
    
    # Initialize StorageFactory with optimizations
    print("Initializing StorageFactory with optimizations...")
    StorageFactory.initialize(
        config,
        backend_mode="cloud",
        lazy_init=True,           # Faster startup
        warmup_connections=False   # Warmup later if needed
    )
    
    # Optional: Preload adapters during app initialization
    # This moves the initialization cost to startup instead of first request
    print("Preloading adapters...")
    StorageFactory.preload_adapters()
    
    # Get pipeline configuration
    print("Getting pipeline configuration...")
    pipeline_config = StorageFactory.get_pipeline_config()
    
    # Create NodeConfig and Graph_pipeline (now works seamlessly!)
    print("Creating Graph_pipeline...")
    node_config = NodeConfig(pipeline_config)
    pipeline = Graph_pipeline(node_config)
    
    # Use cached health checks for monitoring
    health = StorageFactory.get_cached_health_check(cache_ttl=60)
    print(f"System health: {health['status']}")
    
    # Get storage adapters (already initialized)
    neo4j = StorageFactory.get_graph_storage()
    pinecone = StorageFactory.get_embedding_storage()
    
    # Check initialization status
    status = StorageFactory.get_initialization_status()
    print(f"Initialization status: {json.dumps(status, indent=2)}")
    
    print("Pipeline ready for use!")
    
    # Your pipeline operations here...
    
    # Cleanup when done
    StorageFactory.cleanup()


if __name__ == "__main__":
    main()
