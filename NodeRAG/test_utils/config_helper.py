"""
Test configuration helper for NodeRAG testing
Provides consistent NodeConfig initialization for all tests
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

def load_test_config() -> Dict[str, Any]:
    """
    Load test configuration with all required fields for NodeConfig
    
    Returns:
        Dict containing all required configuration fields
    """
    yaml_path = Path('NodeRAG/config/Node_config.yaml')
    
    if yaml_path.exists():
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        
        config['config']['main_folder'] = './test_output'
        
        os.makedirs('./test_output', exist_ok=True)
        os.makedirs('./test_output/info', exist_ok=True)
        os.makedirs('./test_output/cache', exist_ok=True)
        os.makedirs('./test_output/chromadb', exist_ok=True)
        os.makedirs('./test_output/llm_cache', exist_ok=True)
        os.makedirs('./test_output/llm_error_cache', exist_ok=True)
        
        text_decomposition_path = './test_output/cache/text_decomposition.jsonl'
        if not os.path.exists(text_decomposition_path):
            with open(text_decomposition_path, 'w') as f:
                pass  # Create empty file
        
        document_hash_path = './test_output/info/document_hash.json'
        if not os.path.exists(document_hash_path):
            import json
            with open(document_hash_path, 'w') as f:
                json.dump({'document_path': './test_output/documents'}, f)  # Document path registry
        
        return config
    else:
        return get_minimal_test_config()

def get_minimal_test_config() -> Dict[str, Any]:
    """
    Get minimal configuration required for NodeConfig initialization
    Used when YAML file is not available
    
    Returns:
        Dict with minimal required configuration
    """
    config = {
        'model_config': {
            'service_provider': 'openai',
            'model_name': 'gpt-4o-mini',
            'api_keys': None,
            'temperature': 0,
            'max_tokens': 10000,
            'rate_limit': 40
        },
        'embedding_config': {
            'service_provider': 'openai_embedding',
            'embedding_model_name': 'text-embedding-3-small',
            'api_keys': None,
            'rate_limit': 20
        },
        'config': {
            'main_folder': './test_output',
            'language': 'English',
            'docu_type': 'mixed',
            'chunk_size': 1048,
            'embedding_batch_size': 50,
            'use_tqdm': False,
            'use_rich': True,
            'space': 'l2',
            'dim': 1536,
            'm': 50,
            'ef': 200,
            'm0': None,
            'Hcluster_size': 39,
            'url': '127.0.0.1',
            'port': 5000,
            'unbalance_adjust': True,
            'cross_node': 10,
            'Enode': 10,
            'Rnode': 30,
            'Hnode': 10,
            'HNSW_results': 10,
            'similarity_weight': 1,
            'accuracy_weight': 1,
            'ppr_alpha': 0.5,
            'ppr_max_iter': 2
        },
        'eq_config': {
            'metadata': {
                'validate_on_set': True,
                'strict_validation': True
            },
            'storage': {
                'neo4j_uri': 'bolt://localhost:7687',
                'neo4j_user': 'neo4j',
                'neo4j_database': 'neo4j',
                'pinecone_environment': 'us-east-1',
                'pinecone_index': 'eq-noderag',
                'pinecone_dimension': 1536
            },
            'multi_tenant': {
                'require_tenant_id': True,
                'require_account_id': False,
                'enable_cross_account_search': False,
                'default_tenant_id': None
            }
        }
    }
    
    os.makedirs('./test_output', exist_ok=True)
    os.makedirs('./test_output/info', exist_ok=True)
    os.makedirs('./test_output/cache', exist_ok=True)
    os.makedirs('./test_output/chromadb', exist_ok=True)
    os.makedirs('./test_output/llm_cache', exist_ok=True)
    os.makedirs('./test_output/llm_error_cache', exist_ok=True)
    
    text_decomposition_path = './test_output/cache/text_decomposition.jsonl'
    if not os.path.exists(text_decomposition_path):
        with open(text_decomposition_path, 'w') as f:
            pass  # Create empty file
    
    document_hash_path = './test_output/info/document_hash.json'
    if not os.path.exists(document_hash_path):
        import json
        with open(document_hash_path, 'w') as f:
            json.dump({'document_path': './test_output/documents'}, f)  # Document path registry
    
    return config

def create_test_nodeconfig():
    """
    Create a properly initialized NodeConfig instance for testing
    
    Returns:
        NodeConfig instance with test configuration
    """
    import sys
    import os
    
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    
    from NodeRAG.config import NodeConfig
    
    config_dict = load_test_config()
    return NodeConfig(config_dict)

def cleanup_test_output():
    """Clean up test output directories after tests"""
    import shutil
    
    test_dir = Path('./test_output')
    if test_dir.exists():
        shutil.rmtree(test_dir)
        print(f"Cleaned up test directory: {test_dir}")

if __name__ == "__main__":
    print("=== Testing Config Helper ===")
    
    try:
        config_dict = load_test_config()
        print(f"✅ Test config loaded with {len(config_dict)} keys")
        print(f"   Main folder: {config_dict.get('config', {}).get('main_folder')}")
        
        node_config = create_test_nodeconfig()
        print("✅ NodeConfig instance created successfully")
        
        if hasattr(node_config, 'main_folder'):
            print(f"✅ NodeConfig.main_folder = {node_config.main_folder}")
        
    except Exception as e:
        print(f"❌ Config helper test failed: {e}")
        import traceback
        traceback.print_exc()
