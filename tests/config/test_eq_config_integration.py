import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import tempfile
import yaml
import os

from NodeRAG.config.eq_config import EQConfig
from NodeRAG.standards.eq_metadata import EQMetadata

def test_config_metadata_integration():
    """Test that EQConfig properly integrates with EQMetadata"""
    
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    config_data = {
        'config': {
            'main_folder': '/tmp/test_integration',
            'language': 'English',
            'chunk_size': 1048
        },
        'model_config': {
            'service_provider': 'openai',
            'model_name': 'gpt-4o-mini',
            'temperature': 0,
            'max_tokens': 10000,
            'rate_limit': 40
        },
        'embedding_config': {
            'service_provider': 'openai_embedding',
            'embedding_model_name': 'text-embedding-3-small',
            'rate_limit': 20
        },
        'eq_config': {
            'metadata': {
                'validate_on_set': True,
                'strict_validation': True
            },
            'storage': {
                'neo4j_uri': 'bolt://localhost:7687',
                'neo4j_user': 'neo4j',
                'pinecone_environment': 'us-east-1',
                'pinecone_index': 'eq-noderag'
            },
            'multi_tenant': {
                'require_tenant_id': True,
                'require_account_id': False
            }
        }
    }
    yaml.dump(config_data, temp_file)
    temp_file.close()
    
    os.makedirs('/tmp/test_integration', exist_ok=True)
    
    try:
        with open(temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        
        test_metadata = {
            'tenant_id': 'tenant_acme',
            'interaction_id': 'int_a1b3eade-7ab5-48b8-b1b1-84e954723147',
            'interaction_type': 'email',
            'text': 'Subject: Invoice Discrepancy - Need Immediate Review\n\nHi Team,\n\nI noticed a discrepancy in invoice #INV-2024-0156...',
            'account_id': 'acc_6997f38c-1d49-4255-8fb6-a4424abf3bde',
            'timestamp': '2024-01-15T10:30:00Z',
            'user_id': 'usr_13753caa-8c95-45b5-b53a-100a6361cdd8',
            'source_system': 'outlook'
        }
        
        eq_config.current_metadata = test_metadata
        
        assert eq_config.current_metadata == test_metadata
        
        metadata_obj = EQMetadata(**eq_config.current_metadata)
        validation_errors = metadata_obj.validate()
        assert validation_errors == []
        
        metadata_config = eq_config.metadata_config
        assert 'required_fields' in metadata_config
        assert len(metadata_config['required_fields']) == 8
        
        neo4j_config = eq_config.neo4j_config
        assert neo4j_config['uri'] == 'bolt://localhost:7687'
        assert neo4j_config['user'] == 'neo4j'
        
        pinecone_config = eq_config.pinecone_config
        assert pinecone_config['environment'] == 'us-east-1'
        assert pinecone_config['index_name'] == 'eq-noderag'
        
        mt_config = eq_config.multi_tenant_config
        assert mt_config['require_tenant_id'] is True
        assert mt_config['require_account_id'] is False
        
        print("✅ EQConfig successfully integrates with EQMetadata Standard")
        print(f"✅ Metadata validation passed for {len(metadata_config['required_fields'])} required fields")
        print(f"✅ Storage configuration loaded: Neo4j={neo4j_config['uri']}, Pinecone={pinecone_config['index_name']}")
        print(f"✅ Multi-tenant configuration: require_tenant_id={mt_config['require_tenant_id']}")
        
        return True
        
    finally:
        os.unlink(temp_file.name)
        if os.path.exists('/tmp/test_integration'):
            import shutil
            shutil.rmtree('/tmp/test_integration')

if __name__ == "__main__":
    test_config_metadata_integration()
