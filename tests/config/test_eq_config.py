import pytest
import os
import tempfile
import yaml
from NodeRAG.config.eq_config import EQConfig
from NodeRAG.standards.eq_metadata import EQMetadata

class TestEQConfig:
    """Test cases for EQ Configuration module"""
    
    def setup_method(self):
        """Create temporary config file for testing"""
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
        self.config_data = {
            'config': {
                'main_folder': '/tmp/test_noderag',
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
                    'neo4j_uri': 'bolt://test:7687',
                    'neo4j_user': 'test_user',
                    'neo4j_database': 'test_db',
                    'pinecone_environment': 'test-env',
                    'pinecone_index': 'test-index',
                    'pinecone_dimension': 1536
                },
                'multi_tenant': {
                    'require_tenant_id': True,
                    'require_account_id': True,
                    'enable_cross_account_search': False,
                    'default_tenant_id': None
                }
            }
        }
        yaml.dump(self.config_data, self.temp_file)
        self.temp_file.close()
        
        os.makedirs('/tmp/test_noderag', exist_ok=True)
    
    def teardown_method(self):
        """Clean up temp file and folder"""
        os.unlink(self.temp_file.name)
        if os.path.exists('/tmp/test_noderag'):
            import shutil
            shutil.rmtree('/tmp/test_noderag')
    
    def test_eq_config_initialization(self):
        """Test EQConfig initialization"""
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        
        assert eq_config is not None
        assert hasattr(eq_config, 'metadata_config')
        assert hasattr(eq_config, 'neo4j_config')
        assert hasattr(eq_config, 'pinecone_config')
        assert hasattr(eq_config, 'multi_tenant_config')
    
    def test_metadata_config_defaults(self):
        """Test metadata configuration defaults"""
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        metadata_config = eq_config.metadata_config
        
        assert 'required_fields' in metadata_config
        assert len(metadata_config['required_fields']) == 8
        assert 'tenant_id' in metadata_config['required_fields']
        assert 'interaction_id' in metadata_config['required_fields']
        assert 'interaction_type' in metadata_config['required_fields']
        assert 'text' in metadata_config['required_fields']
        assert 'account_id' in metadata_config['required_fields']
        assert 'timestamp' in metadata_config['required_fields']
        assert 'user_id' in metadata_config['required_fields']
        assert 'source_system' in metadata_config['required_fields']
        
        assert metadata_config['interaction_types'] == ['call', 'chat', 'email', 'voice_memo', 'custom_notes']
        assert metadata_config['source_systems'] == ['internal', 'voice_memo', 'custom', 'outlook', 'gmail']
    
    def test_storage_config_loading(self):
        """Test storage configuration loading"""
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        
        neo4j = eq_config.neo4j_config
        assert neo4j['uri'] == 'bolt://test:7687'
        assert neo4j['user'] == 'test_user'
        assert neo4j['database'] == 'test_db'
        
        pinecone = eq_config.pinecone_config
        assert pinecone['environment'] == 'test-env'
        assert pinecone['index_name'] == 'test-index'
        assert pinecone['dimension'] == 1536
    
    def test_environment_variable_override(self):
        """Test environment variable override for storage config"""
        os.environ['NEO4J_URI'] = 'bolt://env-host:7687'
        os.environ['PINECONE_API_KEY'] = 'test-api-key'
        
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        
        assert eq_config.neo4j_config['uri'] == 'bolt://env-host:7687'
        assert eq_config.pinecone_config['api_key'] == 'test-api-key'
        
        del os.environ['NEO4J_URI']
        del os.environ['PINECONE_API_KEY']
    
    def test_multi_tenant_config(self):
        """Test multi-tenant configuration"""
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        mt_config = eq_config.multi_tenant_config
        
        assert mt_config['require_tenant_id'] is True
        assert mt_config['require_account_id'] is True
        assert mt_config['enable_cross_account_search'] is False
        assert mt_config['default_tenant_id'] is None
    
    def test_current_metadata_validation(self):
        """Test current metadata setter with validation"""
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        
        valid_metadata = {
            'tenant_id': 'tenant_acme',
            'interaction_id': 'int_550e8400-e29b-41d4-a716-446655440000',
            'interaction_type': 'email',
            'text': 'Test email content',
            'account_id': 'acc_6ba7b810-9dad-41d1-80b4-00c04fd430c8',
            'timestamp': '2024-01-15T10:30:00Z',
            'user_id': 'usr_6ba7b812-9dad-41d1-80b4-00c04fd430c8',
            'source_system': 'outlook'
        }
        
        eq_config.current_metadata = valid_metadata
        assert eq_config.current_metadata == valid_metadata
        
        invalid_metadata = valid_metadata.copy()
        invalid_metadata['tenant_id'] = ''  # Empty field
        
        with pytest.raises(ValueError, match="Invalid metadata"):
            eq_config.current_metadata = invalid_metadata
    
    def test_config_validation(self):
        """Test configuration validation"""
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        
        errors = eq_config.validate_config()
        assert len(errors) == 2
        assert any('Neo4j password' in e for e in errors)
        assert any('Pinecone API key' in e for e in errors)
        
        os.environ['NEO4J_PASSWORD'] = 'test-password'
        os.environ['PINECONE_API_KEY'] = 'test-api-key'
        
        eq_config2 = EQConfig(config)
        errors2 = eq_config2.validate_config()
        assert len(errors2) == 0
        
        del os.environ['NEO4J_PASSWORD']
        del os.environ['PINECONE_API_KEY']
    
    def test_config_export(self):
        """Test configuration export to dictionary"""
        with open(self.temp_file.name, 'r') as f:
            config = yaml.safe_load(f)
        
        eq_config = EQConfig(config)
        config_dict = eq_config.to_dict()
        
        assert 'eq_config' in config_dict
        assert 'metadata' in config_dict['eq_config']
        assert 'storage' in config_dict['eq_config']
        assert 'multi_tenant' in config_dict['eq_config']
    
    def test_from_main_folder_classmethod(self):
        """Test creating EQConfig from main folder"""
        test_folder = '/tmp/test_eq_config'
        os.makedirs(test_folder, exist_ok=True)
        
        try:
            eq_config = EQConfig.from_main_folder(test_folder)
            assert eq_config is not None
            assert eq_config.main_folder == test_folder
        finally:
            if os.path.exists(test_folder):
                import shutil
                shutil.rmtree(test_folder)
