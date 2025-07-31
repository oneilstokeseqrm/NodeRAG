import os
from typing import Dict, Any, Optional, List
from pathlib import Path
import yaml

from .Node_config import NodeConfig
from ..standards.eq_metadata import EQMetadata


class EQConfig(NodeConfig):
    """Extended NodeRAG configuration for EQ multi-tenant platform"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self._metadata_config = {}
        self._storage_config = {}
        self._multi_tenant_config = {}
        
        if 'eq_config' in config:
            self._load_eq_config(config['eq_config'])
    
    def _load_eq_config(self, eq_config: Dict[str, Any]):
        """Load EQ-specific configuration from config dict"""
        self._metadata_config = eq_config.get('metadata', {})
        self._storage_config = eq_config.get('storage', {})
        self._multi_tenant_config = eq_config.get('multi_tenant', {})
    
    @property
    def metadata_config(self) -> Dict[str, Any]:
        """Get metadata configuration"""
        return {
            'required_fields': [
                'tenant_id', 'interaction_id', 'interaction_type', 
                'text', 'account_id', 'timestamp', 'user_id', 'source_system'
            ],
            'interaction_types': ['call', 'chat', 'email', 'voice_memo', 'custom_notes'],
            'source_systems': ['internal', 'voice_memo', 'custom', 'outlook', 'gmail'],
            **self._metadata_config
        }
    
    @property
    def neo4j_config(self) -> Dict[str, Any]:
        """Get Neo4j configuration"""
        return {
            'uri': os.getenv('NEO4J_URI', self._storage_config.get('neo4j_uri', 'bolt://localhost:7687')),
            'user': os.getenv('NEO4J_USER', self._storage_config.get('neo4j_user', 'neo4j')),
            'password': os.getenv('NEO4J_PASSWORD', self._storage_config.get('neo4j_password', '')),
            'database': self._storage_config.get('neo4j_database', 'neo4j')
        }
    
    @property
    def pinecone_config(self) -> Dict[str, Any]:
        """Get Pinecone configuration"""
        return {
            'api_key': os.getenv('PINECONE_API_KEY', self._storage_config.get('pinecone_api_key', '')),
            'environment': os.getenv('PINECONE_ENV', self._storage_config.get('pinecone_environment', 'us-east-1')),
            'index_name': os.getenv('PINECONE_INDEX', self._storage_config.get('pinecone_index', 'eq-noderag')),
            'dimension': self._storage_config.get('pinecone_dimension', 1536)
        }
    
    @property
    def multi_tenant_config(self) -> Dict[str, Any]:
        """Get multi-tenant configuration"""
        return {
            'require_tenant_id': self._multi_tenant_config.get('require_tenant_id', True),
            'require_account_id': self._multi_tenant_config.get('require_account_id', False),
            'enable_cross_account_search': self._multi_tenant_config.get('enable_cross_account_search', False),
            'default_tenant_id': self._multi_tenant_config.get('default_tenant_id', None)
        }
    
    @property
    def current_metadata(self) -> Optional[Dict[str, Any]]:
        """Get current metadata context (to be set during processing)"""
        return getattr(self, '_current_metadata', None)
    
    @current_metadata.setter
    def current_metadata(self, metadata: Dict[str, Any]):
        """Set current metadata context with validation"""
        eq_metadata = EQMetadata(**metadata)
        errors = eq_metadata.validate()
        if errors:
            raise ValueError(f"Invalid metadata: {errors}")
        
        self._current_metadata = metadata
    
    def validate_config(self) -> List[str]:
        """Validate the entire configuration"""
        errors = []
        
        if not self.neo4j_config['password']:
            errors.append("Neo4j password not configured")
        
        if not self.pinecone_config['api_key']:
            errors.append("Pinecone API key not configured")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Export configuration as dictionary"""
        base_config = super().config if hasattr(super(), 'config') else {}
        
        return {
            **base_config,
            'eq_config': {
                'metadata': self._metadata_config,
                'storage': {
                    'neo4j_uri': self.neo4j_config['uri'],
                    'neo4j_user': self.neo4j_config['user'],
                    'neo4j_database': self.neo4j_config['database'],
                    'pinecone_environment': self.pinecone_config['environment'],
                    'pinecone_index': self.pinecone_config['index_name'],
                    'pinecone_dimension': self.pinecone_config['dimension']
                },
                'multi_tenant': self._multi_tenant_config
            }
        }
    
    @classmethod
    def from_main_folder(cls, main_folder: str):
        """Create EQConfig from main folder with EQ extensions"""
        config_path = cls.create_config_file(main_folder)
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return cls(config)
