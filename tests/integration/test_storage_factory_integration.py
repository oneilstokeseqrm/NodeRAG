"""
Integration tests for StorageFactory with real connections
"""
import pytest
import os
from datetime import datetime
import uuid

from NodeRAG.storage.storage_factory import StorageFactory
from NodeRAG.standards.eq_metadata import EQMetadata


@pytest.mark.integration
class TestStorageFactoryIntegration:
    """Integration tests requiring real Neo4j and Pinecone connections"""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test"""
        yield
        StorageFactory.cleanup()
    
    @pytest.mark.skipif(
        not os.getenv('NEO4J_URI'),
        reason="Neo4j credentials not available"
    )
    def test_neo4j_real_connection(self):
        """Test real Neo4j connection and operations"""
        config = {
            'eq_config': {
                'storage': {
                    'neo4j_uri': os.getenv('NEO4J_URI'),
                    'neo4j_user': os.getenv('NEO4J_USER', 'neo4j'),
                    'neo4j_password': os.getenv('NEO4J_PASSWORD')
                }
            }
        }
        
        StorageFactory.initialize(config, backend_mode="neo4j")
        neo4j_adapter = StorageFactory.get_graph_storage()
        
        assert neo4j_adapter is not None
        
        test_metadata = EQMetadata(
            tenant_id="test-tenant",
            account_id=f"acc_{uuid.uuid4()}",
            interaction_id=f"int_{uuid.uuid4()}",
            interaction_type="email",
            text="Test content",
            timestamp=datetime.utcnow().isoformat() + 'Z',
            user_id="test@example.com",
            source_system="test"
        )
        
        assert neo4j_adapter is not None
    
    @pytest.mark.skipif(
        not os.getenv('PINECONE_API_KEY'),
        reason="Pinecone credentials not available"
    )
    def test_pinecone_real_connection(self):
        """Test real Pinecone connection"""
        config = {
            'eq_config': {
                'storage': {
                    'pinecone_api_key': os.getenv('PINECONE_API_KEY'),
                    'pinecone_index': os.getenv('PINECONE_INDEX', 'noderag-test')
                }
            }
        }
        
        StorageFactory.initialize(config, backend_mode="cloud")
        pinecone_adapter = StorageFactory.get_embedding_storage()
        
        assert pinecone_adapter is not None
        
        assert pinecone_adapter.index is not None
