"""
Storage adapter for Graph_pipeline to use StorageFactory
"""
import pickle
import json
import pandas as pd
from pathlib import Path
from typing import Any, Optional, Dict, List
import logging
import asyncio
import uuid
import numpy as np
from datetime import datetime, timezone

from ...storage.storage_factory import StorageFactory
from ...storage.storage import storage
from ...standards.eq_metadata import EQMetadata

logger = logging.getLogger(__name__)

class PipelineStorageAdapter:
    """Adapter to route Graph_pipeline storage operations through StorageFactory"""
    
    def __init__(self, backend_mode: Optional[str] = None):
        """
        Initialize storage adapter
        
        Args:
            backend_mode: Override backend mode, otherwise uses StorageFactory's current mode
        """
        self.backend_mode = backend_mode or self._detect_backend_mode()
        logger.info(f"PipelineStorageAdapter initialized with backend: {self.backend_mode}")
    
    def _detect_backend_mode(self) -> str:
        """Detect current storage backend mode"""
        try:
            status = StorageFactory.get_initialization_status()
            if status.get('neo4j_adapter') and status.get('pinecone_adapter'):
                return 'cloud'
            elif status.get('backend_mode') == 'file':
                return 'file'
        except:
            pass
        
        return 'file'
    
    def save_pickle(self, data: Any, filepath: str, component_type: str = 'graph', 
                    tenant_id: str = "default") -> bool:
        """Save data as pickle through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type == 'graph':
                graph_storage = StorageFactory.get_graph_storage()
                if hasattr(graph_storage, 'add_node'):
                    return self._store_graph_in_neo4j(data, graph_storage, tenant_id)
            
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
            return True
                
        except Exception as e:
            logger.error(f"Failed to save pickle {filepath}: {e}")
            return False
    
    def load_pickle(self, filepath: str, component_type: str = 'graph',
                    tenant_id: str = "default") -> Optional[Any]:
        """Load pickle data through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type == 'graph':
                graph_storage = StorageFactory.get_graph_storage()
                if hasattr(graph_storage, 'get_subgraph'):
                    data = self._load_graph_from_neo4j(graph_storage, tenant_id)
                    if data is not None:
                        return data
            
            if Path(filepath).exists():
                with open(filepath, 'rb') as f:
                    return pickle.load(f)
            return None
                
        except Exception as e:
            logger.error(f"Failed to load pickle {filepath}: {e}")
            return None
    
    def save_parquet(self, df: pd.DataFrame, filepath: str, component_type: str = 'data', 
                     append: bool = False, namespace: str = "default") -> bool:
        """Save DataFrame as parquet through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type in ['embeddings', 'vectors']:
                embedding_storage = StorageFactory.get_embedding_storage()
                if hasattr(embedding_storage, 'upsert_vector'):
                    return self._store_embeddings_in_pinecone(df, embedding_storage, namespace)
            
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            if append and Path(filepath).exists():
                existing_df = pd.read_parquet(filepath)
                df = pd.concat([existing_df, df], ignore_index=True)
            df.to_parquet(filepath)
            return True
                
        except Exception as e:
            logger.error(f"Failed to save parquet {filepath}: {e}")
            return False
    
    def load_parquet(self, filepath: str, component_type: str = 'data',
                     namespace: str = "default") -> Optional[pd.DataFrame]:
        """Load parquet data through StorageFactory"""
        try:
            if self.backend_mode == 'cloud' and component_type in ['embeddings', 'vectors']:
                embedding_storage = StorageFactory.get_embedding_storage()
                if hasattr(embedding_storage, 'search'):
                    df = self._load_embeddings_from_pinecone(embedding_storage, namespace)
                    if df is not None:
                        return df
            
            if Path(filepath).exists():
                return pd.read_parquet(filepath)
            return None
                
        except Exception as e:
            logger.error(f"Failed to load parquet {filepath}: {e}")
            return None
    
    def save_json(self, data: Any, filepath: str) -> bool:
        """Save JSON data"""
        try:
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to save JSON {filepath}: {e}")
            return False
    
    def load_json(self, filepath: str) -> Optional[Any]:
        """Load JSON data"""
        try:
            if Path(filepath).exists():
                with open(filepath, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            logger.error(f"Failed to load JSON {filepath}: {e}")
            return None
    
    def _store_graph_in_neo4j(self, graph_data: Any, neo4j, tenant_id: str = "default") -> bool:
        """
        Store graph data in Neo4j with proper tenant isolation
        
        Args:
            graph_data: NetworkX graph or similar graph object
            neo4j: Neo4j adapter instance from StorageFactory
            tenant_id: Tenant identifier for data isolation
        """
        try:
            if not hasattr(graph_data, 'nodes') or not hasattr(graph_data, 'edges'):
                logger.warning(f"Graph data missing nodes or edges methods: {type(graph_data)}")
                return False
            
            metadata = EQMetadata(
                tenant_id=tenant_id,
                account_id=f"pipeline_{uuid.uuid4()}",
                interaction_id=f"store_{uuid.uuid4()}",
                interaction_type="graph_storage",
                text="Graph storage operation",
                timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                user_id="pipeline@system",
                source_system="graph_pipeline"
            )
            
            node_count = 0
            for node_id, node_data in graph_data.nodes(data=True):
                node_id_str = str(node_id)
                
                node_type = node_data.get('type', 'entity')
                
                properties = {k: v for k, v in node_data.items() if k != 'type'}
                
                success = neo4j.add_node(
                    node_id=node_id_str,
                    node_type=node_type,
                    metadata=metadata,
                    properties=properties
                )
                
                if success:
                    node_count += 1
                else:
                    logger.warning(f"Failed to store node {node_id_str}")
            
            edge_count = 0
            for source, target, edge_data in graph_data.edges(data=True):
                source_str = str(source)
                target_str = str(target)
                
                rel_type = edge_data.get('type', 'connects')
                
                properties = {k: v for k, v in edge_data.items() if k != 'type'}
                
                success = neo4j.add_relationship(
                    source_id=source_str,
                    target_id=target_str,
                    relationship_type=rel_type,
                    metadata=metadata,
                    properties=properties
                )
                
                if success:
                    edge_count += 1
                else:
                    logger.warning(f"Failed to store edge {source_str} -> {target_str}")
            
            logger.info(f"Stored graph in Neo4j: {node_count} nodes, {edge_count} edges for tenant {tenant_id}")
            return node_count > 0 or edge_count > 0
            
        except Exception as e:
            logger.error(f"Failed to store graph in Neo4j: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _load_graph_from_neo4j(self, neo4j, tenant_id: str = "default") -> Optional[Any]:
        """
        Load graph data from Neo4j for a specific tenant
        
        Args:
            neo4j: Neo4j adapter instance
            tenant_id: Tenant identifier for data isolation
            
        Returns:
            NetworkX graph or None if failed
        """
        try:
            import networkx as nx
            
            G = nx.Graph()
            
            subgraph_data = neo4j.get_subgraph(tenant_id)
            
            if not subgraph_data:
                logger.warning(f"No graph data found for tenant {tenant_id}")
                return G  # Return empty graph rather than None
            
            nodes = subgraph_data.get('nodes', [])
            for node in nodes:
                node_id = node.get('node_id') or node.get('id')
                if node_id:
                    properties = {}
                    
                    if 'node_type' in node:
                        properties['type'] = node['node_type']
                    elif 'type' in node:
                        properties['type'] = node['type']
                    
                    for key, value in node.items():
                        if key not in ['node_id', 'id', 'node_type', 'metadata']:
                            properties[key] = value
                    
                    G.add_node(node_id, **properties)
            
            relationships = subgraph_data.get('relationships', [])
            for rel in relationships:
                source = rel.get('source') or rel.get('source_id')
                target = rel.get('target') or rel.get('target_id')
                
                if source and target:
                    properties = {}
                    
                    if 'relationship_type' in rel:
                        properties['type'] = rel['relationship_type']
                    elif 'type' in rel:
                        properties['type'] = rel['type']
                    
                    for key, value in rel.items():
                        if key not in ['source', 'target', 'source_id', 'target_id', 
                                       'relationship_type', 'metadata']:
                            properties[key] = value
                    
                    G.add_edge(source, target, **properties)
            
            logger.info(f"Loaded graph from Neo4j: {len(G.nodes())} nodes, {len(G.edges())} edges for tenant {tenant_id}")
            return G
            
        except Exception as e:
            logger.error(f"Failed to load graph from Neo4j: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _store_embeddings_in_pinecone(self, df: pd.DataFrame, pinecone, 
                                      namespace: str = "default") -> bool:
        """
        Store embeddings DataFrame in Pinecone
        
        Expected DataFrame columns:
        - 'hash_id' or 'id': Unique identifier for the vector
        - 'embedding': The embedding vector (list or numpy array)
        - Other columns become metadata
        
        Args:
            df: DataFrame with embeddings
            pinecone: Pinecone adapter instance
            namespace: Namespace for data isolation
        """
        try:
            if df.empty:
                logger.warning("Empty DataFrame provided for Pinecone storage")
                return True  # Not an error, just nothing to store
            
            id_column = None
            for col in ['hash_id', 'id', 'node_id']:
                if col in df.columns:
                    id_column = col
                    break
            
            if not id_column:
                logger.error("DataFrame missing ID column (hash_id, id, or node_id)")
                return False
            
            embedding_column = None
            for col in ['embedding', 'vector', 'embeddings']:
                if col in df.columns:
                    embedding_column = col
                    break
            
            if not embedding_column:
                logger.error("DataFrame missing embedding column")
                return False
            
            async def store_vectors():
                """Async function to store vectors in Pinecone"""
                tasks = []
                successful = 0
                failed = 0
                
                for idx, row in df.iterrows():
                    vector_id = str(row[id_column])
                    
                    embedding = row[embedding_column]
                    if isinstance(embedding, np.ndarray):
                        embedding = embedding.tolist()
                    elif isinstance(embedding, str):
                        logger.debug(f"Skipping non-embedded entry: {vector_id}")
                        continue
                    elif embedding is None or (isinstance(embedding, float) and np.isnan(embedding)):
                        logger.debug(f"Skipping null embedding: {vector_id}")
                        continue
                    
                    if not isinstance(embedding, list):
                        try:
                            if hasattr(embedding, '__iter__') and not isinstance(embedding, str):
                                embedding = list(embedding)
                            else:
                                logger.warning(f"Cannot convert embedding to list for {vector_id}: not iterable")
                                failed += 1
                                continue
                        except:
                            logger.warning(f"Cannot convert embedding to list for {vector_id}")
                            failed += 1
                            continue
                    
                    metadata_dict = {
                        'stored_at': datetime.now(timezone.utc).isoformat(),
                        'source': 'graph_pipeline'
                    }
                    
                    for col in df.columns:
                        if col not in [id_column, embedding_column]:
                            value = row[col]
                            if isinstance(value, np.integer):
                                value = int(value)
                            elif isinstance(value, np.floating):
                                value = float(value)
                            elif isinstance(value, np.ndarray):
                                value = value.tolist()
                            elif pd.isna(value):
                                continue
                            
                            metadata_dict[col] = str(value)
                    
                    eq_metadata = EQMetadata(
                        tenant_id=namespace,
                        account_id=metadata_dict.get('account_id', 'pipeline'),
                        interaction_id=metadata_dict.get('interaction_id', vector_id),
                        interaction_type=metadata_dict.get('type', 'embedding'),
                        text=metadata_dict.get('context', ''),
                        timestamp=metadata_dict['stored_at'],
                        user_id=metadata_dict.get('user_id', 'pipeline@system'),
                        source_system='graph_pipeline'
                    )
                    
                    task = pinecone.upsert_vector(
                        vector_id=vector_id,
                        embedding=embedding,
                        metadata=eq_metadata,
                        namespace=namespace
                    )
                    tasks.append(task)
                
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            failed += 1
                            logger.error(f"Pinecone upsert failed: {result}")
                        elif result:
                            successful += 1
                        else:
                            failed += 1
                    
                    logger.info(f"Pinecone storage complete: {successful} successful, {failed} failed")
                    return failed == 0
                else:
                    logger.warning("No valid embeddings to store in Pinecone")
                    return True
            
            return asyncio.run(store_vectors())
            
        except Exception as e:
            logger.error(f"Failed to store embeddings in Pinecone: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _load_embeddings_from_pinecone(self, pinecone, namespace: str = "default",
                                       filter_dict: Optional[Dict] = None,
                                       top_k: int = 1000) -> Optional[pd.DataFrame]:
        """
        Load embeddings from Pinecone
        
        Args:
            pinecone: Pinecone adapter instance
            namespace: Namespace to query
            filter_dict: Optional metadata filters
            top_k: Maximum number of vectors to retrieve
            
        Returns:
            DataFrame with columns: id, embedding, and metadata fields
        """
        try:
            if filter_dict is None:
                filter_dict = {}
            
            dimension = 3072  # Standard dimension for noderag
            query_vector = np.random.randn(dimension).tolist()
            
            async def search_vectors():
                """Async function to search vectors"""
                results = await pinecone.search(
                    embedding=query_vector,
                    filter=filter_dict,
                    top_k=top_k,
                    namespace=namespace,
                    include_values=True,  # Include the actual vectors
                    include_metadata=True  # Include metadata
                )
                return results
            
            search_results = asyncio.run(search_vectors())
            
            if not search_results:
                logger.warning(f"No embeddings found in Pinecone namespace {namespace}")
                return pd.DataFrame()
            
            data = []
            for result in search_results:
                row = {
                    'id': result.get('id'),
                    'embedding': result.get('values', []),
                    'score': result.get('score', 0.0)
                }
                
                metadata = result.get('metadata', {})
                for key, value in metadata.items():
                    if key not in ['id', 'embedding', 'score']:
                        row[key] = value
                
                data.append(row)
            
            df = pd.DataFrame(data)
            logger.info(f"Loaded {len(df)} embeddings from Pinecone namespace {namespace}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load embeddings from Pinecone: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

class StorageFactoryWrapper:
    """Wrapper that maintains storage() API while using StorageFactory"""
    
    def __init__(self, content: Any, adapter: Optional[PipelineStorageAdapter] = None):
        self.content = content
        self.adapter = adapter or PipelineStorageAdapter()
    
    def save_pickle(self, path: str, component_type: str = 'graph', tenant_id: str = 'default') -> None:
        self.adapter.save_pickle(self.content, path, component_type, tenant_id)
    
    def save_parquet(self, path: str, append: bool = False, component_type: str = 'data', 
                     namespace: str = 'default') -> None:
        if isinstance(self.content, list):
            df = pd.DataFrame(self.content)
        elif isinstance(self.content, dict):
            df = pd.DataFrame([self.content])
        else:
            df = self.content
        self.adapter.save_parquet(df, path, component_type, append, namespace)
    
    def save_json(self, path: str, append: bool = False) -> None:
        self.adapter.save_json(self.content, path)

def storage_factory_wrapper(content):
    """Enhanced storage wrapper that uses StorageFactory when available"""
    try:
        adapter = PipelineStorageAdapter()
        return StorageFactoryWrapper(content, adapter)
    except:
        return storage(content)
