"""
Extended Graph_pipeline that uses StorageFactory for all storage operations
"""
from pathlib import Path
from typing import Any, Optional
import pandas as pd
import networkx as nx
import os

from .graph_pipeline import Graph_pipeline as BaseGraphPipeline
from .storage_adapter import PipelineStorageAdapter, StorageFactoryWrapper
from ...storage.storage import storage

class Graph_pipeline(BaseGraphPipeline):
    """Extended Graph_pipeline with StorageFactory integration"""
    
    def __init__(self, config):
        """Initialize with storage adapter"""
        super().__init__(config)
        
        self.storage_adapter = PipelineStorageAdapter()
        self._setup_storage_integration()
    
    def _setup_storage_integration(self):
        """Setup StorageFactory integration for all storage operations"""
        self._original_storage = storage
    
    def load_graph(self) -> nx.Graph:
        """Load graph using StorageFactory"""
        graph = self.storage_adapter.load_pickle(self.config.graph_path, component_type='graph')
        
        if graph is not None:
            return graph
        
        return nx.Graph()
    
    def save_graph(self):
        """Save graph using StorageFactory"""
        if self.data == []:
            return None
        
        success = self.storage_adapter.save_pickle(self.G, self.config.graph_path, component_type='graph')
        if success:
            self.console.print('[green]Graph stored[/green]')
        else:
            self.console.print('[red]Failed to store graph[/red]')
    
    def save_semantic_units(self):
        """Save semantic units using StorageFactory"""
        semantic_units = []
        
        for semantic_unit in self.semantic_units:
            semantic_units.append({
                'type': 'semantic_unit',
                'context': semantic_unit.raw_context,
                'hash_id': semantic_unit.hash_id,
                'human_readable_id': semantic_unit.human_readable_id,
                'text_hash_id': semantic_unit.text_hash_id,
                'embedding': None
            })
        
        return semantic_units
    
    def save_entities(self):
        """Save entities using StorageFactory"""
        entities = []
        
        for entity in self.entities:
            entities.append({
                'type': 'entity',
                'context': entity.raw_context,
                'hash_id': entity.hash_id,
                'human_readable_id': entity.human_readable_id,
                'weight': self.G.nodes[entity.hash_id]['weight'],
                'embedding': None
            })
        
        return entities
    
    def save_relationships(self):
        """Save relationships using StorageFactory"""
        relationships = []
        
        for relationship in getattr(self, 'relationship', []):
            relationships.append({
                'type': 'relationship',
                'context': relationship.raw_context,
                'hash_id': relationship.hash_id,
                'human_readable_id': relationship.human_readable_id,
                'weight': self.G.nodes[relationship.hash_id]['weight'],
                'embedding': None
            })
        
        return relationships
    
    def load_relationship(self):
        """Load relationships using StorageFactory"""
        df = self.storage_adapter.load_parquet(self.config.relationship_path, component_type='data')
        
        if df is not None:
            from ..component import Relationship
            relationship = [Relationship.from_df_row(row) for row in df.itertuples()]
            relationship_lookup = {rel.hash_id: rel for rel in relationship}
            return relationship, relationship_lookup
        
        return [], {}
    
    def save(self):
        """Save all components using StorageFactory"""
        semantic_units = self.save_semantic_units()
        entities = self.save_entities()
        relationships = self.save_relationships()
        
        semantic_units_df = pd.DataFrame(semantic_units)
        entities_df = pd.DataFrame(entities)
        relationships_df = pd.DataFrame(relationships)
        
        self.storage_adapter.save_parquet(
            semantic_units_df, 
            self.config.semantic_units_path, 
            component_type='data',
            append=os.path.exists(self.config.semantic_units_path)
        )
        
        self.storage_adapter.save_parquet(
            entities_df,
            self.config.entities_path,
            component_type='data',
            append=os.path.exists(self.config.entities_path)
        )
        
        self.storage_adapter.save_parquet(
            relationships_df,
            self.config.relationship_path,
            component_type='data',
            append=os.path.exists(self.config.relationship_path)
        )
        
        self.console.print('[green]Semantic units, entities and relationships stored[/green]')
