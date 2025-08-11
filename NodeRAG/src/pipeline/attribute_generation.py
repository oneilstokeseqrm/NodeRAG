import networkx as nx
import numpy as np
import math
import asyncio
import os
from sortedcontainers import SortedDict
from rich.console import Console


from ...storage import (
    Mapper,
    storage
)
from ..component import Attribute
from ...config import NodeConfig
from ...logging import info_timer
from ...storage.storage_factory import StorageFactory
from ...tenant.tenant_context import TenantContext
from ...standards.eq_metadata import EQMetadata
from datetime import datetime, timezone
import uuid



class NodeImportance:
    
    def __init__(self,graph:nx.Graph,console:Console):
        self.G = graph
        self.important_nodes = []
        self.console = console
        
    def K_core(self,k:int|None = None):
        
        if k is None:
            k = self.defult_k()
        
        self.k_subgraph = nx.core.k_core(self.G,k=k)
        
        for nodes in self.k_subgraph.nodes():
            if self.G[nodes]['type'] == 'entity' and self.G[nodes]['weight'] > 1:
                self.important_nodes.append(nodes)
        
    def avarege_degree(self):
        average_degree = sum(dict(self.G.degree()).values())/self.G.number_of_nodes()
        return average_degree
    
    def defult_k(self):
        k = round(np.log(self.G.number_of_nodes())*self.avarege_degree()**(1/2))
        return k
    
    def betweenness_centrality(self):
        
        self.betweenness = nx.betweenness_centrality(self.G,k=10)
        average_betweenness = sum(self.betweenness.values())/len(self.betweenness)
        scale = round(math.log10(len(self.betweenness)))
        
        for node in self.betweenness:
            if self.betweenness[node] > average_betweenness*scale:
                if self.G.nodes[node]['type'] == 'entity' and self.G.nodes[node]['weight'] > 1:
                    self.important_nodes.append(node)
                    
    def main(self):
        self.K_core()
        self.console.print('[bold green]K_core done[/bold green]')
        self.betweenness_centrality()
        self.console.print('[bold green]Betweenness done[/bold green]')
        self.important_nodes = list(set(self.important_nodes))
        return self.important_nodes
        
        
        
class Attribution_generation_pipeline:
            
    def __init__(self,config:NodeConfig):
        

        self.config = config
        self.prompt_manager = config.prompt_manager
        self.indices = config.indices
        self.console = config.console
        self.API_client = config.API_client
        self.token_counter = config.token_counter
        self.important_nodes = []
        self.attributes = []
        
        
        self.mapper = Mapper([self.config.entities_path,self.config.relationship_path,self.config.semantic_units_path])
        
        factory = StorageFactory()
        if factory.is_cloud_storage():
            tenant_id = TenantContext.get_current_tenant_or_default()
            neo4j_adapter = factory.get_graph_storage()
            
            subgraph_data = neo4j_adapter.get_subgraph(tenant_id)
            
            if subgraph_data:
                self.G = nx.Graph()
                
                for node in subgraph_data.get('nodes', []):
                    node_id = node.get('node_id')
                    if node_id:
                        node_attrs = {k: v for k, v in node.items() if k != 'node_id'}
                        self.G.add_node(node_id, **node_attrs)
                
                for rel in subgraph_data.get('relationships', []):
                    source = rel.get('source_id')
                    target = rel.get('target_id')
                    if source and target:
                        edge_attrs = {k: v for k, v in rel.items() 
                                    if k not in ['source_id', 'target_id']}
                        self.G.add_edge(source, target, **edge_attrs)
                
                self.console.print(f'[bold green]Loaded graph from Neo4j: {self.G.number_of_nodes()} nodes, {self.G.number_of_edges()} edges[/bold green]')
            else:
                self.G = nx.Graph()
                self.console.print('[bold yellow]No graph data found in Neo4j, starting with empty graph[/bold yellow]')
        else:
            if os.path.exists(self.config.graph_path):
                self.G = storage.load(self.config.graph_path)
                self.console.print(f'[bold green]Loaded graph from file: {self.G.number_of_nodes()} nodes[/bold green]')
            else:
                self.G = nx.Graph()
                self.console.print('[bold yellow]No graph file found, starting with empty graph[/bold yellow]')
        
    def get_important_nodes(self):
        
        node_importance = NodeImportance(self.G,self.config.console)
        important_nodes = node_importance.main()
        
        if os.path.exists(self.config.attributes_path):
            attributes = storage.load(self.config.attributes_path)
            existing_nodes = attributes['node'].tolist()
            important_nodes = [node for node in important_nodes if node not in existing_nodes]
        
        self.important_nodes = important_nodes
        self.console.print('[bold green]Important nodes found[/bold green]')
    
    def get_neighbours_material(self,node:str):
       
        entity = self.mapper.get(node,'context')
        semantic_neighbours = ''+'\n'
        relationship_neighbours = ''+'\n'
       
        for neighbour in self.G.neighbors(node):
            if self.G.nodes[neighbour]['type'] == 'semantic_unit':
                semantic_neighbours += f'{self.mapper.get(neighbour,"context")}\n'
            elif self.G.nodes[neighbour]['type'] == 'relationship':
                relationship_neighbours += f'{self.mapper.get(neighbour,"context")}\n'
       
        query = self.prompt_manager.attribute_generation.format(entity = entity,semantic_units = semantic_neighbours,relationships = relationship_neighbours)
        return query
    
    
    def get_important_neibours_material(self,node:str):
        
        entity = self.mapper.get(node,'context')
        semantic_neighbours = ''+'\n'
        relationship_neighbours = ''+'\n'
        sorted_neighbours = SortedDict()
        
        for neighbour in self.G.neighbors(node):
            value = 0
            for neighbour_neighbour in self.G.neighbors(neighbour):
                value += self.G.nodes[neighbour_neighbour]['weight']
            sorted_neighbours[neighbour] = value
        
        query = ''
        for neighbour in reversed(sorted_neighbours):
            while not self.token_counter.token_limit(query):
                query = self.prompt_manager.attribute_generation.format(entity = entity,semantic_units = semantic_neighbours,relationships = relationship_neighbours)
                if self.G.nodes[neighbour]['type'] == 'semantic_unit':
                    semantic_neighbours += f'{self.mapper.get(neighbour,"context")}\n'
                elif self.G.nodes[neighbour]['type'] == 'relationship':
                    relationship_neighbours += f'{self.mapper.get(neighbour,"context")}\n'
        
        return query
    
    async def generate_attribution_main(self):
        
        tasks = []
        self.config.tracker.set(len(self.important_nodes),desc="Generating attributes")
        
        for node in self.important_nodes:
            tasks.append(self.generate_attribution(node))
        
        await asyncio.gather(*tasks)
        
        self.config.tracker.close()
                    
            
            
            
    async def generate_attribution(self,node:str):
        query = self.get_neighbours_material(node)
        
        
        if self.token_counter.token_limit(query):
            query = self.get_important_neibours_material(node)
            
        response = await self.API_client({'query':query})
        if response is not None:
            entity_metadata = None
            if self.G.has_node(node):
                node_data = self.G.nodes[node]
                
                if all(field in node_data for field in ['tenant_id', 'account_id', 'interaction_id', 
                                                        'interaction_type', 'timestamp', 'user_id', 'source_system']):
                    from ...standards.eq_metadata import EQMetadata
                    try:
                        entity_metadata = EQMetadata(
                            tenant_id=node_data['tenant_id'],
                            account_id=node_data['account_id'],
                            interaction_id=node_data['interaction_id'],
                            interaction_type=node_data['interaction_type'],
                            text=f'Attribute for entity {node}',
                            timestamp=node_data['timestamp'],
                            user_id=node_data['user_id'],
                            source_system=node_data['source_system']
                        )
                    except Exception as e:
                        print(f"Warning: Could not create metadata for attribute: {e}")
            
            attribute = Attribute(response, node, metadata=entity_metadata)
            
            self.attributes.append(attribute)
            self.G.nodes[node]['attributes'] = [attribute.hash_id]
            
            if entity_metadata:
                node_attrs = {
                    'type': 'attribute',
                    'weight': 1,
                    'tenant_id': entity_metadata.tenant_id,
                    'account_id': entity_metadata.account_id,
                    'interaction_id': entity_metadata.interaction_id,
                    'interaction_type': entity_metadata.interaction_type,
                    'timestamp': entity_metadata.timestamp,
                    'user_id': entity_metadata.user_id,
                    'source_system': entity_metadata.source_system
                }
                self.G.add_node(attribute.hash_id, **node_attrs)
            else:
                print(f"Warning: No metadata found for entity {node}, creating attribute without metadata")
                self.G.add_node(attribute.hash_id, type='attribute', weight=1)
                
            self.G.add_edge(node,attribute.hash_id,weight=1)
        self.config.tracker.update()

    def save_attributes(self):
        """Store attributes to Neo4j or file storage based on backend"""
        
        attributes = []
        
        for attribute in self.attributes:
            attributes.append({'node':attribute.node,
                               'type':'attribute',
                                 'context':attribute.raw_context,
                                 'hash_id':attribute.hash_id,
                                 'human_readable_id':attribute.human_readable_id,
                                 'weight':self.G.nodes[attribute.node]['weight'] if self.G.has_node(attribute.node) else 1,
                                 'embedding':None})
        
        factory = StorageFactory()
        if factory.is_cloud_storage():
            neo4j_adapter = factory.get_graph_storage()
            tenant_id = TenantContext.get_current_tenant_or_default()
            stored_count = 0
            failed_count = 0
            
            default_metadata = EQMetadata(
                tenant_id=tenant_id,
                account_id=f"attribute_pipeline_{tenant_id}",
                interaction_id=f"attribute_{uuid.uuid4().hex[:8]}",
                interaction_type='attribute',
                text='',
                timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                user_id='system',
                source_system='attribute_pipeline'
            )
            
            for attr in attributes:
                if self.G.has_node(attr['hash_id']):
                    attr_node_data = self.G.nodes[attr['hash_id']]
                    
                    if all(field in attr_node_data for field in ['tenant_id', 'account_id', 'interaction_id']):
                        attr_metadata = EQMetadata(
                            tenant_id=attr_node_data.get('tenant_id', tenant_id),
                            account_id=attr_node_data.get('account_id', default_metadata.account_id),
                            interaction_id=attr_node_data.get('interaction_id', default_metadata.interaction_id),
                            interaction_type=attr_node_data.get('interaction_type', 'attribute'),
                            text=attr['context'][:500] if attr.get('context') else '',  # Include some context
                            timestamp=attr_node_data.get('timestamp', default_metadata.timestamp),
                            user_id=attr_node_data.get('user_id', default_metadata.user_id),
                            source_system=attr_node_data.get('source_system', default_metadata.source_system)
                        )
                    else:
                        attr_metadata = default_metadata
                        self.config.console.print(f'[yellow]Warning: Attribute {attr["hash_id"]} missing metadata, using defaults[/yellow]')
                    
                    success = neo4j_adapter.add_node(
                        node_id=attr['hash_id'],
                        node_type='attribute',
                        metadata=attr_metadata,
                        properties={
                            'context': attr['context'],
                            'node': attr['node'],  # Reference to parent entity
                            'human_readable_id': attr['human_readable_id'],
                            'weight': attr['weight']
                        }
                    )
                    
                    if success:
                        stored_count += 1
                    else:
                        failed_count += 1
                        self.config.console.print(f'[red]Failed to store attribute {attr["hash_id"]} to Neo4j[/red]')
                else:
                    self.config.console.print(f'[red]Warning: Attribute {attr["hash_id"]} not found in graph[/red]')
                    failed_count += 1
            
            if failed_count > 0:
                self.config.console.print(f'[bold yellow]⚠ Attributes storage partial: {stored_count} stored, {failed_count} failed[/bold yellow]')
            else:
                self.config.console.print(f'[bold green]✅ All {stored_count} attributes stored to Neo4j successfully[/bold green]')
            
            if attributes:
                from .storage_adapter import storage_factory_wrapper
                storage_factory_wrapper(attributes).save_parquet(
                    self.config.attributes_path,
                    append=os.path.exists(self.config.attributes_path), 
                    component_type='data'
                )
        else:
            from .storage_adapter import storage_factory_wrapper
            storage_factory_wrapper(attributes).save_parquet(
                self.config.attributes_path,
                append=os.path.exists(self.config.attributes_path), 
                component_type='data'
            )
            self.config.console.print('[bold green]Attributes stored to file[/bold green]')
        
        
    def save_graph(self):
        """Store graph to Neo4j or file storage based on backend"""
        
        factory = StorageFactory()
        if factory.is_cloud_storage():
            neo4j_adapter = factory.get_graph_storage()
            tenant_id = TenantContext.get_current_tenant_or_default()
            
            storage_metadata = EQMetadata(
                tenant_id=tenant_id,
                account_id=f"attribute_pipeline_{tenant_id}",
                interaction_id=f"attribute_{uuid.uuid4().hex[:8]}",
                interaction_type='attribute_generation',
                text='Graph storage from attribute pipeline',
                timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                user_id='system',
                source_system='attribute_pipeline'
            )
            
            nodes_to_store = []
            edges_to_store = []
            
            attr_count = 0
            for node_id, node_data in self.G.nodes(data=True):
                if node_data.get('type') == 'attribute':
                    attr_count += 1
                    if all(field in node_data for field in ['tenant_id', 'account_id', 'interaction_id']):
                        node_metadata = EQMetadata(
                            tenant_id=node_data['tenant_id'],
                            account_id=node_data['account_id'],
                            interaction_id=node_data['interaction_id'],
                            interaction_type=node_data.get('interaction_type', 'attribute'),
                            text=f'Attribute for entity {node_data.get("entity", "")}',
                            timestamp=node_data.get('timestamp', storage_metadata.timestamp),
                            user_id=node_data.get('user_id', 'system'),
                            source_system=node_data.get('source_system', 'internal')
                        )
                    else:
                        node_metadata = storage_metadata
                else:
                    if 'tenant_id' in node_data:
                        node_metadata = EQMetadata(
                            tenant_id=node_data.get('tenant_id', tenant_id),
                            account_id=node_data.get('account_id', storage_metadata.account_id),
                            interaction_id=node_data.get('interaction_id', storage_metadata.interaction_id),
                            interaction_type=node_data.get('interaction_type', 'entity'),
                            text='',
                            timestamp=node_data.get('timestamp', storage_metadata.timestamp),
                            user_id=node_data.get('user_id', storage_metadata.user_id),
                            source_system=node_data.get('source_system', storage_metadata.source_system)
                        )
                    else:
                        node_metadata = storage_metadata
                
                nodes_to_store.append({
                    'node_id': str(node_id),
                    'node_type': node_data.get('type', 'entity'),
                    'metadata': node_metadata,
                    'properties': {k: v for k, v in node_data.items() 
                                 if k not in ['tenant_id', 'account_id', 'interaction_id', 
                                             'interaction_type', 'timestamp', 'user_id', 'source_system']}
                })
            
            node_count = 0
            node_failures = []
            for node_spec in nodes_to_store:
                try:
                    success = neo4j_adapter.add_node(**node_spec)
                    if success:
                        node_count += 1
                    else:
                        node_failures.append(node_spec['node_id'])
                except Exception as e:
                    self.config.console.print(f'[red]Error storing node {node_spec["node_id"]}: {e}[/red]')
                    node_failures.append(node_spec['node_id'])
            
            for source, target, edge_data in self.G.edges(data=True):
                source_data = self.G.nodes[source]
                if 'tenant_id' in source_data:
                    edge_metadata = EQMetadata(
                        tenant_id=source_data.get('tenant_id', tenant_id),
                        account_id=source_data.get('account_id', storage_metadata.account_id),
                        interaction_id=source_data.get('interaction_id', storage_metadata.interaction_id),
                        interaction_type=source_data.get('interaction_type', 'attribute'),
                        text='',
                        timestamp=source_data.get('timestamp', storage_metadata.timestamp),
                        user_id=source_data.get('user_id', storage_metadata.user_id),
                        source_system=source_data.get('source_system', storage_metadata.source_system)
                    )
                else:
                    edge_metadata = storage_metadata
                
                source_type = source_data.get('type', 'unknown')
                target_type = self.G.nodes[target].get('type', 'unknown') if self.G.has_node(target) else 'unknown'
                
                if 'type' in edge_data:
                    rel_type = edge_data['type']
                elif source_type == 'entity' and target_type == 'attribute':
                    rel_type = 'has_attribute'
                elif source_type == 'attribute' and target_type == 'entity':
                    rel_type = 'attribute_of'
                elif source_type == 'entity' and target_type == 'entity':
                    rel_type = 'relates_to'
                elif source_type == 'semantic_unit':
                    rel_type = 'contains'
                else:
                    rel_type = 'connected_to'  # Generic fallback
                
                if source_type == 'attribute' or target_type == 'attribute':
                    self.config.console.print(f'[dim]Edge: {source}({source_type}) -> {target}({target_type}) = {rel_type}[/dim]')
                
                edges_to_store.append({
                    'source_id': str(source),
                    'target_id': str(target),
                    'relationship_type': rel_type,
                    'metadata': edge_metadata,
                    'properties': {k: v for k, v in edge_data.items() if k != 'type'}
                })
            
            edge_count = 0
            edge_failures = []
            for edge_spec in edges_to_store:
                try:
                    success = neo4j_adapter.add_relationship(**edge_spec)
                    if success:
                        edge_count += 1
                    else:
                        edge_failures.append(f"{edge_spec['source_id']}->{edge_spec['target_id']}")
                except Exception as e:
                    self.config.console.print(f'[red]Error storing edge: {e}[/red]')
                    edge_failures.append(f"{edge_spec['source_id']}->{edge_spec['target_id']}")
            
            if node_failures or edge_failures:
                self.config.console.print(f'[bold yellow]⚠ Graph storage partial: {node_count} nodes, {edge_count} edges stored[/bold yellow]')
                if node_failures:
                    self.config.console.print(f'[yellow]Failed nodes: {node_failures[:5]}{"..." if len(node_failures) > 5 else ""}[/yellow]')
                if edge_failures:
                    self.config.console.print(f'[yellow]Failed edges: {edge_failures[:5]}{"..." if len(edge_failures) > 5 else ""}[/yellow]')
            else:
                self.config.console.print(f'[bold green]✅ Graph stored to Neo4j: {node_count} nodes ({attr_count} attributes), {edge_count} edges[/bold green]')
        else:
            from .storage_adapter import storage_factory_wrapper
            storage_factory_wrapper(self.G).save_pickle(self.config.graph_path, component_type='graph')
            self.config.console.print('[bold green]Graph stored to file[/bold green]')
        
    @info_timer(message='Attribute Generation')
    async def main(self):
        
        if os.path.exists(self.config.graph_path):
            
            self.get_important_nodes()
            await self.generate_attribution_main()
            self.save_attributes()
            self.save_graph()
            self.indices.store_all_indices(self.config.indices_path)
            

        
                               
        
        
            
                
                
        
        