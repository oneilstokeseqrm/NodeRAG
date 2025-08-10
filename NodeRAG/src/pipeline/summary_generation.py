import leidenalg as la
import os
import json
import asyncio
import faiss
import math
import numpy as np
from datetime import datetime, timezone

from ...storage import (
    Mapper,
    storage
)

from ..component import (
    Community_summary,
    High_level_elements
)
from ...config import NodeConfig
from ...standards.eq_metadata import EQMetadata

from ...utils import (
    IGraph,
)

from ...logging import info_timer

class SummaryGeneration:
    
    def __init__(self,config:NodeConfig):
        
        self.config = config
        self.indices = self.config.indices
        self.communities = []
        self.high_level_elements = []

        from ...storage.storage_factory import StorageFactory
        from ...tenant.tenant_context import TenantContext
        
        factory = StorageFactory()
        if factory.is_cloud_storage():
            tenant_id = TenantContext.get_current_tenant_or_default()
            neo4j_adapter = factory.get_graph_storage()
            
            subgraph_data = neo4j_adapter.get_subgraph(tenant_id)
            
            if subgraph_data:
                import networkx as nx
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
            else:
                import networkx as nx
                self.G = nx.Graph()
        else:
            if os.path.exists(self.config.graph_path):
                self.G = storage.load_pickle(self.config.graph_path)
            else:
                import networkx as nx
                self.G = nx.Graph()

        if os.path.exists(self.config.graph_path):
            self.mapper = Mapper([self.config.semantic_units_path,
                                  self.config.attributes_path])
            self.mapper.add_embedding(self.config.embedding)
            
            import networkx as nx
            if not isinstance(self.G, nx.Graph):
                raise TypeError(f"Expected networkx.Graph, got {type(self.G)}")
            
            print(f"Loaded graph with {self.G.number_of_nodes()} nodes and {self.G.number_of_edges()} edges")
            self.G_ig = IGraph(self.G).to_igraph()
            self.nodes_high_level_elements_group = []
            self.nodes_high_level_elements_match = []

        

    
    def partition(self):
        
        partition = la.find_partition(self.G_ig,la.ModularityVertexPartition)
        
        for i,community in enumerate(partition):
            community_name = [self.G_ig.vs[node]['name'] for node in community if self.G_ig.vs[node]['name'] in self.mapper.embeddings]
            community_node = community_name[0] if community_name else None
            self.communities.append(Community_summary(community_node,self.mapper,self.G,self.config))
    
    def _extract_metadata_from_community(self, node_names: list[str]) -> EQMetadata:
        """Extract metadata from community member nodes for high_level_elements
        
        Uses AGGREGATED tenant_id for cross-tenant summaries when nodes span multiple tenants
        """
        print(f"Extracting metadata from community of {len(node_names)} nodes")
        
        tenant_ids = set()
        valid_metadata_node = None
        
        for node_name in node_names:
            if self.G.has_node(node_name):
                node_data = self.G.nodes[node_name]
                if 'tenant_id' in node_data:
                    tenant_ids.add(node_data['tenant_id'])
                    
                required_fields = ['tenant_id', 'account_id', 'interaction_id', 
                                 'interaction_type', 'timestamp', 'user_id', 'source_system']
                
                if all(field in node_data for field in required_fields) and valid_metadata_node is None:
                    valid_metadata_node = node_data
        
        if len(tenant_ids) > 1:
            print(f"  Cross-tenant summary detected: {tenant_ids}")
            from datetime import datetime, timezone
            return EQMetadata(
                tenant_id='AGGREGATED',
                account_id='AGGREGATED',
                interaction_id='AGGREGATED',
                interaction_type='summary',
                text='',
                timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                user_id='system',
                source_system='internal'
            )
        elif valid_metadata_node:
            print(f"  Using single-tenant metadata: tenant_id={valid_metadata_node['tenant_id']}")
            return EQMetadata(
                tenant_id=valid_metadata_node['tenant_id'],
                account_id=valid_metadata_node['account_id'],
                interaction_id=valid_metadata_node['interaction_id'],
                interaction_type=valid_metadata_node['interaction_type'],
                text='',
                timestamp=valid_metadata_node['timestamp'],
                        user_id=node_data['user_id'],
                        source_system=node_data['source_system']
                    )
        
        print(f"  No valid metadata found, using AGGREGATED fallback")
        return EQMetadata(
            tenant_id='AGGREGATED',
            account_id='AGGREGATED', 
            interaction_id='AGGREGATED',
            interaction_type='summary',
            text='',
            timestamp=datetime.now(timezone.utc).isoformat(),
            user_id='system',
            source_system='internal'
        )
            
    async def generate_community_summary(self,community:Community_summary):
        
        await community.generate_community_summary()
        if isinstance(community.response,str):
            self.config.tracker.update()
            return
        
        community_dict = {'community':community.community_node,
                          'response':community.response,
                          'hash_id':community.hash_id,
                          'human_readable_id':community.human_readable_id}
        
        with open(self.config.summary_path,'a',encoding='utf-8') as f:
            f.write(json.dumps(community_dict,ensure_ascii=False)+'\n')
        
        self.config.tracker.update()
        
        
            
    async def generate_high_level_element_summary(self):
        
        self.partition()
        
        tasks = []
        
        self.config.tracker.set(len(self.communities),'Community Summary')
        for community in self.communities:
            tasks.append(self.generate_community_summary(community))
        
        await asyncio.gather(*tasks)
        
        self.config.tracker.close()
       
        
    async def get_summary_embedding(self):
        tasks = []
        self.config.tracker.set(math.ceil(len(self.high_level_elements)/self.config.embedding_batch_size),'High Level Element Embedding')
        
        for i in range(0,len(self.high_level_elements),self.config.embedding_batch_size):
            high_level_element_batch = self.high_level_elements[i:i+self.config.embedding_batch_size]
            tasks.append(self.embedding_store(high_level_element_batch))
        await asyncio.gather(*tasks)
        self.config.tracker.close()
        
    async def embedding_store(self,high_level_element_batch:list[High_level_elements]):
        
        context = [high_level_element.context for high_level_element in high_level_element_batch]
        embedding = await self.config.embedding_client(context)
        
        for i in range(len(high_level_element_batch)):
            high_level_element_batch[i].store_embedding(embedding[i])
        self.config.tracker.update()

   
    async def high_level_element_summary(self):
        results = []
        
        with open(self.config.summary_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = json.loads(line)
                results.append(line)
                
        All_nodes = []
        self.config.tracker.set(len(results),'High Level Element Summary')
        for result in results:
            high_level_elements = []
            node_names = result['community']
            for high_level_element in  result['response']['high_level_elements']:
                he = High_level_elements(high_level_element['description'],high_level_element['title'],self.config)
                he.related_node(node_names)
                if self.G.has_node(he.hash_id):
                    self.G.nodes[he.hash_id]['weight'] += 1
                    if self.G.has_node(he.title_hash_id):
                        self.G.nodes[he.title_hash_id]['weight'] += 1
                    else:
                        continue
                    
                else:
                    metadata = self._extract_metadata_from_community(node_names)
                    
                    print(f"Creating high_level_element node {he.hash_id[:20]}... with metadata:")
                    print(f"  tenant_id: {metadata.tenant_id}")
                    print(f"  source: {'extracted' if metadata.tenant_id != 'AGGREGATED' else 'fallback'}")
                    
                    node_attrs = {
                        'type': 'high_level_element', 
                        'weight': 1,
                        'tenant_id': metadata.tenant_id,
                        'account_id': metadata.account_id,
                        'interaction_id': metadata.interaction_id,
                        'interaction_type': metadata.interaction_type,
                        'timestamp': metadata.timestamp,
                        'user_id': metadata.user_id,
                        'source_system': metadata.source_system
                    }
                    self.G.add_node(he.hash_id, **node_attrs)
                    
                    title_attrs = {
                        'type': 'high_level_element_title', 
                        'weight': 1, 
                        'related_node': he.hash_id,
                        'tenant_id': metadata.tenant_id,
                        'account_id': metadata.account_id,
                        'interaction_id': metadata.interaction_id,
                        'interaction_type': metadata.interaction_type,
                        'timestamp': metadata.timestamp,
                        'user_id': metadata.user_id,
                        'source_system': metadata.source_system
                    }
                    self.G.add_node(he.title_hash_id, **title_attrs)
                    print(f"Created title node {he.title_hash_id[:20]}... with same metadata")
                    high_level_elements.append(he)
                
                edge = (he.hash_id,he.title_hash_id)
                
                if not self.G.has_edge(*edge):
                    self.G.add_edge(*edge,weight=1)
            
            All_nodes.extend(node_names)
            self.high_level_elements.extend(high_level_elements)
            self.config.tracker.update()
        self.config.tracker.close()
        await self.get_summary_embedding()

        
        centroids = math.ceil(math.sqrt(len(All_nodes)+len(self.high_level_elements)))
        threshold = (len(All_nodes)+len(self.high_level_elements))/centroids
        n=0
        if threshold > self.config.Hcluster_size:
            embedding_list = np.array([self.mapper.embeddings[node] for node in All_nodes], dtype=np.float32)
            high_level_element_embedding = np.array([he.embedding for he in self.high_level_elements], dtype=np.float32)
            all_embeddings = np.vstack([high_level_element_embedding, embedding_list])

            kmeans = faiss.Kmeans(d=all_embeddings.shape[1], k=centroids)
            kmeans.train(all_embeddings.astype(np.float32))
            _, cluster_labels = kmeans.assign(all_embeddings.astype(np.float32))
            high_level_element_cluster_labels = cluster_labels[:len(self.high_level_elements)]
            embedding_cluster_labels = cluster_labels[len(self.high_level_elements):]
            self.config.console.print(f'[bold green]KMeans Clustering with {centroids} centroids[/bold green]')
        
            self.config.tracker.set(len(self.high_level_elements),'Adding High Level Element Summary')
            for i in range(len(self.high_level_elements)):
                for j in range(len(All_nodes)):
                    if high_level_element_cluster_labels[i] == embedding_cluster_labels[j] and All_nodes[j] in self.high_level_elements[i].related_node:
                        self.G.add_edge(All_nodes[j],self.high_level_elements[i].hash_id,weight=1)
                        n+=1
                self.config.tracker.update()
           


        else:
            self.config.tracker.set(len(self.high_level_elements),'Adding High Level Element Summary')
            for he in self.high_level_elements:
                for node in he.related_node:
                    self.G.add_edge(node,he.hash_id,weight=1)
                    n+=1
                self.config.tracker.update()
        
        self.config.tracker.close()
        self.config.console.print(f'[bold green]Added {n} edges[/bold green]')
        
       
   
            
    
            
        
            
            

                
   
    def store_graph(self):
        """Store graph to Neo4j or file storage based on backend"""
        from ...storage.storage_factory import StorageFactory
        from ...tenant.tenant_context import TenantContext
        from ...standards.eq_metadata import EQMetadata
        from datetime import datetime, timezone
        import uuid
        
        factory = StorageFactory()
        if factory.is_cloud_storage():
            neo4j_adapter = factory.get_graph_storage()
            tenant_id = TenantContext.get_current_tenant_or_default()
            
            storage_metadata = EQMetadata(
                tenant_id=tenant_id,
                account_id=f"summary_pipeline_{tenant_id}",
                interaction_id=f"summary_{uuid.uuid4().hex[:8]}",
                interaction_type='summary_generation',
                text='Graph storage from summary pipeline',
                timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                user_id='system',
                source_system='summary_pipeline'
            )
            
            node_count = 0
            for node_id, node_data in self.G.nodes(data=True):
                if 'tenant_id' in node_data:
                    node_metadata = EQMetadata(
                        tenant_id=node_data.get('tenant_id', tenant_id),
                        account_id=node_data.get('account_id', storage_metadata.account_id),
                        interaction_id=node_data.get('interaction_id', storage_metadata.interaction_id),
                        interaction_type=node_data.get('interaction_type', 'summary'),
                        text='',
                        timestamp=node_data.get('timestamp', storage_metadata.timestamp),
                        user_id=node_data.get('user_id', storage_metadata.user_id),
                        source_system=node_data.get('source_system', storage_metadata.source_system)
                    )
                else:
                    node_metadata = storage_metadata
                
                success = neo4j_adapter.add_node(
                    node_id=str(node_id),
                    node_type=node_data.get('type', 'entity'),
                    metadata=node_metadata,
                    properties={k: v for k, v in node_data.items() 
                              if k not in ['tenant_id', 'account_id', 'interaction_id', 
                                          'interaction_type', 'timestamp', 'user_id', 'source_system']}
                )
                if success:
                    node_count += 1
            
            edge_count = 0
            for source, target, edge_data in self.G.edges(data=True):
                source_data = self.G.nodes[source]
                if 'tenant_id' in source_data:
                    edge_metadata = EQMetadata(
                        tenant_id=source_data.get('tenant_id', tenant_id),
                        account_id=source_data.get('account_id', storage_metadata.account_id),
                        interaction_id=source_data.get('interaction_id', storage_metadata.interaction_id),
                        interaction_type=source_data.get('interaction_type', 'summary'),
                        text='',
                        timestamp=source_data.get('timestamp', storage_metadata.timestamp),
                        user_id=source_data.get('user_id', storage_metadata.user_id),
                        source_system=source_data.get('source_system', storage_metadata.source_system)
                    )
                else:
                    edge_metadata = storage_metadata
                
                success = neo4j_adapter.add_relationship(
                    source_id=str(source),
                    target_id=str(target),
                    relationship_type=edge_data.get('type', 'relates_to'),
                    metadata=edge_metadata,
                    properties={k: v for k, v in edge_data.items() if k != 'type'}
                )
                if success:
                    edge_count += 1
            
            self.config.console.print(f'[bold green]Graph stored to Neo4j: {node_count} nodes, {edge_count} edges[/bold green]')
        else:
            from .storage_adapter import storage_factory_wrapper
            storage_factory_wrapper(self.G).save_pickle(self.config.graph_path, component_type='graph')
            self.config.console.print('[bold green]Graph stored to file[/bold green]')
        
    def delete_community_cache(self):
        os.remove(self.config.summary_path)
        
    def store_high_level_elements(self):
        """Store high-level elements to Neo4j or file storage"""
        from ...storage.storage_factory import StorageFactory
        from ...tenant.tenant_context import TenantContext
        from ...standards.eq_metadata import EQMetadata
        from datetime import datetime, timezone
        
        high_level_elements = []
        titles = []
        embedding_list = []
        
        for high_level_element in self.high_level_elements:
            high_level_elements.append({
                'type': 'high_level_element',
                'title_hash_id': high_level_element.title_hash_id,
                'context': high_level_element.context,
                'hash_id': high_level_element.hash_id,
                'human_readable_id': high_level_element.human_readable_id,
                'related_nodes': list(self.G.neighbors(high_level_element.hash_id)),
                'embedding': 'done'
            })
            
            titles.append({
                'type': 'high_level_element_title',
                'hash_id': high_level_element.title_hash_id,
                'context': high_level_element.title,
                'human_readable_id': high_level_element.human_readable_id
            })
            
            embedding_list.append({
                'hash_id': high_level_element.hash_id,
                'embedding': high_level_element.embedding
            })
        
        G_high_level_elements = [node for node in self.G.nodes 
                                if self.G.nodes[node].get('type') == 'high_level_element']
        assert len(high_level_elements) == len(G_high_level_elements), \
            f"Count mismatch: {len(high_level_elements)} != {len(G_high_level_elements)}"
        
        factory = StorageFactory()
        if factory.is_cloud_storage():
            neo4j_adapter = factory.get_graph_storage()
            tenant_id = TenantContext.get_current_tenant_or_default()
            
            stored_count = 0
            for he in high_level_elements:
                if self.G.has_node(he['hash_id']):
                    node_data = self.G.nodes[he['hash_id']]
                    
                    metadata = EQMetadata(
                        tenant_id=node_data.get('tenant_id', 'AGGREGATED'),
                        account_id=node_data.get('account_id', 'AGGREGATED'),
                        interaction_id=node_data.get('interaction_id', 'AGGREGATED'),
                        interaction_type=node_data.get('interaction_type', 'summary'),
                        text='',
                        timestamp=node_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        user_id=node_data.get('user_id', 'system'),
                        source_system=node_data.get('source_system', 'internal')
                    )
                    
                    success = neo4j_adapter.add_node(
                        node_id=he['hash_id'],
                        node_type='high_level_element',
                        metadata=metadata,
                        properties={
                            'context': he['context'],
                            'title_hash_id': he['title_hash_id'],
                            'human_readable_id': he['human_readable_id'],
                            'related_nodes': he['related_nodes']
                        }
                    )
                    if success:
                        stored_count += 1
            
            title_count = 0
            for title in titles:
                if self.G.has_node(title['hash_id']):
                    node_data = self.G.nodes[title['hash_id']]
                    
                    metadata = EQMetadata(
                        tenant_id=node_data.get('tenant_id', 'AGGREGATED'),
                        account_id=node_data.get('account_id', 'AGGREGATED'),
                        interaction_id=node_data.get('interaction_id', 'AGGREGATED'),
                        interaction_type='summary',
                        text='',
                        timestamp=node_data.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        user_id=node_data.get('user_id', 'system'),
                        source_system=node_data.get('source_system', 'internal')
                    )
                    
                    success = neo4j_adapter.add_node(
                        node_id=title['hash_id'],
                        node_type='high_level_element_title',
                        metadata=metadata,
                        properties={
                            'context': title['context'],
                            'human_readable_id': title['human_readable_id']
                        }
                    )
                    if success:
                        title_count += 1
            
            if embedding_list:
                from .storage_adapter import storage_factory_wrapper
                storage_factory_wrapper(embedding_list).save_parquet(
                    self.config.embedding, 
                    append=os.path.exists(self.config.embedding), 
                    component_type='embeddings'
                )
            
            self.config.console.print(f'[bold green]High level elements stored to Neo4j: {stored_count} elements, {title_count} titles[/bold green]')
        else:
            from .storage_adapter import storage_factory_wrapper
            storage_factory_wrapper(high_level_elements).save_parquet(
                self.config.high_level_elements_path,
                append=os.path.exists(self.config.high_level_elements_path), 
                component_type='data'
            )
            storage_factory_wrapper(titles).save_parquet(
                self.config.high_level_elements_titles_path,
                append=os.path.exists(self.config.high_level_elements_titles_path), 
                component_type='data'
            )
            storage_factory_wrapper(embedding_list).save_parquet(
                self.config.embedding,
                append=os.path.exists(self.config.embedding), 
                component_type='embeddings'
            )
            self.config.console.print('[bold green]High level elements stored to files[/bold green]')
            
    @info_timer(message='Summary Generation Pipeline')        
    async def main(self):
        if os.path.exists(self.config.graph_path):
            if os.path.exists(self.config.summary_path):
                os.remove(self.config.summary_path)
            await self.generate_high_level_element_summary()
            await self.high_level_element_summary()
            self.store_high_level_elements()
            self.store_graph()
            self.indices.store_all_indices(self.config.indices_path)
            self.delete_community_cache()
            
        
            
            
            
                
                    
            
            
            
    
    
            
             
        
            
            
            
        
       
        

        
