import os
import json
from ...storage import storage
from ...config import NodeConfig
from ...utils import MultigraphConcat
from ...logging import info_timer



class Insert_text:
    
    def __init__(self,config:NodeConfig):
        self.config = config
        self.G = storage.load(self.config.graph_path)
        self.base_G = self.load_base_graph(self.config.base_graph_path)
        self.semantic_units = storage.load(self.config.semantic_units_path)

    def insert_text(self):
        text_metadata_map = {}
        if os.path.exists(self.config.text_decomposition_path):
            with open(self.config.text_decomposition_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        if 'metadata' in data and data['metadata']:
                            text_hash_id = data.get('text_hash_id')
                            if text_hash_id:
                                text_metadata_map[text_hash_id] = data['metadata']
                    except json.JSONDecodeError:
                        continue
        
        self.config.tracker.set(len(self.semantic_units),'Inserting text')
        for id,row in self.semantic_units.iterrows():
            if row['insert'] is None:
                semantic_unit_hash_id = row['hash_id']
                text_unit_hash_id = row['text_hash_id']
                
                if not self.G.has_node(text_unit_hash_id):
                    metadata_dict = text_metadata_map.get(text_unit_hash_id)
                    
                    if metadata_dict:
                        node_attrs = {
                            'type': 'text',
                            'weight': 1,
                            'tenant_id': metadata_dict.get('tenant_id'),
                            'account_id': metadata_dict.get('account_id'),
                            'interaction_id': metadata_dict.get('interaction_id'),
                            'interaction_type': metadata_dict.get('interaction_type'),
                            'timestamp': metadata_dict.get('timestamp'),
                            'user_id': metadata_dict.get('user_id'),
                            'source_system': metadata_dict.get('source_system')
                        }
                        self.G.add_node(text_unit_hash_id, **node_attrs)
                    else:
                        print(f"Warning: No metadata found for text unit {text_unit_hash_id}")
                        self.G.add_node(text_unit_hash_id, type='text', weight=1)
                        
                if not self.G.has_edge(semantic_unit_hash_id,text_unit_hash_id):
                    self.G.add_edge(semantic_unit_hash_id,text_unit_hash_id,type='text',weight=1)
                self.semantic_units.at[id,'insert'] = True
            self.config.tracker.update()
        self.config.tracker.close()
        from .storage_adapter import storage_factory_wrapper
        storage_factory_wrapper(self.semantic_units).save_parquet(self.config.semantic_units_path, component_type='data')
    
    def concatenate_graph(self):
        
        self.base_G = MultigraphConcat(self.base_G).concat(self.G)
        from .storage_adapter import storage_factory_wrapper
        storage_factory_wrapper(self.base_G).save_pickle(self.config.base_graph_path, component_type='graph')
        os.remove(self.config.graph_path)
        self.config.console.print('[bold green]Graph has been concatenated, stored in base graph[/bold green]')
    
    def load_base_graph(self,base_graph_path:str):
        if os.path.exists(base_graph_path):
            return storage.load(base_graph_path)
        else:
            return None
    @info_timer(message="Insert text and concatenate graph")
    async def main(self):
        if os.path.exists(self.config.graph_path):
            self.insert_text()
            self.concatenate_graph()
        