from typing import Dict
import os
import asyncio
import json
import math
import time
import uuid
from datetime import datetime, timezone

from ...config import NodeConfig
from ...LLM import Embedding_message

from ...storage import (
    Mapper,
    storage
)
from ...storage.storage_factory import StorageFactory
from ...tenant.tenant_context import TenantContext
from ...standards.eq_metadata import EQMetadata
from ...logging import info_timer

class Embedding_pipeline():

    def __init__(self,config:NodeConfig):
        self.config = config
        self.embedding_client = self.config.embedding_client
        self.mapper = self.load_mapper()
        self.validate_config()
        
        

    def load_mapper(self) -> Mapper:
        mapping_list = [self.config.text_path,
                        self.config.semantic_units_path,
                        self.config.attributes_path]
        mapping_list = [path for path in mapping_list if os.path.exists(path)]
        return Mapper(mapping_list)
    
    async def get_embeddings(self,context_dict:Dict[str,Embedding_message]):
        
        empty_ids = [key for key, value in context_dict.items() if value == ""]
        
        if len(empty_ids) > 0:
            
            context_dict = {key: value for key, value in context_dict.items() if value != ""}
            
            for empty_id in empty_ids:
                self.mapper.delete(empty_id)

        
        embedding_input = list(context_dict.values())
        
        ids = list(context_dict.keys())
        
        embedding_output = await self.embedding_client(embedding_input,cache_path=self.config.LLM_error_cache,meta_data = {'ids':ids})
        
        if embedding_output == 'Error cached':
            return

        
        with open(self.config.embedding_cache,'a',encoding='utf-8') as f:

            for i in range(len(ids)):
                line = {'hash_id':ids[i],'embedding':embedding_output[i]} 
                f.write(json.dumps(line)+'\n')
                
        self.config.tracker.update()
    
    def delete_embedding_cache(self):
        
        if os.path.exists(self.config.embedding_cache):
            os.remove(self.config.embedding_cache)
    
            
            
    async def generate_embeddings(self):
        tasks = []
        none_embedding_ids = self.mapper.find_none_embeddings()
        self.config.tracker.set(math.ceil(len(none_embedding_ids)/self.config.embedding_batch_size),desc='Generating embeddings')
        for i in range(0,len(none_embedding_ids),self.config.embedding_batch_size):
            context_dict = {}
            for id in none_embedding_ids[i:i+self.config.embedding_batch_size]:
                context_dict[id] = self.mapper.get(id,'context')
            tasks.append(self.get_embeddings(context_dict))
        await asyncio.gather(*tasks)
        self.config.tracker.close()
        
    def insert_embeddings(self):
        
        if not os.path.exists(self.config.embedding_cache):
            return None
        
        with open(self.config.embedding_cache,'r',encoding='utf-8') as f:
            lines = []
            for line in f:
                line = json.loads(line.strip())
                if isinstance(line['embedding'],str):
                    continue
                self.mapper.add_attribute(line['hash_id'],'embedding','done')
                lines.append(line)
        
        self._store_embeddings_in_pinecone(lines)
        self.mapper.update_save()
        
    def check_error_cache(self) -> None:
        
            if os.path.exists(self.config.LLM_error_cache):
                num = 0
                
                with open(self.config.LLM_error_cache,'r',encoding='utf-8') as f:
                    for line in f:
                        num += 1
                        
                if num > 0:
                    self.config.console.print(f"[red]LLM Error Detected,There are {num} errors")
                    self.config.console.print("[red]Please check the error log")
                    self.config.console.print("[red]The error cache is named LLM_error.jsonl, stored in the cache folder")
                    self.config.console.print("[red]Please fix the error and run the pipeline again")
                    raise Exception("Error happened in embedding pipeline, Error cached.")
                    
    async def rerun(self):
        
        with open(self.config.LLM_error_cache,'r',encoding='utf-8') as f:
            LLM_store = []
            
            for line in f:
                line = json.loads(line)
                LLM_store.append(line)
        
        tasks = []
        context_dict = {}
        
        self.config.tracker.set(len(LLM_store),desc='Rerun embedding')
        
        for store in LLM_store:
            input_data = store['input_data']
            meta_data = store['meta_data']
            store.pop('input_data')
            store.pop('meta_data')
            tasks.append(self.request_save(input_data,store,self.config))
        
        await asyncio.gather(*tasks)
        self.config.tracker.close()
        self.insert_embeddings()
        self.delete_embedding_cache()
        self.check_error_cache()
        await self.main_async()
        
    async def request_save(self,
                           input_data:Embedding_message,
                           meta_data:Dict,
                           config:NodeConfig) -> None:
        
        response = await config.client(input_data,cache_path=config.LLM_error_cache,meta_data = meta_data)
        
        if response == 'Error cached':
            return
        
        with open(self.config.embedding_cache,'a',encoding='utf-8') as f:
            for i in range(len(meta_data['ids'])):
                line = {'hash_id':meta_data['ids'][i],'embedding':response[i]} 
                f.write(json.dumps(line)+'\n')

    

    def check_embedding_cache(self):
        if os.path.exists(self.config.embedding_cache):
            self.insert_embeddings()
            self.delete_embedding_cache()
            
    def validate_config(self):
        """Validate that required config fields are present"""
        recommended_fields = ['interaction_type', 'source_system']
        missing_fields = []
        
        for field in recommended_fields:
            if not hasattr(self.config, field):
                missing_fields.append(field)
        
        if missing_fields:
            self.config.console.print(f"[yellow]Warning: Config missing fields: {missing_fields}")
            self.config.console.print("[yellow]Using fallback values - this may affect data lineage tracking")
        
        self.config.console.print(f"[blue]Embedding pipeline using:")
        self.config.console.print(f"  interaction_type: {getattr(self.config, 'interaction_type', 'embedding_generation')}")
        self.config.console.print(f"  source_system: {getattr(self.config, 'source_system', 'embedding_pipeline')}")

    def _store_embeddings_in_pinecone(self, lines):
        """Store embeddings directly in Pinecone with tenant isolation"""
        if not lines:
            return
        
        tenant_id = TenantContext.get_current_tenant_or_default()
        namespace = TenantContext.get_tenant_namespace('embeddings')
        
        factory = StorageFactory()
        if not factory.is_cloud_storage():
            from .storage_adapter import storage_factory_wrapper
            storage_factory_wrapper(lines).save_parquet(
                self.config.embedding, 
                append=os.path.exists(self.config.embedding), 
                component_type='embeddings'
            )
            return
        
        interaction_type = getattr(self.config, 'interaction_type', 'embedding_generation')
        source_system = getattr(self.config, 'source_system', 'embedding_pipeline')
        
        account_id = getattr(self.config, 'account_id', None)
        interaction_id = getattr(self.config, 'interaction_id', None)
        user_id = getattr(self.config, 'user_id', None)
        
        if not interaction_id:
            interaction_id = f"embedding_batch_{tenant_id}_{int(time.time())}"
        
        if not user_id:
            user_id = 'system'  # Use a fixed system user, not random
        
        pinecone_adapter = factory.get_embedding_storage()
        
        batch_size = 100
        successful_count = 0
        failed_count = 0
        
        for i in range(0, len(lines), batch_size):
            batch = lines[i:i+batch_size]
            vectors = []
            
            for item in batch:
                vector_id = f"{tenant_id}_embedding_{item['hash_id']}"
                embedding = item['embedding']
                
                if hasattr(embedding, 'tolist'):
                    embedding = embedding.tolist()
                elif not isinstance(embedding, list):
                    embedding = list(embedding)
                
                metadata = EQMetadata(
                    tenant_id=tenant_id,
                    account_id=account_id or f"pipeline_{tenant_id}",  # Deterministic fallback
                    interaction_id=interaction_id,
                    interaction_type=interaction_type,  # From actual config!
                    text='',  # Embeddings don't need text
                    timestamp=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                    user_id=user_id,
                    source_system=source_system  # From actual config!
                )
                
                vectors.append((vector_id, embedding, metadata, None))
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    if hasattr(pinecone_adapter, 'index'):
                        formatted_vectors = []
                        for vector_id, embedding, metadata, _ in vectors:
                            metadata_dict = {
                                'tenant_id': metadata.tenant_id,
                                'account_id': metadata.account_id,
                                'interaction_id': metadata.interaction_id,
                                'interaction_type': metadata.interaction_type,
                                'timestamp': metadata.timestamp,
                                'user_id': metadata.user_id,
                                'source_system': metadata.source_system
                            }
                            formatted_vectors.append({
                                'id': vector_id,
                                'values': embedding,
                                'metadata': metadata_dict
                            })
                        
                        response = pinecone_adapter.index.upsert(
                            vectors=formatted_vectors,
                            namespace=namespace
                        )
                        successful_count += len(formatted_vectors)
                        break
                    else:
                        raise NotImplementedError("PineconeAdapter must provide synchronous upsert")
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        self.config.console.print(f"[red]Failed to store embedding batch after {max_retries} attempts: {e}")
                        failed_count += len(vectors)
        
        self.config.console.print(f"[green]Stored {successful_count} embeddings in Pinecone namespace {namespace}")
        if failed_count > 0:
            self.config.console.print(f"[yellow]Failed to store {failed_count} embeddings")

    @info_timer(message='Embedding Pipeline')
    async def main(self):
        self.check_embedding_cache()
        await self.generate_embeddings()
        self.insert_embeddings()
        self.delete_embedding_cache()    
        self.check_error_cache()
            
        
    
    
    
    