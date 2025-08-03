"""
Neo4j Storage Adapter for NodeRAG with EQ Metadata Support
"""
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import os

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, TransientError

from ..standards.eq_metadata import EQMetadata
from ..utils.id_generation import NodeIDGenerator


logger = logging.getLogger(__name__)


class Neo4jAdapter:
    """Async Neo4j storage adapter for NodeRAG with EQ metadata support"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize Neo4j adapter with configuration"""
        self.config = config or {}
        self.driver: Optional[AsyncDriver] = None
        self.database = self.config.get('database', 'neo4j')
        
        self.uri = self.config.get('uri', os.getenv('NEO4J_URI', 'bolt://localhost:7687'))
        self.user = self.config.get('user', os.getenv('NEO4J_USERNAME', 'neo4j'))
        self.password = self.config.get('password', os.getenv('NEO4J_PASSWORD', ''))
        
        self.batch_size = self.config.get('batch_size', 1000)
        self.max_connection_pool_size = self.config.get('max_connection_pool_size', 50)
        
    async def connect(self) -> bool:
        """Establish connection to Neo4j database"""
        try:
            self.driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                max_connection_pool_size=self.max_connection_pool_size,
                connection_timeout=30,
                max_transaction_retry_time=15
            )
            
            async with self.driver.session(database=self.database) as session:
                result = await session.run("RETURN 1 as test")
                await result.consume()
                
            logger.info(f"Successfully connected to Neo4j at {self.uri}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            return False
    
    async def close(self):
        """Close Neo4j connection"""
        if self.driver:
            await self.driver.close()
            self.driver = None
    
    async def create_constraints_and_indexes(self):
        """Create constraints and indexes for EQ metadata fields"""
        constraints_and_indexes = [
            "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.node_id IS UNIQUE",
            "CREATE CONSTRAINT relationship_id_unique IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() REQUIRE r.relationship_id IS UNIQUE",
            
            "CREATE INDEX tenant_id_index IF NOT EXISTS FOR (n:Node) ON (n.tenant_id)",
            "CREATE INDEX account_id_index IF NOT EXISTS FOR (n:Node) ON (n.account_id)",
            "CREATE INDEX interaction_id_index IF NOT EXISTS FOR (n:Node) ON (n.interaction_id)",
            "CREATE INDEX interaction_type_index IF NOT EXISTS FOR (n:Node) ON (n.interaction_type)",
            "CREATE INDEX source_system_index IF NOT EXISTS FOR (n:Node) ON (n.source_system)",
            "CREATE INDEX timestamp_index IF NOT EXISTS FOR (n:Node) ON (n.timestamp)",
            "CREATE INDEX node_type_index IF NOT EXISTS FOR (n:Node) ON (n.node_type)",
            
            "CREATE INDEX rel_tenant_id_index IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() ON (r.tenant_id)",
            "CREATE INDEX rel_interaction_id_index IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() ON (r.interaction_id)",
        ]
        
        async with self.driver.session(database=self.database) as session:
            for query in constraints_and_indexes:
                try:
                    await session.run(query)
                    logger.debug(f"Executed: {query}")
                except Exception as e:
                    logger.warning(f"Failed to execute {query}: {e}")
    
    async def add_node(self, node_id: str, node_type: str, metadata: EQMetadata, 
                      properties: Optional[Dict[str, Any]] = None) -> bool:
        """Add a single node with EQ metadata"""
        errors = metadata.validate()
        if errors:
            logger.error(f"Invalid metadata for node {node_id}: {errors}")
            return False
        
        node_data = {
            'node_id': node_id,
            'node_type': node_type,
            **metadata.to_dict(),
            **(properties or {})
        }
        
        query = """
        MERGE (n:Node {node_id: $node_id})
        SET n += $properties
        RETURN n.node_id as id
        """
        
        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(query, node_id=node_id, properties=node_data)
                record = await result.single()
                return record is not None
        except Exception as e:
            logger.error(f"Failed to add node {node_id}: {e}")
            return False
    
    async def add_nodes_batch(self, nodes: List[Dict[str, Any]]) -> Tuple[int, List[str]]:
        """Add multiple nodes in batch with EQ metadata"""
        successful_count = 0
        errors = []
        
        validated_nodes = []
        for node in nodes:
            try:
                metadata = EQMetadata(**{k: v for k, v in node.items() 
                                       if k in ['tenant_id', 'interaction_id', 'interaction_type', 
                                               'text', 'account_id', 'timestamp', 'user_id', 'source_system']})
                validation_errors = metadata.validate()
                if validation_errors:
                    errors.append(f"Node {node.get('node_id', 'unknown')}: {validation_errors}")
                    continue
                validated_nodes.append(node)
            except Exception as e:
                errors.append(f"Node {node.get('node_id', 'unknown')}: {str(e)}")
        
        for i in range(0, len(validated_nodes), self.batch_size):
            batch = validated_nodes[i:i + self.batch_size]
            
            query = """
            UNWIND $nodes as node
            MERGE (n:Node {node_id: node.node_id})
            SET n += node
            RETURN count(n) as created
            """
            
            try:
                async with self.driver.session(database=self.database) as session:
                    result = await session.run(query, nodes=batch)
                    record = await result.single()
                    successful_count += record['created'] if record else 0
            except Exception as e:
                errors.append(f"Batch {i//self.batch_size + 1}: {str(e)}")
        
        return successful_count, errors
    
    async def add_relationship(self, source_id: str, target_id: str, relationship_type: str,
                             metadata: EQMetadata, properties: Optional[Dict[str, Any]] = None) -> bool:
        """Add a relationship between two nodes"""
        errors = metadata.validate()
        if errors:
            logger.error(f"Invalid metadata for relationship {source_id}->{target_id}: {errors}")
            return False
        
        relationship_id = NodeIDGenerator.generate_relationship_id(
            source_id, target_id, relationship_type, metadata.tenant_id
        )
        
        rel_data = {
            'relationship_id': relationship_id,
            'relationship_type': relationship_type,
            **metadata.to_dict(),
            **(properties or {})
        }
        
        query = """
        MATCH (source:Node {node_id: $source_id})
        MATCH (target:Node {node_id: $target_id})
        MERGE (source)-[r:RELATIONSHIP {relationship_id: $relationship_id}]->(target)
        SET r += $properties
        RETURN r.relationship_id as id
        """
        
        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(
                    query, 
                    source_id=source_id, 
                    target_id=target_id,
                    relationship_id=relationship_id,
                    properties=rel_data
                )
                record = await result.single()
                return record is not None
        except Exception as e:
            logger.error(f"Failed to add relationship {source_id}->{target_id}: {e}")
            return False
    
    async def add_relationships_batch(self, relationships: List[Dict[str, Any]]) -> Tuple[int, List[str]]:
        """Add multiple relationships in batch"""
        successful_count = 0
        errors = []
        
        validated_relationships = []
        for rel in relationships:
            try:
                metadata = EQMetadata(**{k: v for k, v in rel.items() 
                                       if k in ['tenant_id', 'interaction_id', 'interaction_type', 
                                               'text', 'account_id', 'timestamp', 'user_id', 'source_system']})
                validation_errors = metadata.validate()
                if validation_errors:
                    errors.append(f"Relationship {rel.get('source_id', 'unknown')}->{rel.get('target_id', 'unknown')}: {validation_errors}")
                    continue
                
                relationship_id = NodeIDGenerator.generate_relationship_id(
                    rel['source_id'], rel['target_id'], rel['relationship_type'], metadata.tenant_id
                )
                rel['relationship_id'] = relationship_id
                validated_relationships.append(rel)
            except Exception as e:
                errors.append(f"Relationship {rel.get('source_id', 'unknown')}->{rel.get('target_id', 'unknown')}: {str(e)}")
        
        for i in range(0, len(validated_relationships), self.batch_size):
            batch = validated_relationships[i:i + self.batch_size]
            
            query = """
            UNWIND $relationships as rel
            MATCH (source:Node {node_id: rel.source_id})
            MATCH (target:Node {node_id: rel.target_id})
            MERGE (source)-[r:RELATIONSHIP {relationship_id: rel.relationship_id}]->(target)
            SET r += rel
            RETURN count(r) as created
            """
            
            try:
                async with self.driver.session(database=self.database) as session:
                    result = await session.run(query, relationships=batch)
                    record = await result.single()
                    successful_count += record['created'] if record else 0
            except Exception as e:
                errors.append(f"Relationship batch {i//self.batch_size + 1}: {str(e)}")
        
        return successful_count, errors
    
    async def get_nodes_by_tenant(self, tenant_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all nodes for a specific tenant"""
        query = """
        MATCH (n:Node {tenant_id: $tenant_id})
        RETURN n
        """
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(query, tenant_id=tenant_id)
                return [dict(record['n']) async for record in result]
        except Exception as e:
            logger.error(f"Failed to get nodes for tenant {tenant_id}: {e}")
            return []
    
    async def get_nodes_by_metadata(self, filters: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get nodes filtered by metadata fields"""
        where_clauses = []
        params = {}
        
        for key, value in filters.items():
            if key in ['tenant_id', 'account_id', 'interaction_id', 'interaction_type', 
                      'user_id', 'source_system', 'node_type']:
                where_clauses.append(f"n.{key} = ${key}")
                params[key] = value
        
        if not where_clauses:
            return []
        
        query = f"""
        MATCH (n:Node)
        WHERE {' AND '.join(where_clauses)}
        RETURN n
        """
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(query, **params)
                return [dict(record['n']) async for record in result]
        except Exception as e:
            logger.error(f"Failed to get nodes by metadata {filters}: {e}")
            return []
    
    async def get_subgraph(self, tenant_id: str, account_id: Optional[str] = None, 
                          interaction_id: Optional[str] = None) -> Dict[str, Any]:
        """Get subgraph for tenant/account/interaction filtering"""
        where_clauses = [f"n.tenant_id = $tenant_id"]
        params = {'tenant_id': tenant_id}
        
        if account_id:
            where_clauses.append("n.account_id = $account_id")
            params['account_id'] = account_id
        
        if interaction_id:
            where_clauses.append("n.interaction_id = $interaction_id")
            params['interaction_id'] = interaction_id
        
        query = f"""
        MATCH (n:Node)
        WHERE {' AND '.join(where_clauses)}
        OPTIONAL MATCH (n)-[r:RELATIONSHIP]->(m:Node)
        WHERE {' AND '.join([clause.replace('n.', 'm.') for clause in where_clauses])}
        RETURN n, r, m
        """
        
        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run(query, **params)
                
                nodes = {}
                relationships = []
                seen_relationships = set()
                
                async for record in result:
                    node = dict(record['n'])
                    nodes[node['node_id']] = node
                    
                    if record['r'] and record['m']:
                        rel = dict(record['r'])
                        target_node = dict(record['m'])
                        
                        rel_id = rel.get('relationship_id', f"{rel.get('source_id', '')}-{rel.get('target_id', '')}")
                        if rel_id not in seen_relationships:
                            relationships.append(rel)
                            seen_relationships.add(rel_id)
                        
                        nodes[target_node['node_id']] = target_node
                
                return {
                    'nodes': list(nodes.values()),
                    'relationships': relationships,
                    'node_count': len(nodes),
                    'relationship_count': len(relationships)
                }
        except Exception as e:
            logger.error(f"Failed to get subgraph for tenant {tenant_id}: {e}")
            return {'nodes': [], 'relationships': [], 'node_count': 0, 'relationship_count': 0}
    
    async def delete_node_by_id(self, node_id: str) -> bool:
        """Delete a single node by its ID"""
        async with self.driver.session(database=self.database) as session:
            query = """
            MATCH (n {node_id: $node_id})
            DETACH DELETE n
            RETURN count(n) as deleted_count
            """
            result = await session.run(query, node_id=node_id)
            record = await result.single()
            deleted_count = record["deleted_count"] if record else 0
            
            logger.info(f"Deleted {deleted_count} nodes with ID {node_id}")
            return deleted_count > 0

    async def delete_node(self, node_id: str) -> bool:
        """Delete a node by ID (alias for delete_node_by_id)"""
        return await self.delete_node_by_id(node_id)


    async def clear_tenant_data(self, tenant_id: str) -> bool:
        """Delete all nodes and relationships for a tenant
        
        Args:
            tenant_id: The tenant whose data to clear
            
        Returns:
            bool: True if successful, False if error occurred
        """
        try:
            async with self.driver.session(database=self.database) as session:
                count_query = """
                MATCH (n {tenant_id: $tenant_id})
                WITH count(n) as node_count
                MATCH ()-[r {tenant_id: $tenant_id}]-()
                RETURN node_count, count(r) as rel_count
                """
                
                result = await session.run(count_query, tenant_id=tenant_id)
                record = await result.single()
                
                if record:
                    node_count = record["node_count"]
                    rel_count = record["rel_count"] // 2  # Relationships are counted twice
                else:
                    node_count = 0
                    rel_count = 0
                
                delete_query = """
                MATCH (n {tenant_id: $tenant_id})
                DETACH DELETE n
                """
                
                await session.run(delete_query, tenant_id=tenant_id)
                
                logger.info(f"Cleared tenant {tenant_id}: {node_count} nodes, {rel_count} relationships")
                return True  # Return boolean success indicator
        except Exception as e:
            logger.error(f"Failed to clear data for tenant {tenant_id}: {e}")
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run("MATCH (n) RETURN count(n) as count")
                record = await result.single()
                total_nodes = record['count'] if record else 0
                
                result = await session.run("MATCH ()-[r]->() RETURN count(r) as count")
                record = await result.single()
                total_relationships = record['count'] if record else 0
                
                nodes_by_type = {}
                result = await session.run("MATCH (n) WHERE n.node_type IS NOT NULL RETURN n.node_type as type, count(n) as count")
                async for record in result:
                    if record['type']:
                        nodes_by_type[record['type']] = record['count']
                
                nodes_by_tenant = {}
                result = await session.run("MATCH (n) WHERE n.tenant_id IS NOT NULL RETURN n.tenant_id as tenant, count(n) as count")
                async for record in result:
                    if record['tenant']:
                        nodes_by_tenant[record['tenant']] = record['count']
                
                return {
                    'total_nodes': total_nodes,
                    'total_relationships': total_relationships,
                    'nodes_by_type': nodes_by_type,
                    'nodes_by_tenant': nodes_by_tenant
                }
                
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {'error': str(e)}
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on Neo4j connection"""
        try:
            async with self.driver.session(database=self.database) as session:
                start_time = datetime.now()
                result = await session.run("RETURN 1 as test")
                await result.consume()
                response_time = (datetime.now() - start_time).total_seconds()
                
                return {
                    'status': 'healthy',
                    'response_time_seconds': response_time,
                    'database': self.database,
                    'uri': self.uri
                }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'database': self.database,
                'uri': self.uri
            }
