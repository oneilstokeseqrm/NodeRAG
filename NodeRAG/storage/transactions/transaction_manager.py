"""
Transaction Manager for coordinating Neo4j and Pinecone operations
"""
import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple, Callable
from enum import Enum
from datetime import datetime, timezone
import uuid

from ..neo4j_adapter import Neo4jAdapter
from ..pinecone_adapter import PineconeAdapter
from ...standards.eq_metadata import EQMetadata

logger = logging.getLogger(__name__)


class TransactionState(Enum):
    """Transaction states for two-phase commit"""
    INITIATED = "initiated"
    NEO4J_PREPARED = "neo4j_prepared"
    PINECONE_PREPARED = "pinecone_prepared"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


class TransactionOperation:
    """Represents a single operation within a transaction"""
    def __init__(self, 
                 operation_type: str,
                 target_store: str,
                 method: Callable,
                 args: tuple,
                 kwargs: dict,
                 rollback_method: Optional[Callable] = None,
                 rollback_args: Optional[tuple] = None,
                 rollback_kwargs: Optional[dict] = None):
        self.operation_type = operation_type
        self.target_store = target_store  # 'neo4j' or 'pinecone'
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.rollback_method = rollback_method
        self.rollback_args = rollback_args or ()
        self.rollback_kwargs = rollback_kwargs or {}
        self.result = None
        self.executed = False


class Transaction:
    """Represents a multi-store transaction"""
    def __init__(self, transaction_id: str, tenant_id: str):
        self.id = transaction_id
        self.tenant_id = tenant_id
        self.state = TransactionState.INITIATED
        self.operations: List[TransactionOperation] = []
        self.created_at = datetime.now(timezone.utc)
        self.completed_at = None
        self.error = None
        
    def add_operation(self, operation: TransactionOperation):
        """Add an operation to the transaction"""
        self.operations.append(operation)


class TransactionManager:
    """Manages transactions across Neo4j and Pinecone stores"""
    
    def __init__(self, neo4j_adapter: Neo4jAdapter, pinecone_adapter: PineconeAdapter):
        self.neo4j = neo4j_adapter
        self.pinecone = pinecone_adapter
        self.active_transactions: Dict[str, Transaction] = {}
        self.transaction_log: List[Dict[str, Any]] = []
        
    def begin_transaction(self, tenant_id: str) -> Transaction:
        """Begin a new transaction"""
        transaction_id = f"txn_{uuid.uuid4().hex[:16]}"
        transaction = Transaction(transaction_id, tenant_id)
        self.active_transactions[transaction_id] = transaction
        
        logger.info(f"Beginning transaction {transaction_id} for tenant {tenant_id}")
        self._log_transaction_event(transaction_id, "BEGIN", {"tenant_id": tenant_id})
        
        return transaction
    
    async def execute_transaction(self, transaction: Transaction) -> Tuple[bool, Optional[str]]:
        """Execute a transaction with two-phase commit"""
        try:
            neo4j_ops = [op for op in transaction.operations if op.target_store == "neo4j"]
            pinecone_ops = [op for op in transaction.operations if op.target_store == "pinecone"]
            
            if neo4j_ops:
                logger.info(f"Executing {len(neo4j_ops)} Neo4j operations")
                for op in neo4j_ops:
                    try:
                        op.result = await op.method(*op.args, **op.kwargs)
                        op.executed = True
                    except Exception as e:
                        logger.error(f"Neo4j operation failed: {e}")
                        transaction.error = str(e)
                        await self._rollback_transaction(transaction)
                        return False, str(e)
                
                transaction.state = TransactionState.NEO4J_PREPARED
            
            if pinecone_ops:
                logger.info(f"Executing {len(pinecone_ops)} Pinecone operations")
                for op in pinecone_ops:
                    try:
                        op.result = await op.method(*op.args, **op.kwargs)
                        op.executed = True
                    except Exception as e:
                        logger.error(f"Pinecone operation failed: {e}")
                        transaction.error = str(e)
                        await self._rollback_transaction(transaction)
                        return False, str(e)
                
                transaction.state = TransactionState.PINECONE_PREPARED
            
            transaction.state = TransactionState.COMMITTED
            transaction.completed_at = datetime.now(timezone.utc)
            
            self._log_transaction_event(transaction.id, "COMMIT", {
                "operations": len(transaction.operations),
                "duration_ms": (transaction.completed_at - transaction.created_at).total_seconds() * 1000
            })
            
            del self.active_transactions[transaction.id]
            
            return True, None
            
        except Exception as e:
            logger.error(f"Transaction {transaction.id} failed: {e}")
            await self._rollback_transaction(transaction)
            return False, str(e)
    
    async def _rollback_transaction(self, transaction: Transaction):
        """Rollback a failed transaction"""
        logger.warning(f"Rolling back transaction {transaction.id}")
        transaction.state = TransactionState.ROLLED_BACK
        
        for op in reversed(transaction.operations):
            if op.executed and op.rollback_method:
                try:
                    await op.rollback_method(*op.rollback_args, **op.rollback_kwargs)
                    logger.info(f"Rolled back {op.operation_type} on {op.target_store}")
                except Exception as e:
                    logger.error(f"Rollback failed for {op.operation_type}: {e}")
        
        self._log_transaction_event(transaction.id, "ROLLBACK", {
            "error": transaction.error,
            "operations_executed": sum(1 for op in transaction.operations if op.executed)
        })
        
        if transaction.id in self.active_transactions:
            del self.active_transactions[transaction.id]
    
    def _log_transaction_event(self, transaction_id: str, event: str, details: Dict[str, Any]):
        """Log transaction events for debugging and audit"""
        log_entry = {
            "transaction_id": transaction_id,
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "details": details
        }
        self.transaction_log.append(log_entry)
        
        if len(self.transaction_log) > 1000:
            self.transaction_log = self.transaction_log[-1000:]
    
    async def add_node_with_embedding(self, 
                                     node_id: str,
                                     node_type: str,
                                     metadata: EQMetadata,
                                     embedding: List[float],
                                     node_properties: Optional[Dict[str, Any]] = None,
                                     vector_metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[str]]:
        """Atomically add a node to Neo4j and its embedding to Pinecone"""
        transaction = self.begin_transaction(metadata.tenant_id)
        
        neo4j_op = TransactionOperation(
            operation_type="add_node",
            target_store="neo4j",
            method=self.neo4j.add_node,
            args=(node_id, node_type, metadata.to_dict()),
            kwargs={"properties": node_properties or {}},
            rollback_method=self.neo4j.delete_node,
            rollback_args=(node_id,)
        )
        transaction.add_operation(neo4j_op)
        
        pinecone_op = TransactionOperation(
            operation_type="upsert_vector",
            target_store="pinecone",
            method=self.pinecone.upsert_vector,
            args=(node_id, embedding, metadata),
            kwargs={"additional_metadata": vector_metadata},
            rollback_method=self.pinecone.delete_vectors,
            rollback_args=([node_id], metadata.tenant_id)
        )
        transaction.add_operation(pinecone_op)
        
        success, error = await self.execute_transaction(transaction)
        return success, error
    
    async def add_nodes_batch_with_embeddings(self,
                                            nodes_data: List[Dict[str, Any]]) -> Tuple[int, List[str]]:
        """Atomically add multiple nodes and embeddings
        
        Args:
            nodes_data: List of dicts with keys:
                - node_id: str
                - node_type: str
                - metadata: EQMetadata
                - embedding: List[float]
                - node_properties: Optional[Dict]
                - vector_metadata: Optional[Dict]
        """
        if not nodes_data:
            return 0, []
        
        tenant_id = nodes_data[0]["metadata"].tenant_id
        transaction = self.begin_transaction(tenant_id)
        
        neo4j_batch = []
        pinecone_batch = []
        
        for node in nodes_data:
            neo4j_batch.append((
                node["node_id"],
                node["node_type"],
                {**node["metadata"].to_dict(), **(node.get("node_properties", {}))}
            ))
            
            pinecone_batch.append((
                node["node_id"],
                node["embedding"],
                node["metadata"],
                node.get("vector_metadata", {})
            ))
        
        neo4j_op = TransactionOperation(
            operation_type="add_nodes_batch",
            target_store="neo4j",
            method=self.neo4j.add_nodes_batch,
            args=(neo4j_batch,),
            kwargs={},
        )
        transaction.add_operation(neo4j_op)
        
        pinecone_op = TransactionOperation(
            operation_type="upsert_vectors_batch",
            target_store="pinecone",
            method=self.pinecone.upsert_vectors_batch,
            args=(pinecone_batch,),
            kwargs={"namespace": tenant_id}
        )
        transaction.add_operation(pinecone_op)
        
        success, error = await self.execute_transaction(transaction)
        
        if success:
            neo4j_result = neo4j_op.result
            pinecone_result = pinecone_op.result
            
            total_success = neo4j_result if isinstance(neo4j_result, int) else pinecone_result[0]
            errors = pinecone_result[1] if isinstance(pinecone_result, tuple) else []
            
            return total_success, errors
        else:
            return 0, [error]
    
    def get_transaction_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent transaction log entries"""
        return self.transaction_log[-limit:]
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of transaction manager and stores"""
        neo4j_health = await self.neo4j.health_check() if hasattr(self.neo4j, 'health_check') else {"status": "unknown"}
        pinecone_stats = await self.pinecone.get_stats()
        
        return {
            "transaction_manager": {
                "status": "healthy",
                "active_transactions": len(self.active_transactions),
                "log_entries": len(self.transaction_log)
            },
            "neo4j": neo4j_health,
            "pinecone": {
                "status": "healthy" if pinecone_stats else "unhealthy",
                "total_vectors": pinecone_stats.get("total_vectors", 0)
            }
        }
