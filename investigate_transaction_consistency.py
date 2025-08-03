#!/usr/bin/env python3
"""Investigate transaction consistency test failures"""

import os
import sys
import asyncio
import json
import time
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from NodeRAG.storage.transactions.transaction_manager import TransactionManager
from NodeRAG.storage.neo4j_adapter import Neo4jAdapter
from NodeRAG.storage.pinecone_adapter import PineconeAdapter
from NodeRAG.standards.eq_metadata import EQMetadata
import numpy as np

async def test_transaction_rollback():
    """Test transaction rollback scenarios"""
    
    results = {
        "setup": None,
        "normal_transaction": None,
        "failed_transaction": None,
        "rollback_verification": None,
        "consistency_issues": []
    }
    
    tm = TransactionManager()
    neo4j = Neo4jAdapter()
    pinecone = PineconeAdapter()
    
    metadata = EQMetadata(
        tenant_id="tenant_12345678-1234-4567-8901-123456789012",
        interaction_id="int_12345678-1234-4567-8901-123456789012",
        interaction_type="email",
        text="Transaction test content",
        account_id="acc_12345678-1234-4567-8901-123456789012",
        timestamp="2024-01-01T12:00:00Z",
        user_id="usr_12345678-1234-4567-8901-123456789012",
        source_system="test"
    )
    
    try:
        await neo4j.connect()
        tm.register_adapter("neo4j", neo4j)
        tm.register_adapter("pinecone", pinecone)
        results["setup"] = "SUCCESS"
        
        async with tm.transaction():
            neo4j_data = {
                "id": "tx_test_normal",
                "type": "Entity",
                "content": "Normal transaction test",
                **metadata.to_dict()
            }
            await neo4j.create_node(neo4j_data)
            
            vector_data = {
                "id": "tx_test_normal",
                "values": np.random.rand(3072).tolist(),  # Use 3072 dimensions
                "metadata": metadata.to_dict()
            }
            await pinecone.upsert_vectors([vector_data], namespace=metadata.tenant_id)
            
        results["normal_transaction"] = "SUCCESS"
        
        neo4j_node = await neo4j.get_node("tx_test_normal")
        results["normal_transaction_verified"] = neo4j_node is not None
        
        try:
            async with tm.transaction():
                neo4j_data = {
                    "id": "tx_test_fail",
                    "type": "Entity", 
                    "content": "Failed transaction test",
                    **metadata.to_dict()
                }
                await neo4j.create_node(neo4j_data)
                
                raise Exception("Simulated transaction failure")
                
        except Exception as e:
            results["failed_transaction"] = f"Rolled back: {str(e)}"
        
        failed_node = await neo4j.get_node("tx_test_fail")
        results["rollback_verification"] = {
            "neo4j_rolled_back": failed_node is None,
            "expected": True
        }
        
        if failed_node is not None:
            results["consistency_issues"].append("Neo4j rollback failed - node still exists")
        
        await neo4j.delete_node("tx_test_normal")
        
    except Exception as e:
        results["error"] = str(e)
        results["consistency_issues"].append(f"Transaction test error: {str(e)}")
        
    finally:
        await neo4j.close()
    
    return results

async def test_concurrent_transactions():
    """Test concurrent transaction handling"""
    
    results = {
        "concurrent_creates": None,
        "isolation_maintained": None,
        "deadlock_detected": False
    }
    
    tm = TransactionManager()
    neo4j = Neo4jAdapter()
    
    async def create_node(node_id: str, delay: float = 0):
        """Create a node with optional delay"""
        async with tm.transaction():
            if delay:
                await asyncio.sleep(delay)
            
            await neo4j.create_node({
                "id": node_id,
                "type": "Entity",
                "content": f"Concurrent test {node_id}",
                "tenant_id": "tenant_concurrent_test"
            })
    
    try:
        await neo4j.connect()
        tm.register_adapter("neo4j", neo4j)
        
        tasks = [
            create_node("concurrent_1", 0.1),
            create_node("concurrent_2", 0.05),
            create_node("concurrent_3", 0)
        ]
        
        start_time = time.time()
        await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = time.time() - start_time
        
        results["concurrent_creates"] = {
            "completed": True,
            "elapsed_time": elapsed,
            "expected_behavior": "All nodes created successfully"
        }
        
        nodes_found = 0
        for i in range(1, 4):
            node = await neo4j.get_node(f"concurrent_{i}")
            if node:
                nodes_found += 1
                await neo4j.delete_node(f"concurrent_{i}")
        
        results["isolation_maintained"] = nodes_found == 3
        
    except Exception as e:
        results["error"] = str(e)
        if "deadlock" in str(e).lower():
            results["deadlock_detected"] = True
            
    finally:
        await neo4j.close()
    
    return results

async def test_asyncio_event_loop_issues():
    """Test for asyncio event loop problems in transaction integration"""
    
    results = {
        "event_loop_available": None,
        "transaction_manager_init": None,
        "adapter_connections": None,
        "asyncio_errors": []
    }
    
    try:
        try:
            loop = asyncio.get_event_loop()
            results["event_loop_available"] = {
                "success": True,
                "running": loop.is_running(),
                "closed": loop.is_closed()
            }
        except RuntimeError as e:
            results["event_loop_available"] = {
                "success": False,
                "error": str(e)
            }
            results["asyncio_errors"].append(f"Event loop error: {str(e)}")
        
        try:
            tm = TransactionManager()
            results["transaction_manager_init"] = {"success": True}
        except Exception as e:
            results["transaction_manager_init"] = {
                "success": False,
                "error": str(e)
            }
            results["asyncio_errors"].append(f"TransactionManager init error: {str(e)}")
        
        try:
            neo4j = Neo4jAdapter()
            pinecone = PineconeAdapter()
            
            await neo4j.connect()
            
            results["adapter_connections"] = {
                "neo4j": "SUCCESS",
                "pinecone": "SUCCESS"
            }
            
            await neo4j.close()
            
        except Exception as e:
            results["adapter_connections"] = {
                "error": str(e)
            }
            results["asyncio_errors"].append(f"Adapter connection error: {str(e)}")
    
    except Exception as e:
        results["general_error"] = str(e)
        results["asyncio_errors"].append(f"General asyncio test error: {str(e)}")
    
    return results

if __name__ == "__main__":
    print("=== Transaction Consistency Investigation ===\n")
    
    loop = asyncio.get_event_loop()
    
    print("Testing transaction rollback...")
    rollback_results = loop.run_until_complete(test_transaction_rollback())
    
    print("\nTesting concurrent transactions...")
    concurrent_results = loop.run_until_complete(test_concurrent_transactions())
    
    print("\nTesting asyncio event loop issues...")
    asyncio_results = loop.run_until_complete(test_asyncio_event_loop_issues())
    
    investigation = {
        "rollback_test": rollback_results,
        "concurrent_test": concurrent_results,
        "asyncio_test": asyncio_results,
        "consistency_analysis": {
            "rollback_working": rollback_results.get("rollback_verification", {}).get("neo4j_rolled_back", False),
            "concurrent_safe": concurrent_results.get("isolation_maintained", False),
            "asyncio_issues": len(asyncio_results.get("asyncio_errors", [])) > 0,
            "issues_found": []
        }
    }
    
    all_issues = rollback_results.get("consistency_issues", [])
    
    if not investigation["consistency_analysis"]["rollback_working"]:
        all_issues.append("Transaction rollback not functioning properly")
    
    if not investigation["consistency_analysis"]["concurrent_safe"]:
        all_issues.append("Concurrent transaction isolation issues")
    
    if investigation["consistency_analysis"]["asyncio_issues"]:
        all_issues.extend(asyncio_results.get("asyncio_errors", []))
        
    investigation["consistency_analysis"]["issues_found"] = all_issues
    
    with open("transaction_consistency_investigation.json", "w") as f:
        json.dump(investigation, f, indent=2)
    
    print("\nInvestigation complete. See transaction_consistency_investigation.json")
