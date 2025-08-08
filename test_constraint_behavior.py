#!/usr/bin/env python3
"""
Test script to verify composite constraint behavior
"""
import sys
import os
from datetime import datetime, timezone
from neo4j import GraphDatabase


def create_constraints_and_indexes(driver, database="neo4j"):
    """Create constraints and indexes for Locked In multi-tenant design"""
    labels = [
        "Entity", "SemanticUnit", "TextChunk", "Attribute", 
        "Community", "Summary", "HighLevelElement"
    ]
    
    constraints_and_indexes = []
    
    constraints_and_indexes.extend([
        "DROP CONSTRAINT node_id_unique IF EXISTS",
        "DROP CONSTRAINT relationship_id_unique IF EXISTS"
    ])
    
    for label in labels:
        constraints_and_indexes.append(
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE (n.tenant_id, n.node_id) IS UNIQUE"
        )
    
    for label in labels:
        constraints_and_indexes.extend([
            f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.tenant_id)",
            f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.tenant_id, n.account_id)"
        ])
    
    constraints_and_indexes.append(
        "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() REQUIRE r.relationship_id IS UNIQUE"
    )
    
    constraints_and_indexes.extend([
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() ON (r.tenant_id)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() ON (r.interaction_id)"
    ])
    
    with driver.session(database=database) as session:
        for query in constraints_and_indexes:
            try:
                session.run(query)
            except Exception as e:
                print(f"   Warning: Failed to execute {query}: {e}")


def test_composite_constraints():
    """Test that composite (tenant_id, node_id) constraints work correctly"""
    print("🧪 Testing composite constraint behavior...")
    
    neo4j_uri = os.getenv("Neo4j_Credentials_NEO4J_URI", os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    neo4j_user = os.getenv("Neo4j_Credentials_NEO4J_USERNAME", os.getenv("NEO4J_USERNAME", "neo4j"))
    neo4j_password = os.getenv("Neo4j_Credentials_NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD", "password"))
    neo4j_database = os.getenv("Neo4j_Credentials_NEO4J_DATABASE", os.getenv("NEO4J_DATABASE", "neo4j"))
    
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        with driver.session(database=neo4j_database) as session:
            session.run("RETURN 1")
        print("✅ Connected to Neo4j successfully")
        
        create_constraints_and_indexes(driver, neo4j_database)
        
        print("   Test 1: Same node_id across different tenants...")
        
        with driver.session(database=neo4j_database) as session:
            try:
                session.run("""
                    CREATE (n:Entity {
                        tenant_id: 'T1',
                        node_id: 'hash123',
                        account_id: 'acc_001',
                        interaction_id: 'int_test_001',
                        name: 'Test Entity T1'
                    })
                """)
                print("      Node in T1: ✅")
            except Exception as e:
                print(f"      Node in T1: ❌ {e}")
        
        with driver.session(database=neo4j_database) as session:
            try:
                session.run("""
                    CREATE (n:Entity {
                        tenant_id: 'T2',
                        node_id: 'hash123',
                        account_id: 'acc_002',
                        interaction_id: 'int_test_002',
                        name: 'Test Entity T2'
                    })
                """)
                print("      Node in T2: ✅")
            except Exception as e:
                print(f"      Node in T2: ❌ {e}")
        
        print("   Test 2: Duplicate node_id in same tenant...")
        
        with driver.session(database=neo4j_database) as session:
            try:
                session.run("""
                    CREATE (n:Entity {
                        tenant_id: 'T1',
                        node_id: 'hash123',
                        account_id: 'acc_001',
                        interaction_id: 'int_test_003',
                        name: 'Duplicate Entity'
                    })
                """)
                print("      Duplicate in T1: ❌ Should have failed!")
            except Exception as e:
                print(f"      Duplicate in T1: ✅ Correctly rejected ({type(e).__name__})")
        
        print("🎉 Constraint behavior test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
    finally:
        if 'driver' in locals():
            driver.close()


if __name__ == "__main__":
    success = test_composite_constraints()
    sys.exit(0 if success else 1)
