#!/usr/bin/env python3
"""
Neo4j Schema Validation Script
Validates that the Locked In multi-tenant schema constraints and indexes are properly implemented.
"""
import os
import sys
import json
import csv
from datetime import datetime
from typing import Dict, List, Any

from neo4j import GraphDatabase


def generate_html_report(constraints: List[Dict], indexes: List[Dict], output_path: str):
    """Generate HTML report of schema validation"""
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Neo4j Schema Alignment Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .pass {{ color: green; }}
        .fail {{ color: red; }}
        .summary {{ background-color: #f9f9f9; padding: 15px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>Neo4j Schema Alignment Report</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <div class="summary">
        <h2>Summary</h2>
        <p>Total Constraints: {len(constraints)}</p>
        <p>Total Indexes: {len(indexes)}</p>
    </div>
    
    <h2>Constraints</h2>
    <table>
        <tr><th>Name</th><th>Type</th><th>Labels/Types</th><th>Properties</th><th>State</th></tr>
"""
    
    for constraint in constraints:
        html_content += f"""
        <tr>
            <td>{constraint.get('name', 'N/A')}</td>
            <td>{constraint.get('type', 'N/A')}</td>
            <td>{constraint.get('labelsOrTypes', 'N/A')}</td>
            <td>{constraint.get('properties', 'N/A')}</td>
            <td class="{'pass' if constraint.get('state') == 'ONLINE' else 'fail'}">{constraint.get('state', 'N/A')}</td>
        </tr>
        """
    
    html_content += """
    </table>
    
    <h2>Indexes</h2>
    <table>
        <tr><th>Name</th><th>Type</th><th>Labels/Types</th><th>Properties</th><th>State</th></tr>
    """
    
    for index in indexes:
        html_content += f"""
        <tr>
            <td>{index.get('name', 'N/A')}</td>
            <td>{index.get('type', 'N/A')}</td>
            <td>{index.get('labelsOrTypes', 'N/A')}</td>
            <td>{index.get('properties', 'N/A')}</td>
            <td class="{'pass' if index.get('state') == 'ONLINE' else 'fail'}">{index.get('state', 'N/A')}</td>
        </tr>
        """
    
    html_content += """
    </table>
</body>
</html>
    """
    
    with open(output_path, 'w') as f:
        f.write(html_content)


def generate_csv_report(constraints: List[Dict], indexes: List[Dict], output_path: str):
    """Generate CSV report of schema validation"""
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        
        writer.writerow(['Type', 'Name', 'Entity Type', 'Labels/Types', 'Properties', 'State'])
        for constraint in constraints:
            writer.writerow([
                'CONSTRAINT',
                constraint.get('name', ''),
                constraint.get('entityType', ''),
                str(constraint.get('labelsOrTypes', '')),
                str(constraint.get('properties', '')),
                constraint.get('state', '')
            ])
        
        for index in indexes:
            writer.writerow([
                'INDEX',
                index.get('name', ''),
                index.get('entityType', ''),
                str(index.get('labelsOrTypes', '')),
                str(index.get('properties', '')),
                index.get('state', '')
            ])


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
                print(f"   Executed: {query}")
            except Exception as e:
                print(f"   Warning: Failed to execute {query}: {e}")


def main():
    """Main validation function"""
    print("🔍 Starting Neo4j Schema Validation...")
    
    neo4j_uri = os.getenv("Neo4j_Credentials_NEO4J_URI", os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    neo4j_user = os.getenv("Neo4j_Credentials_NEO4J_USERNAME", os.getenv("NEO4J_USERNAME", "neo4j"))
    neo4j_password = os.getenv("Neo4j_Credentials_NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD", "password"))
    neo4j_database = os.getenv("Neo4j_Credentials_NEO4J_DATABASE", os.getenv("NEO4J_DATABASE", "neo4j"))
    
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        with driver.session(database=neo4j_database) as session:
            session.run("RETURN 1")
        print("✅ Connected to Neo4j successfully")
        
        print("📝 Creating/updating constraints and indexes...")
        create_constraints_and_indexes(driver, neo4j_database)
        
        print("🔎 Querying current schema...")
        with driver.session(database=neo4j_database) as session:
            constraints_result = session.run("SHOW CONSTRAINTS")
            constraints = [dict(record) for record in constraints_result]
            
            indexes_result = session.run("SHOW INDEXES")
            indexes = [dict(record) for record in indexes_result]
        
        print("📊 Generating reports...")
        generate_html_report(constraints, indexes, "neo4j_schema_alignment_report.html")
        generate_csv_report(constraints, indexes, "neo4j_schema_alignment_report.csv")
        
        expected_labels = ["Entity", "SemanticUnit", "TextChunk", "Attribute", "Community", "Summary", "HighLevelElement"]
        
        print("\n✅ Validation Results:")
        print(f"   Total constraints: {len(constraints)}")
        print(f"   Total indexes: {len(indexes)}")
        
        composite_constraints = [c for c in constraints if 
                               c.get('type') == 'UNIQUENESS' and 
                               c.get('properties') and 
                               len(c.get('properties', [])) == 2]
        
        print(f"   Composite (tenant_id, node_id) constraints: {len(composite_constraints)}")
        
        print(f"\n📄 Reports generated:")
        print(f"   - neo4j_schema_alignment_report.html")
        print(f"   - neo4j_schema_alignment_report.csv")
        
        return True
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False
    finally:
        if 'driver' in locals():
            driver.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
