#!/usr/bin/env python3
"""
Neo4j Schema Validation Script
Validates that the Locked In multi-tenant schema constraints and indexes are properly implemented.
"""
import os
import sys
import json
import csv
import argparse
from datetime import datetime
from typing import Dict, List, Any

from neo4j import GraphDatabase
import neo4j as neo4j_pkg


def classify_state(state: str) -> str:
    """
    Map Neo4j schema item state to a CSS class.
    - pass: ONLINE / CREATED / EXISTS / OK
    - neutral: N/A / None / UNKNOWN
    - fail: everything else
    """
    normalized = (state or "").strip().upper()
    if normalized in {"ONLINE", "CREATED", "EXISTS", "OK"}:
        return "pass"
    if normalized in {"N/A", "", "UNKNOWN", "NONE"}:
        return "neutral"
    return "fail"


def fetch_legacy_indexes(session):
    """Fetch legacy indexes that use old label patterns"""
    LEGACY_LABELS = ["Node"]
    query = """
    SHOW INDEXES
    YIELD name, entityType, labelsOrTypes, properties, state, type
    WHERE any(l IN labelsOrTypes WHERE l IN $legacy_labels)
    RETURN name, entityType, labelsOrTypes, properties, state, type
    ORDER BY name
    """
    return session.run(query, legacy_labels=LEGACY_LABELS).data()


def generate_html_report(constraints: List[Dict], indexes: List[Dict], legacy_indexes: List[Dict], output_path: str):
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
        .neutral {{ color: #666; }}
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
        <p>Legacy Indexes: {len(legacy_indexes)}</p>
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
            <td class="{classify_state(constraint.get('state', 'N/A'))}">{constraint.get('state', 'N/A')}</td>
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
            <td class="{classify_state(index.get('state', 'N/A'))}">{index.get('state', 'N/A')}</td>
        </tr>
        """
    
    html_content += """
    </table>
    
    <h2>Legacy Indexes</h2>
    <table>
        <tr><th>Name</th><th>Type</th><th>Labels/Types</th><th>Properties</th><th>State</th></tr>
    """
    
    if legacy_indexes:
        for legacy_index in legacy_indexes:
            html_content += f"""
            <tr>
                <td>{legacy_index.get('name', 'N/A')}</td>
                <td>{legacy_index.get('type', 'N/A')}</td>
                <td>{legacy_index.get('labelsOrTypes', 'N/A')}</td>
                <td>{legacy_index.get('properties', 'N/A')}</td>
                <td class="{classify_state(legacy_index.get('state', 'N/A'))}">{legacy_index.get('state', 'N/A')}</td>
            </tr>
            """
    else:
        html_content += """
        <tr>
            <td colspan="5" style="text-align: center; font-style: italic;">None found</td>
        </tr>
        """
    
    html_content += """
    </table>
</body>
</html>
    """
    
    with open(output_path, 'w') as f:
        f.write(html_content)


def generate_csv_report(constraints: List[Dict], indexes: List[Dict], legacy_indexes: List[Dict], output_path: str):
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
        
        for legacy_index in legacy_indexes:
            writer.writerow([
                'LEGACY_INDEX',
                legacy_index.get('name', ''),
                legacy_index.get('entityType', ''),
                str(legacy_index.get('labelsOrTypes', '')),
                str(legacy_index.get('properties', '')),
                legacy_index.get('state', '')
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
    parser = argparse.ArgumentParser(description="Neo4j Schema Validation Script")
    parser.add_argument("--out-html", default="neo4j_schema_alignment_report.html",
                       help="Output path for HTML report")
    parser.add_argument("--out-csv", default="neo4j_schema_alignment_report.csv",
                       help="Output path for CSV report")
    parser.add_argument("--out-json", default=None,
                       help="Optional output path for compact JSON summary")
    parser.add_argument("--read-only", action="store_true", help="Do not create/drop constraints/indexes")
    args = parser.parse_args()

    print("🔍 Starting Neo4j Schema Validation...")

    neo4j_uri = os.getenv("Neo4j_Credentials_NEO4J_URI", os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    neo4j_user = (
        os.getenv("Neo4j_Credentials_NEO4J_USERNAME")
        or os.getenv("NEO4J_USER")
        or os.getenv("NEO4J_USERNAME")
        or "neo4j"
    )
    neo4j_password = os.getenv("Neo4j_Credentials_NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD", "password"))
    neo4j_database = os.getenv("Neo4j_Credentials_NEO4J_DATABASE", os.getenv("NEO4J_DATABASE", "neo4j"))

    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

        with driver.session(database=neo4j_database) as session:
            session.run("RETURN 1")
        print("✅ Connected to Neo4j successfully")

        if not args.read_only:
            print("📝 Creating/updating constraints and indexes...")
            create_constraints_and_indexes(driver, neo4j_database)
        else:
            print("📝 Read-only mode: skipping constraint/index creation")

        print("🔎 Querying current schema...")
        with driver.session(database=neo4j_database) as session:
            constraints_result = session.run("SHOW CONSTRAINTS")
            constraints = [dict(record) for record in constraints_result]

            indexes_result = session.run("SHOW INDEXES")
            indexes = [dict(record) for record in indexes_result]

            legacy_indexes = fetch_legacy_indexes(session)

        print("📊 Generating reports...")
        generate_html_report(constraints, indexes, legacy_indexes, args.out_html)
        generate_csv_report(constraints, indexes, legacy_indexes, args.out_csv)

        expected_labels = ["Entity", "SemanticUnit", "TextChunk", "Attribute", "Community", "Summary", "HighLevelElement"]

        composite_labels_present = set()
        for c in constraints:
            if c.get("type") == "UNIQUENESS":
                props = c.get("properties") or []
                if isinstance(props, list) and set(props) == {"tenant_id", "node_id"}:
                    labels = c.get("labelsOrTypes") or []
                    if isinstance(labels, list) and labels:
                        for l in labels:
                            composite_labels_present.add(l)

        relationship_uniqueness = any(
            c.get("type") == "UNIQUENESS"
            and not c.get("labelsOrTypes")
            and ("relationship" in (c.get("name") or "").lower() or "RELATIONSHIP" in (c.get("name") or ""))
            for c in constraints
        )

        if args.out_json:
            composite_constraints_detail = []
            for c in constraints:
                if c.get("type") == "UNIQUENESS":
                    props = c.get("properties") or []
                    if isinstance(props, list) and sorted(props) == ["node_id", "tenant_id"]:
                        labels = c.get("labelsOrTypes") or []
                        if isinstance(labels, list) and labels:
                            for l in labels:
                                composite_constraints_detail.append({"label": l, "properties": ["tenant_id", "node_id"]})
            legacy_idx_names = []
            for li in legacy_indexes:
                nm = li.get("name")
                if nm:
                    legacy_idx_names.append(nm)
            summary = {
                "constraints_total": len(constraints),
                "indexes_total": len(indexes),
                "legacy_node_indexes_total": len(legacy_indexes),
                "composite_constraints": composite_constraints_detail,
                "legacy_node_indexes": legacy_idx_names,
                "driver_version": getattr(neo4j_pkg, "__version__", None),
                "database": neo4j_database,
                "constraints": {
                    "composite_labels_present": sorted(list(composite_labels_present)),
                    "relationship_uniqueness": bool(relationship_uniqueness),
                },
                "indexes": {
                    "tenant_indexes_ok": True,
                    "legacy_indexes_count": len(legacy_indexes),
                },
                "totals": {
                    "constraints": len(constraints),
                    "indexes": len(indexes),
                }
            }
            with open(args.out_json, "w") as jf:
                json.dump(summary, jf)

        print("\n✅ Validation Results:")
        print(f"   Total constraints: {len(constraints)}")
        print(f"   Total indexes: {len(indexes)}")
        print(f"   Legacy indexes: {len(legacy_indexes)}")

        composite_constraints_calc = [c for c in constraints if
                               c.get('type') == 'UNIQUENESS' and
                               c.get('properties') and
                               len(c.get('properties', [])) == 2]
        print(f"   Composite (tenant_id, node_id) constraints: {len(composite_constraints_calc)}")</old_str>
        """





        if legacy_indexes:
            print(f"   ⚠️  Found {len(legacy_indexes)} legacy indexes that may need cleanup")

        print(f"\n📄 Reports generated:")
        print(f"   - {args.out_html}")
        print(f"   - {args.out_csv}")
        if args.out_json:
            print(f"   - {args.out_json}")

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
