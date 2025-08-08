#!/usr/bin/env python3
import os
import sys
import uuid
import time
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
from neo4j import GraphDatabase
from pinecone import Pinecone

def env_or(secret_name: str, default: str = "") -> str:
    return os.getenv(secret_name, default)

def ensure_dirs():
    Path("test-reports/phase_4/wp0/smoke").mkdir(parents=True, exist_ok=True)

def connect_neo4j():
    uri = env_or("Neo4j_Credentials_NEO4J_URI", env_or("NEO4J_URI", "bolt://localhost:7687"))
    user = (
        env_or("Neo4j_Credentials_NEO4J_USERNAME")
        or env_or("NEO4J_USER")
        or env_or("NEO4J_USERNAME")
        or "neo4j"
    )
    pwd = env_or("Neo4j_Credentials_NEO4J_PASSWORD", env_or("NEO4J_PASSWORD", "password"))
    db = env_or("Neo4j_Credentials_NEO4J_DATABASE", env_or("NEO4J_DATABASE", "neo4j"))
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    with driver.session(database=db) as s:
        s.run("RETURN 1")
    return driver, db

def pinecone_connect():
    api_key = env_or("PINECONE_API_KEY") or env_or("pinecone_API_key")
    if not api_key:
        raise RuntimeError("Missing PINECONE_API_KEY")
    index_name = env_or("PINECONE_INDEX") or env_or("Pinecone_Index_Name")
    if not index_name:
        raise RuntimeError("Missing PINECONE_INDEX")
    pc = Pinecone(api_key=api_key)
    existing = [i.name for i in pc.list_indexes()]
    if index_name not in existing:
        raise RuntimeError(f"Pinecone index {index_name} not found. Do not auto-create in WP-0.")
    index = pc.Index(index_name)
    return index

def upsert_vectors(index, namespace: str, vectors: List[Tuple[str, List[float], Dict]]):
    try:
        index.upsert(vectors=vectors, namespace=namespace)
    except TypeError:
        to_send = [{"id": vid, "values": vals, "metadata": meta} for vid, vals, meta in vectors]
        index.upsert(vectors=to_send, namespace=namespace)

def list_namespaces(index) -> List[str]:
    try:
        stats = index.describe_index_stats()
        ns = list(stats.get("namespaces", {}).keys())
        return ns
    except Exception:
        stats = index.describe_index_stats()
        return list(stats.get("namespaces", {}).keys())

def get_sample_vectors(index, namespace: str, top_k: int = 3):
    dim = 3072
    q = np.random.rand(dim).astype(np.float32).tolist()
    try:
        res = index.query(vector=q, namespace=namespace, top_k=top_k, include_metadata=True)
        matches = getattr(res, "matches", None)
        if matches is None:
            matches = res.get("matches", [])
        return matches
    except Exception:
        return []

def create_neo4j_nodes_edges(driver, db, tenant_id: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    account_id = f"acc_{uuid.uuid4()}"
    user_id = f"usr_{uuid.uuid4()}"
    interaction_id = f"int_{uuid.uuid4()}"
    nodes = [
        ("Entity", f"ent_{uuid.uuid4()}"),
        ("SemanticUnit", f"sem_{uuid.uuid4()}"),
        ("TextChunk", f"txt_{uuid.uuid4()}"),
    ]
    rels = []
    if len(nodes) >= 2:
        rels.append((nodes[0][1], nodes[1][1], "RELATED_TO"))
    with driver.session(database=db) as s:
        labels = ["Entity","SemanticUnit","TextChunk","Attribute","Community","Summary","HighLevelElement"]
        for label in labels:
            s.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE (n.tenant_id, n.node_id) IS UNIQUE")
            s.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.tenant_id)")
            s.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.tenant_id, n.account_id)")
        s.run("CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELATIONSHIP]-() REQUIRE r.relationship_id IS UNIQUE")
        for label, node_id in nodes:
            s.run(
                f"MERGE (n:{label} {{tenant_id:$tenant_id, node_id:$node_id}}) "
                "SET n.account_id=$account_id, n.interaction_id=$interaction_id, "
                "n.user_id=$user_id, n.source_system=$source_system, "
                "n.interaction_type=$interaction_type, n.timestamp=$timestamp",
                tenant_id=tenant_id,
                node_id=node_id,
                account_id=account_id,
                interaction_id=interaction_id,
                user_id=user_id,
                source_system="internal",
                interaction_type="chat",
                timestamp=now_iso,
            )
        for sid, tid, rtype in rels:
            rel_id = f"rel_{uuid.uuid4()}"
            s.run(
                "MATCH (a {tenant_id:$tenant_id, node_id:$sid}), (b {tenant_id:$tenant_id, node_id:$tid}) "
                "MERGE (a)-[r:RELATIONSHIP {relationship_id:$rid}]->(b) "
                "SET r.tenant_id=$tenant_id, r.interaction_id=$interaction_id",
                tenant_id=tenant_id,
                sid=sid,
                tid=tid,
                rid=rel_id,
                interaction_id=interaction_id,
            )
    return {"tenant": tenant_id, "created_nodes": len(nodes), "created_edges": len(rels)}

def pinecone_upserts_for_tenant(index, tenant_id: str):
    dim = 3072
    now_iso = datetime.now(timezone.utc).isoformat()
    base_meta = {
        "tenant_id": tenant_id,
        "interaction_id": f"int_{uuid.uuid4()}",
        "interaction_type": "chat",
        "account_id": f"acc_{uuid.uuid4()}",
        "timestamp": now_iso,
        "user_id": f"usr_{uuid.uuid4()}",
        "source_system": "internal",
    }
    components = ["entities", "semantic_units"]
    ns_created = []
    for comp in components:
        ns = f"{tenant_id}_{comp}"
        vecs = []
        for i in range(3):
            vid = f"{comp}_{uuid.uuid4()}"
            vals = np.random.rand(dim).astype(np.float32).tolist()
            meta = dict(base_meta)  # exactly 7 fields
            vecs.append((vid, vals, meta))
        upsert_vectors(index, ns, vecs)
        ns_created.append(ns)
    return ns_created

def query_neo4j_counts(driver, db, tenants: List[str]):
    out = {}
    with driver.session(database=db) as s:
        for t in tenants:
            counts = {}
            for label in ["Entity","SemanticUnit","TextChunk","Attribute","Community","Summary","HighLevelElement"]:
                rec = s.run(f"MATCH (n:{label} {{tenant_id:$t}}) RETURN count(n) AS c", t=t).single()
                counts[label] = rec["c"] if rec else 0
            relc = s.run("MATCH ()-[r:RELATIONSHIP {tenant_id:$t}]-() RETURN count(r) AS c", t=t).single()
            counts["RELATIONSHIP"] = relc["c"] if relc else 0
            cross = s.run("MATCH (a {tenant_id:$t1})-[r:RELATIONSHIP]-(b {tenant_id:$t2}) RETURN count(r) AS c",
                          t1=t, t2="__other__").single()
            out[t] = counts
    return out

def verify_no_cross_tenant_edges(driver, db, t1: str, t2: str) -> bool:
    with driver.session(database=db) as s:
        rec = s.run(
            "MATCH (a {tenant_id:$t1})-[r:RELATIONSHIP]-(b {tenant_id:$t2}) RETURN count(r) AS c",
            t1=t1, t2=t2
        ).single()
        return (rec["c"] if rec else 0) == 0

def check_no_local_embedding_cache() -> bool:
    patterns = ["**/embedding_cache*.pkl", "**/*embeddings*.pkl"]
    for pat in patterns:
        if any(Path(".").rglob(pat)):
            return False
    return True

def write_csv(csv_path: Path, neo4j_counts: Dict[str, Dict[str,int]], namespaces: List[str]):
    lines = ["tenant,component_type,count"]
    for tenant, counts in neo4j_counts.items():
        for comp, c in counts.items():
            lines.append(f"{tenant},{comp},{c}")
    lines.append("tenants_and_namespaces,namespace,count_placeholder")
    for ns in namespaces:
        lines.append(f"_,{ns},-")
    csv_path.write_text("\n".join(lines), encoding="utf-8")

def write_html(html_path: Path, tenants: List[str], neo4j_counts: Dict[str, Dict[str,int]], namespaces: List[str], meta_samples: Dict[str, List[dict]], no_cache: bool):
    html = []
    html.append("<!DOCTYPE html><html><head><meta charset='utf-8'><title>Embedding Storage Smoke Report</title>")
    html.append("<style>body{font-family:Arial;margin:20px} table{border-collapse:collapse} td,th{border:1px solid #ddd;padding:6px} .pass{color:green} .fail{color:red} pre{background:#f6f8fa;padding:12px;white-space:pre-wrap;border:1px solid #e1e4e8}</style>")
    html.append("</head><body>")
    html.append("<h1>Embedding Storage Smoke Report</h1>")
    html.append(f"<p>Generated: {datetime.now().isoformat()}</p>")
    html.append("<h2>Neo4j Counts by Tenant</h2>")
    for t in tenants:
        html.append(f"<h3>{t}</h3>")
        html.append("<table><tr><th>Component</th><th>Count</th></tr>")
        for comp, c in sorted(neo4j_counts.get(t, {}).items()):
            html.append(f"<tr><td>{comp}</td><td>{c}</td></tr>")
        html.append("</table>")
    html.append("<h2>Pinecone Namespaces</h2>")
    html.append("<ul>")
    for ns in namespaces:
        html.append(f"<li>{ns}</li>")
    html.append("</ul>")
    html.append("<h2>Sample Vector Metadata</h2>")
    for ns, matches in meta_samples.items():
        html.append(f"<h3>{ns}</h3>")
        if not matches:
            html.append("<p>No matches returned</p>")
        else:
            html.append("<table><tr><th>ID</th><th>Metadata Keys</th></tr>")
            for m in matches:
                mid = getattr(m, "id", None) or m.get("id", "")
                md = getattr(m, "metadata", None) or m.get("metadata", {})
                keys = list(md.keys()) if isinstance(md, dict) else []
                html.append(f"<tr><td>{mid}</td><td>{', '.join(keys)}</td></tr>")
            html.append("</table>")
    html.append("<h2>Local Embedding Cache Check</h2>")
    html.append(f"<p>Status: <span class='{('pass' if no_cache else 'fail')}'>{'No local cache files' if no_cache else 'Found local cache files'}</span></p>")
    html.append("</body></html>")
    html_path.write_text("\n".join(html), encoding="utf-8")

def main():
    os.environ["NODERAG_STORAGE_BACKEND"] = "cloud"
    ensure_dirs()
    tenants = ["tenant_alpha", "tenant_beta"]

    driver, db = connect_neo4j()
    index = pinecone_connect()

    for t in tenants:
        create_neo4j_nodes_edges(driver, db, t)

    created_namespaces = []
    for t in tenants:
        created_namespaces.extend(pinecone_upserts_for_tenant(index, t))

    neo4j_counts = query_neo4j_counts(driver, db, tenants)
    isolation_ok = verify_no_cross_tenant_edges(driver, db, tenants[0], tenants[1])

    all_namespaces = list(set(list_namespaces(index)))
    relevant_ns = [ns for ns in all_namespaces if any(ns.startswith(f"{t}_") for t in tenants)]
    meta_samples = {}
    for ns in relevant_ns:
        meta_samples[ns] = get_sample_vectors(index, ns, top_k=3)

    required_fields = {"tenant_id","interaction_id","interaction_type","account_id","timestamp","user_id","source_system"}
    for ns, matches in meta_samples.items():
        for m in matches:
            md = getattr(m, "metadata", None) or m.get("metadata", {})
            keys = set(md.keys()) if isinstance(md, dict) else set()
            if keys != required_fields:
                print(f"WARNING: namespace {ns} sample metadata keys != required 7 fields: {keys}", file=sys.stderr)

    no_cache = check_no_local_embedding_cache()

    csv_path = Path("test-reports/phase_4/wp0/smoke/embedding_storage_smoke.csv")
    html_path = Path("test-reports/phase_4/wp0/smoke/embedding_storage_smoke_report.html")
    write_csv(csv_path, neo4j_counts, relevant_ns)
    write_html(html_path, tenants, neo4j_counts, relevant_ns, meta_samples, no_cache)

    print(str(csv_path))
    print(str(html_path))
    print(f"Isolation OK: {isolation_ok}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
