#!/usr/bin/env python3
import os, sys, subprocess, json
from pathlib import Path
from datetime import datetime

def have_secrets():
    neo_ok = all([
        os.getenv("NEO4J_URI") or os.getenv("Neo4j_Credentials_NEO4J_URI"),
        (os.getenv("NEO4J_USER") or os.getenv("NEO4J_USERNAME") or os.getenv("Neo4j_Credentials_NEO4J_USERNAME")),
        os.getenv("NEO4J_PASSWORD") or os.getenv("Neo4j_Credentials_NEO4J_PASSWORD"),
    ])
    pc_ok = all([
        os.getenv("PINECONE_API_KEY") or os.getenv("pinecone_API_key"),
        os.getenv("PINECONE_INDEX") or os.getenv("Pinecone_Index_Name"),
    ])
    return neo_ok and pc_ok

def run(cmd, cwd=".", check=True):
    p = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    return p

def write_summary(out_dir: Path, status: dict):
    md = []
    md.append(f"# WP‑0b CI Summary")
    md.append(f"_Generated {datetime.utcnow().isoformat()}Z_")
    for k,v in status.items():
        ok = v.get("ok", False)
        note = v.get("note","")
        md.append(f"- {'✅' if ok else '❌'} {k}: {note}")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "_ci_summary.md").write_text("\n".join(md), encoding="utf-8")

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--mode", choices=["soft","strict"], required=True)
    ap.add_argument("--cleanup", action="store_true")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    status = {}

    if not have_secrets():
        note = "Missing secrets; skipping in soft mode" if args.mode=="soft" else "Missing secrets"
        status["env"] = {"ok": args.mode=="soft", "note": note}
        write_summary(out_dir, status)
        sys.exit(0 if args.mode=="soft" else 1)

    short_sha = (os.getenv("GITHUB_SHA","")[:7] or "local")
    tenant_a = f"wp0b_{short_sha}_a"
    tenant_b = f"wp0b_{short_sha}_b"

    os.environ["NODERAG_STORAGE_BACKEND"] = "cloud"

    schema_dir = out_dir / "schema"
    schema_dir.mkdir(parents=True, exist_ok=True)
    p = run([
        sys.executable, "scripts/validate_neo4j_schema.py",
        "--out-html", str(schema_dir/"neo4j_schema_alignment_report.html"),
        "--out-csv", str(schema_dir/"neo4j_schema_alignment_report.csv"),
        "--out-json", str(schema_dir/"neo4j_schema_alignment_report.json"),
    ])
    schema_ok = p.returncode == 0
    status["schema"] = {"ok": schema_ok, "note": "validator completed"}
    if args.mode=="strict" and not schema_ok:
        write_summary(out_dir, status); sys.exit(1)

    tenancy_dir = out_dir / "tenancy"
    tenancy_dir.mkdir(parents=True, exist_ok=True)
    p1 = run([sys.executable, "-m", "pytest", "-q",
              "tests/test_multi_tenant_isolation.py", "tests/test_tenant_resource_limits.py",
              "--maxfail=1", "--disable-warnings"])
    p2 = run([sys.executable, "scripts/build_tenancy_validation_report.py",
              "--out", str(tenancy_dir/"tenancy_validation_report.html")])
    ten_ok = (p1.returncode == 0 and p2.returncode == 0)
    status["tenancy"] = {"ok": ten_ok, "note": "13 tests expected to pass"}
    if args.mode=="strict" and not ten_ok:
        write_summary(out_dir, status); sys.exit(1)

    smoke_dir = out_dir / "smoke"
    smoke_dir.mkdir(parents=True, exist_ok=True)
    smoke_cmd = [
        sys.executable, "scripts/smoke_cloud_roundtrip.py",
        "--tenant-a", tenant_a, "--tenant-b", tenant_b,
        "--out-dir", str(smoke_dir),
        "--assert-metadata-7", "--assert-namespaces", "--no-cache-files",
    ]
    if args.cleanup:
        smoke_cmd.append("--cleanup")
    p = run(smoke_cmd)
    smoke_ok = p.returncode == 0
    status["smoke"] = {"ok": smoke_ok, "note": f"tenants: {tenant_a}, {tenant_b}"}
    if args.mode=="strict" and not smoke_ok:
        write_summary(out_dir, status); sys.exit(1)

    factory_dir = out_dir / "factory"
    factory_dir.mkdir(parents=True, exist_ok=True)
    p = run([sys.executable, "scripts/verify_storage_factory.py"])
    fact_ok = p.returncode == 0
    status["factory"] = {"ok": fact_ok, "note": "singleton + cache + file-mode deprecation"}
    if args.mode=="strict" and not fact_ok:
        write_summary(out_dir, status); sys.exit(1)

    p = run([sys.executable, "scripts/build_wp0_index.py",
             "--in-dir", str(out_dir),
             "--out", str(out_dir/"wp0_foundation_verification.html"),
             "--ci"])
    idx_ok = p.returncode == 0
    status["combined_index"] = {"ok": idx_ok, "note": "index generated"}

    write_summary(out_dir, status)
    sys.exit(0 if args.mode=="soft" else (0 if all(v["ok"] for v in status.values()) else 1))

if __name__ == "__main__":
    main()
