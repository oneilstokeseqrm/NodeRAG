#!/usr/bin/env python3
import os, sys, subprocess, json
from pathlib import Path
from datetime import datetime

def _guard_no_artifacts():
    from pathlib import Path
    marker = "<" + "/old_str>"
    bad = []
    for p in Path("scripts").rglob("*.py"):
        try:
            if marker in p.read_text(encoding="utf-8"):
                bad.append(str(p))
        except Exception:
            continue
    if bad:
        print(f"Artifact marker found in: {', '.join(bad)}", file=sys.stderr)
        sys.exit(2)

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
    md.append(f"# WP-0b CI Summary")
    md.append(f"_Generated {datetime.utcnow().isoformat()}Z_")
    for k, v in status.items():
        ok = v.get("ok", False)
        note = v.get("note", "")
        md.append(f"- {'✅' if ok else '❌'} {k}: {note}")
        paths = v.get("paths")
        if paths:
            for label, p in paths.items():
                md.append(f"  - {label}: {p}")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "_ci_summary.md").write_text("\n".join(md), encoding="utf-8")</old_str>" in p.read_text(encoding="utf-8"):
                bad.append(str(p))
        except Exception:
            continue
    if bad:
        print("Artifact marker '</old_str>' found in: " + ", ".join(bad), file=sys.stderr)
        os.getenv("NEO4J_URI") or os.getenv("Neo4j_Credentials_NEO4J_URI"),
        (os.getenv("NEO4J_USER") or os.getenv("NEO4J_USERNAME") or os.getenv("Neo4j_Credentials_NEO4J_USERNAME")),
        os.getenv("NEO4J_PASSWORD") or os.getenv("Neo4j_Credentials_NEO4J_PASSWORD"),
    ])
    pc_ok = all([
        os.getenv("PINECONE_API_KEY") or os.getenv("pinecone_API_key"),
        os.getenv("PINECONE_INDEX") or os.getenv("Pinecone_Index_Name"),
    ])




def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--mode", choices=["soft","strict"], required=True)
    ap.add_argument("--cleanup", action="store_true")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tenants_for_cleanup = []
    _guard_no_artifacts()

    status = {}
    try:
        if not have_secrets():
            note = "Missing secrets; skipping in soft mode" if args.mode == "soft" else "Missing secrets"
            status["env"] = {"ok": args.mode == "soft", "note": note}
            write_summary(out_dir, status)
            sys.exit(0 if args.mode == "soft" else 1)

        short_sha = (os.getenv("GITHUB_SHA", "")[:7] or "local")
        run_id = os.getenv("GITHUB_RUN_ID", "")
        if args.mode == "strict" and run_id:
            tenant_a = f"wp0s_{run_id}_a"
            tenant_b = f"wp0s_{run_id}_b"
        else:
            tenant_a = f"wp0b_{short_sha}_a"
            tenant_b = f"wp0b_{short_sha}_b"
        tenants_for_cleanup = [tenant_a, tenant_b]

        os.environ["NODERAG_STORAGE_BACKEND"] = "cloud"

        baseline = None
        if args.mode == "strict":
            baseline_path = Path("ci/baselines/wp0_baseline.json")
            if not baseline_path.exists():
                status["baseline"] = {"ok": False, "note": f"Baseline file missing at {baseline_path}"}
                write_summary(out_dir, status)
                sys.exit(1)
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))

        schema_dir = out_dir / "schema"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_json = schema_dir / "neo4j_schema_alignment_report.json"
        schema_cmd = [
            sys.executable, "scripts/validate_neo4j_schema.py",
            "--out-html", str(schema_dir / "neo4j_schema_alignment_report.html"),
            "--out-csv", str(schema_dir / "neo4j_schema_alignment_report.csv"),
            "--out-json", str(schema_json),
        ]
        if args.mode == "strict":
            schema_cmd.append("--read-only")
        p = run(schema_cmd)
        schema_ok = p.returncode == 0 and ("Traceback (most recent call last):" not in ((p.stdout or "") + (p.stderr or "")))
        status["schema"] = {"ok": schema_ok, "note": "validator completed", "paths": {"json": str(schema_json)}}
        if args.mode == "strict":
            if not schema_ok:
                write_summary(out_dir, status)
                sys.exit(1)
            try:
                data = json.loads(schema_json.read_text(encoding="utf-8"))
                sc = (baseline or {}).get("schema", {})
                if int(data.get("constraints_total", 0)) < int(sc["min_constraints"]):
                    status["schema"]["ok"] = False
                    status["schema"]["note"] = f"constraints_total {data.get('constraints_total')} < {sc['min_constraints']}"
                    write_summary(out_dir, status)
                    sys.exit(1)
                if int(data.get("indexes_total", 0)) < int(sc["min_indexes"]):
                    status["schema"]["ok"] = False
                    status["schema"]["note"] = f"indexes_total {data.get('indexes_total')} < {sc['min_indexes']}"
                    write_summary(out_dir, status)
                    sys.exit(1)
                if int(data.get("legacy_node_indexes_total", 0)) > int(sc["max_legacy_node_indexes"]):
                    status["schema"]["ok"] = False
                    status["schema"]["note"] = f"legacy_node_indexes_total {data.get('legacy_node_indexes_total')} > {sc['max_legacy_node_indexes']}"
                    write_summary(out_dir, status)
                    sys.exit(1)
                present = set((c.get("label"), tuple(sorted(c.get("properties", [])))) for c in data.get("composite_constraints", []))
                for req in sc["required_composite_constraints"]:
                    tup = (req["label"], tuple(sorted(req["properties"])))
                    if tup not in present:
                        status["schema"]["ok"] = False
                        status["schema"]["note"] = f"missing required composite constraint for {req['label']}"
                        write_summary(out_dir, status)
                        sys.exit(1)
            except Exception as e:
                status["schema"]["ok"] = False
                status["schema"]["note"] = f"schema gating error: {e}"
                write_summary(out_dir, status)
                sys.exit(1)

        tenancy_dir = out_dir / "tenancy"
        tenancy_dir.mkdir(parents=True, exist_ok=True)
        if args.cleanup:
            try:
                run([
                    sys.executable, "scripts/smoke_cloud_roundtrip.py",
                    "--tenant-a", tenant_a, "--tenant-b", tenant_b,
                    "--out-dir", str(out_dir / "smoke"),
                    "--cleanup-only"
                ])
            except Exception:
                pass
        ten_json = tenancy_dir / "tenancy_validation_report.json"
        p2 = run([sys.executable, "scripts/build_tenancy_validation_report.py",
                  "--out", str(tenancy_dir / "tenancy_validation_report.html"),
                  "--out-json", str(ten_json)])
        ten_ok = (p2.returncode == 0) and ("Traceback (most recent call last):" not in ((p2.stdout or "") + (p2.stderr or "")))
        status["tenancy"] = {"ok": ten_ok, "note": "tenancy report generated", "paths": {"json": str(ten_json)}}
        if args.mode == "strict":
            if not ten_ok:
                write_summary(out_dir, status)
                sys.exit(1)
            try:
                data = json.loads(ten_json.read_text(encoding="utf-8"))
                if not data.get("pytest_passed", False):
                    status["tenancy"]["ok"] = False
                    status["tenancy"]["note"] = "pytest did not pass"
                    write_summary(out_dir, status)
                    sys.exit(1)
                ten_base = (baseline or {}).get("tenancy", {})
                if ten_base.get("require_enforce_limits", False) and not data.get("enforce_limits_enabled", False):
                    status["tenancy"]["ok"] = False
                    status["tenancy"]["note"] = "enforce limits not enabled"
                    write_summary(out_dir, status)
                    sys.exit(1)
            except Exception as e:
                status["tenancy"]["ok"] = False
                status["tenancy"]["note"] = f"tenancy gating error: {e}"
                write_summary(out_dir, status)
                sys.exit(1)

        smoke_dir = out_dir / "smoke"
        smoke_dir.mkdir(parents=True, exist_ok=True)
        smoke_json = smoke_dir / "smoke_summary.json"
        smoke_cmd = [
            sys.executable, "scripts/smoke_cloud_roundtrip.py",
            "--tenant-a", tenant_a, "--tenant-b", tenant_b,
            "--out-dir", str(smoke_dir),
            "--assert-metadata-7", "--assert-namespaces", "--no-cache-files",
            "--out-json", str(smoke_json)
        ]
        p = run(smoke_cmd)
        smoke_ok = p.returncode == 0 and ("Traceback (most recent call last):" not in ((p.stdout or "") + (p.stderr or "")))
        status["smoke"] = {"ok": smoke_ok, "note": f"tenants: {tenant_a}, {tenant_b}", "paths": {"json": str(smoke_json)}}
        if args.mode == "strict":
            if not smoke_ok:
                write_summary(out_dir, status)
                sys.exit(1)
            try:
                data = json.loads(smoke_json.read_text(encoding="utf-8"))
                sb = (baseline or {}).get("smoke", {})
                required_meta = set(sb["required_metadata_keys"])
                if data.get("local_cache_detected", False):
                    status["smoke"]["ok"] = False
                    status["smoke"]["note"] = "local embedding cache detected"
                    write_summary(out_dir, status)
                    sys.exit(1)
                for t in data.get("tenants", []):
                    if int(t.get("neo4j_nodes", 0)) < int(sb["min_nodes_per_tenant"]):
                        status["smoke"]["ok"] = False
                        status["smoke"]["note"] = f"tenant {t.get('tenant_id')} has too few nodes"
                        write_summary(out_dir, status)
                        sys.exit(1)
                    suffixes = sb["required_namespaces_suffixes"]
                    for suf in suffixes:
                        if not any(ns.endswith(suf) for ns in t.get("namespaces", [])):
                            status["smoke"]["ok"] = False
                            status["smoke"]["note"] = f"missing namespace suffix {suf} for {t.get('tenant_id')}"
                            write_summary(out_dir, status)
                            sys.exit(1)
                for ns, info in (data.get("namespaces") or {}).items():
                    keys = set(info.get("metadata_keys") or [])
                    if sb.get("forbid_text_metadata", False) and "text" in keys:
                        status["smoke"]["ok"] = False
                        status["smoke"]["note"] = f"'text' key present in metadata for {ns}"
                        write_summary(out_dir, status)
                        sys.exit(1)
                    if keys and keys != required_meta:
                        status["smoke"]["ok"] = False
                        status["smoke"]["note"] = f"metadata keys mismatch in {ns}: {sorted(list(keys))}"
                        write_summary(out_dir, status)
                        sys.exit(1)
            except Exception as e:
                status["smoke"]["ok"] = False
                status["smoke"]["note"] = f"smoke gating error: {e}"
                write_summary(out_dir, status)
                sys.exit(1)

        factory_dir = out_dir / "factory"
        factory_dir.mkdir(parents=True, exist_ok=True)
        fact_json = factory_dir / "storage_factory_summary.json"
        p = run([sys.executable, "scripts/verify_storage_factory.py",
                 "--out", str(factory_dir / "storage_factory_verification.html"),
                 "--out-json", str(fact_json)])
        fact_ok = p.returncode == 0 and ("Traceback (most recent call last):" not in ((p.stderr or "") + (p.stdout or "")))
        status["factory"] = {"ok": fact_ok, "note": "singleton + cache + file-mode deprecation", "paths": {"json": str(fact_json)}}
        if args.mode == "strict":
            if not fact_ok:
                write_summary(out_dir, status)
                sys.exit(1)
            try:
                data = json.loads(fact_json.read_text(encoding="utf-8"))
                fb = (baseline or {}).get("factory", {})
                if fb.get("require_deprecation_warning_in_file_mode", False) and not data.get("deprecation_warning_seen", False):
                    status["factory"]["ok"] = False
                    status["factory"]["note"] = "deprecation warning not seen in file mode"
                    write_summary(out_dir, status)
                    sys.exit(1)
                if data.get("cloud_connections_attempted_in_file_mode", False):
                    status["factory"]["ok"] = False
                    status["factory"]["note"] = "cloud connections attempted in file mode"
                    write_summary(out_dir, status)
                    sys.exit(1)
            except Exception as e:
                status["factory"]["ok"] = False
                status["factory"]["note"] = f"factory gating error: {e}"
                write_summary(out_dir, status)
                sys.exit(1)

        p = run([sys.executable, "scripts/build_wp0_index.py",
                 "--in-dir", str(out_dir),
                 "--out", str(out_dir / "wp0_foundation_verification.html"),
                 "--ci"])
        idx_ok = p.returncode == 0
        status["combined_index"] = {"ok": idx_ok, "note": "index generated"}

        write_summary(out_dir, status)
        sys.exit(0 if args.mode == "soft" else (0 if all(v["ok"] for v in status.values()) else 1))
    finally:
        try:
            if args.cleanup and tenants_for_cleanup:
                smoke_dir = out_dir / "smoke"
                smoke_dir.mkdir(parents=True, exist_ok=True)
                run([sys.executable, "scripts/smoke_cloud_roundtrip.py",
                     "--tenant-a", tenants_for_cleanup[0], "--tenant-b", tenants_for_cleanup[1],
                     "--out-dir", str(smoke_dir),
                     "--cleanup-only"])
                ns_list = []
                for t in tenants_for_cleanup:
                    ns_list.extend([f"{t}_entities", f"{t}_semantic_units"])
                cleanup_obj = {
                    "tenants_deleted": tenants_for_cleanup,
                    "namespaces_deleted": ns_list
                }
                (out_dir / "cleanup.json").write_text(json.dumps(cleanup_obj), encoding="utf-8")
        except Exception:
            pass

if __name__ == "__main__":
    main()
