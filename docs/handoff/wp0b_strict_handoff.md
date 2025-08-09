# WP-0b-strict Handoff (resume in new thread)

## Scope & Goal
Add strict CI gating for schema drift, tenancy regressions, storage routing, and cloud round-trip failures via JSON gates and a fixed baseline. No runtime code changes.

## Prior PRs
- #31, #32, #33, #34 (merged)

## Current Branch
- handoff/wp0b-strict-20250809-122635

## Modified Paths (full git paths)
- scripts/validate_neo4j_schema.py
- scripts/build_tenancy_validation_report.py
- scripts/smoke_cloud_roundtrip.py
- scripts/verify_storage_factory.py
- scripts/ci_run_wp0.py
- .github/workflows/wp0_verification_strict.yml
- ci/baselines/wp0_baseline.json
- docs/ci/wp0_strict.md
- requirements/wp0.txt

## How to Run (copy/paste)
```bash
python3 -m venv .venv-wp0 && source .venv-wp0/bin/activate
pip install -r requirements/wp0.txt

# Soft (no secrets)
python scripts/ci_run_wp0.py --mode soft --out-dir ./test-reports/phase_4/wp0 --cleanup || true

# Strict (requires secrets; optional here)
export NEO4J_URI=...
export NEO4J_USER=...   # fallback to NEO4J_USERNAME supported
export NEO4J_PASSWORD=...
export PINECONE_API_KEY=...
export PINECONE_INDEX=...
export PINECONE_ENVIRONMENT=us-east-1
export NODERAG_ENFORCE_TENANT_LIMITS=true
python scripts/ci_run_wp0.py --mode strict --out-dir ./test-reports/phase_4/wp0_strict --cleanup || true
```

Ephemeral Tenants / Namespaces
- Tenants: wp0b_a, wp0b_b
- Pinecone namespaces: {tenant_id}_{component_type}

## Baseline Gates (strict)
- Composite (tenant_id,node_id) for: Entity, SemanticUnit, TextChunk, Attribute, Community, Summary, HighLevelElement
- Counts: constraints ≥ baseline.min_constraints; indexes ≥ baseline.min_indexes; legacy :Node indexes ≤ baseline.max_legacy_node_indexes
- Tenancy: pytest pass; enforce limits true
- Smoke: required namespaces exist; vectors have exactly these 7 metadata keys (no text):
  tenant_id, account_id, interaction_id, interaction_type, timestamp, user_id, source_system
- Factory: singletons stable; file mode deprecation warning; no cloud connections in file mode

## Status Snapshot
- JSON outputs implemented for tenancy, smoke, factory; schema JSON enriched but has a trailing artifact
- Soft run artifacts: test-reports/phase_4/wp0_handoff_20250809-122635/
- Strict run artifacts: absent (not executed during handoff)
- _ci_summary.md: absent due to soft run failure early
- Known issues observed in this thread:
  - scripts/validate_neo4j_schema.py: stray </old_str> artifact and orphaned triple-quoted span near “Validation Results”
  - scripts/ci_run_wp0.py: injected artifact tail after write_summary write_text, causing SyntaxError

## Blockers & Repro
- Blocker 1:
  - File/lines: scripts/ci_run_wp0.py (around summary write; early function body)
  - Command: python -m py_compile scripts/ci_run_wp0.py
  - Error: SyntaxError: unterminated string literal at the write_text line (artifact tail)
  - Notes: Do not fix in handoff; remove the injected `</old_str>` tail and restore normal block when resuming.

- Blocker 2:
  - File/lines: scripts/validate_neo4j_schema.py (after composite print; lines ~334–349)
  - Command: python -m py_compile scripts/validate_neo4j_schema.py
  - Error: SyntaxError due to stray `</old_str>` and orphaned triple-quoted text
  - Notes: Do not fix in handoff; remove artifact, restore try/except/finally, ensure --read-only is honored, and normalize composite properties.

## Next Steps (cut list)
1. Remove stray artifacts in schema validator and CI runner; add --read-only to schema and pass it in strict mode.
2. Strict runner: pre-run file scan to guard for artifact markers; fail-fast gates; always write _ci_summary.md; cleanup in finally.
3. Smoke: deterministic fetch of known IDs with retries; exact 7 metadata keys; forbid "text"; include pinecone client version and index name in JSON.
4. Wire strict workflow to protected contexts; ensure NODERAG_ENFORCE_TENANT_LIMITS=true in step env; upload artifacts and append job summary.
5. Validate all gates on main/nightly; confirm cleanup removed ephemeral tenants and namespaces.

## Cleanup
- Runner supports --cleanup; manual fallback:
  - Neo4j: MATCH (n {tenant_id:$t}) DETACH DELETE n; MATCH ()-[r:RELATIONSHIP {tenant_id:$t}]-() DELETE r
  - Pinecone: index.delete(delete_all=True, namespace=ns) for ns starting with each tenant prefix

## Artifacts
- Zip to attach on Draft PR: wp0b_strict_artifacts_20250809-122635.zip
- Contents index: see test-reports/phase_4/wp0_handoff_20250809-122635/_files.txt
