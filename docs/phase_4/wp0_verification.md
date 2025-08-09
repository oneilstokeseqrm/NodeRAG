# WP‑0b — CI wiring & hardening for foundation verification

This doc explains the CI workflow and how to run the WP‑0 verification locally.

## What it does

Automates four checks:
- Schema validation and behavior (Neo4j)
- TenantContext isolation and resource limits (pytest)
- Cloud round‑trip smoke (Neo4j + Pinecone) with metadata checks
- StorageFactory singleton and file‑mode deprecation verification

No production code is modified.

## Local run

```
python3 -m venv .venv-wp0 && source .venv-wp0/bin/activate
pip install -r requirements/wp0.txt
export NODERAG_STORAGE_BACKEND=cloud
# export Neo4j and Pinecone envs:
# NEO4J_URI, NEO4J_USER or NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
# PINECONE_API_KEY, PINECONE_INDEX, PINECONE_ENVIRONMENT=us-east-1
python scripts/ci_run_wp0.py --out-dir test-reports/phase_4/wp0 --mode soft --cleanup
```

Outputs are saved under `test-reports/phase_4/wp0/` and include HTML/CSV for each check plus a combined index and `_ci_summary.md`.

## CI behavior

- PR/manual: soft mode (skips with summary if secrets are missing)
- Nightly: strict mode (fails if secrets missing or any check fails)
- Artifacts uploaded as `wp0-reports`

## Troubleshooting

- Missing secrets: PR soft mode will SKIP; nightly strict mode fails
- Pinecone v3: ensure `pinecone-client` is v3 and index exists; do not auto‑create
- ABI issues: use the pinned `requirements/wp0.txt` in an isolated venv
