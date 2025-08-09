# WP0 Strict CI: Gated Verification and Drift Detection

This workflow enforces strict gating on:
- Neo4j schema baselines and composite constraints
- Tenancy isolation and resource limit enforcement
- Cloud round‑trip (Neo4j + Pinecone) with exact metadata keys and no local cache
- StorageFactory singleton stability and deprecation behavior

Baselines are defined in ci/baselines/wp0_baseline.json.

## Triggers
- push to main and release/**
- merge_group
- workflow_dispatch
- nightly schedule

Secrets are only used in this strict workflow; PRs from forks are not targeted by this workflow.

## Running locally

- Create an environment and install:
  - python -m venv .venv-wp0 && source .venv-wp0/bin/activate
  - pip install -r requirements/wp0.txt
- Export secrets:
  - NEO4J_URI, NEO4J_USER or NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
  - PINECONE_API_KEY, PINECONE_INDEX, PINECONE_ENVIRONMENT=us-east-1
- Run:
  - python scripts/ci_run_wp0.py --mode strict --out-dir /tmp/wp0_strict --cleanup

Artifacts are written under the output directory (HTML/CSV/JSON) with a summary markdown file `_ci_summary.md`.

## Adjusting baselines

If a legitimate change modifies the expected schema or thresholds, update ci/baselines/wp0_baseline.json in the same PR and call out the change in the PR description.
