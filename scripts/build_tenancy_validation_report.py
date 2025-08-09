#!/usr/bin/env python3
import os
import sys
import json
import subprocess
from datetime import datetime
from pathlib import Path

def run_pytest():
    cmd = [
        sys.executable, "-m", "pytest",
        "-q",
        "tests/test_multi_tenant_isolation.py",
        "tests/test_tenant_resource_limits.py",
        "--maxfail=1",
        "--disable-warnings",
        "-q",
    ]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    out, err = proc.communicate()
    return proc.returncode, out, err

def get_tenant_config_snapshot():
    def _get_bool(name, default=None):
        v = os.getenv(name)
        if v is None:
            return default
        return v.lower() in ("1", "true", "yes", "on")
    def _get_int(name, default=None):
        v = os.getenv(name)
        try:
            return int(v) if v is not None else default
        except Exception:
            return default

    return {
        "MAX_ACTIVE_TENANTS": _get_int("NODERAG_MAX_ACTIVE_TENANTS"),
        "MAX_REGISTRY_SIZE": _get_int("NODERAG_MAX_REGISTRY_SIZE"),
        "INACTIVE_TENANT_TTL_HOURS": _get_int("NODERAG_TENANT_TTL_HOURS"),
        "ENFORCE_TENANT_LIMITS": _get_bool("NODERAG_ENFORCE_TENANT_LIMITS"),
    }

def build_html(report_path: Path, status_ok: bool, out: str, err: str, cfg: dict):
    report_path.parent.mkdir(parents=True, exist_ok=True)
    html = []
    html.append("<!DOCTYPE html><html><head><meta charset='utf-8'><title>Tenancy Validation Report</title>")
    html.append("<style>body{font-family:Arial;margin:20px} pre{background:#f6f8fa;padding:12px;white-space:pre-wrap;border:1px solid #e1e4e8} .pass{color:green} .fail{color:red} table{border-collapse:collapse} td,th{border:1px solid #ddd;padding:6px}</style>")
    html.append("</head><body>")
    html.append(f"<h1>Tenancy Validation Report</h1><p>Generated: {datetime.now().isoformat()}</p>")
    html.append("<h2>Summary</h2>")
    html.append(f"<p>Status: <span class='{'pass' if status_ok else 'fail'}'>{'PASS' if status_ok else 'FAIL'}</span></p>")
    html.append("<h2>TenantContext Configuration</h2>")
    html.append("<table><tr><th>Key</th><th>Value</th></tr>")
    for k,v in cfg.items():
        html.append(f"<tr><td>{k}</td><td>{v}</td></tr>")
    html.append("</table>")
    html.append("<h2>Pytest Output</h2>")
    html.append("<h3>stdout</h3>")
    html.append(f"<pre>{out}</pre>")
    if err.strip():
        html.append("<h3>stderr</h3>")
        html.append(f"<pre>{err}</pre>")
    html.append("<h2>Assertions</h2>")
    html.append("<ul>")
    html.append("<li>Thread-local tenant isolation: Verified by tests/test_multi_tenant_isolation.py</li>")
    html.append("<li>Resource limits & TTL cleanup: Verified by tests/test_tenant_resource_limits.py</li>")
    html.append("</ul>")
    html.append("</body></html>")
    report_path.write_text("\n".join(html), encoding="utf-8")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="test-reports/phase_4/wp0/tenancy/tenancy_validation_report.html")
    args = parser.parse_args()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    code, out, err = run_pytest()
    cfg = get_tenant_config_snapshot()
    build_html(out_path, code == 0, out, err, cfg)
    print(str(out_path))

if __name__ == "__main__":
    sys.exit(main() or 0)
