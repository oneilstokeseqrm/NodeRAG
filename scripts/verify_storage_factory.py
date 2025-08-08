#!/usr/bin/env python3
import os
import sys
import time
import warnings
from unittest import mock
from datetime import datetime
from pathlib import Path

def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def build_html(out_path: Path, content: str):
    ensure_dir(out_path)
    html = []
    html.append("<!DOCTYPE html><html><head><meta charset='utf-8'><title>StorageFactory Verification</title>")
    html.append("<style>body{font-family:Arial;margin:20px} pre{background:#f6f8fa;padding:12px;white-space:pre-wrap;border:1px solid #e1e4e8} .pass{color:green} .fail{color:red}</style>")
    html.append("</head><body>")
    html.append("<h1>StorageFactory Verification</h1>")
    html.append(f"<p>Generated: {datetime.now().isoformat()}</p>")
    html.append("<pre>")
    html.append(content)
    html.append("</pre>")
    html.append("</body></html>")
    out_path.write_text("\n".join(html), encoding="utf-8")

def main():
    log_lines = []
    log = lambda s: log_lines.append(s)

    try:
        from NodeRAG.storage.storage_factory import StorageFactory
    except Exception as e:
        print(f"Import error: {e}", file=sys.stderr)
        return 1

    config = {
        "config": {"main_folder": "/tmp/noderag", "language": "en", "chunk_size": 256},
        "model_config": {"model_name": "gpt-4o"},
        "embedding_config": {"model_name": "text-embedding-3-large", "dimension": 3072},
    }

    try:
        os.environ["NODERAG_STORAGE_BACKEND"] = "cloud"
        t0 = time.perf_counter()
        StorageFactory.initialize(config, backend_mode="cloud")
        neo1 = StorageFactory.get_graph_storage()
        pin1 = StorageFactory.get_embedding_storage()
        t1 = time.perf_counter()

        hc1_start = time.perf_counter()
        neo1_h = getattr(neo1, "health_check", None)
        pin1_h = getattr(pin1, "health_check", None)
        hc1 = {
            "neo4j": neo1_h() if callable(neo1_h) else "N/A",
            "pinecone": pin1_h() if callable(pin1_h) else "N/A",
        }
        hc1_end = time.perf_counter()

        neo2 = StorageFactory.get_graph_storage()
        pin2 = StorageFactory.get_embedding_storage()
        hc2_start = time.perf_counter()
        neo2_h = getattr(neo2, "health_check", None)
        pin2_h = getattr(pin2, "health_check", None)
        hc2 = {
            "neo4j": neo2_h() if callable(neo2_h) else "N/A",
            "pinecone": pin2_h() if callable(pin2_h) else "N/A",
        }
        hc2_end = time.perf_counter()

        same_instance = (id(neo1) == id(neo2)) and (id(pin1) == id(pin2))

        log(f"[Cloud] Initialization time: {(t1 - t0):.4f}s")
        log(f"[Cloud] Singleton check (id equal): {same_instance}")
        log(f"[Cloud] Neo4j Adapter IDs: {id(neo1)} , {id(neo2)}")
        log(f"[Cloud] Pinecone Adapter IDs: {id(pin1)} , {id(pin2)}")
        log(f"[Cloud] Health check #1 duration: {(hc1_end - hc1_start):.4f}s")
        log(f"[Cloud] Health check #2 duration: {(hc2_end - hc2_start):.4f}s")
        log(f"[Cloud] Health check #1 result: {hc1}")
        log(f"[Cloud] Health check #2 result: {hc2}")

        os.environ["NODERAG_STORAGE_BACKEND"] = "file"
        with mock.patch("neo4j.GraphDatabase.driver") as neo_drv, \
             mock.patch("pinecone.init") as pc_init, \
             mock.patch("pinecone.Index") as pc_index, \
             warnings.catch_warnings(record=True) as wlist:
            warnings.simplefilter("always")
            StorageFactory.initialize(config, backend_mode="file")
            _g = StorageFactory.get_graph_storage()
            _e = StorageFactory.get_embedding_storage()
            deprecations = [w for w in wlist if issubclass(w.category, DeprecationWarning)]

        log(f"[File] Deprecation warnings: {len(deprecations)}")
        log(f"[File] neo4j.GraphDatabase.driver calls: {neo_drv.call_count}")
        log(f"[File] pinecone.init calls: {pc_init.call_count}")
        log(f"[File] pinecone.Index calls: {pc_index.call_count}")

    except Exception as e:
        log(f"Error during verification: {e}")

    out_path = Path("test-reports/phase_4/wp0/factory/storage_factory_verification.html")
    build_html(out_path, "\n".join(log_lines))
    print(str(out_path))
    return 0

if __name__ == "__main__":
    sys.exit(main())
