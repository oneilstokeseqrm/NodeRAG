#!/usr/bin/env python3
import sys
print(f"Python path: {sys.path}")
print(f"Python version: {sys.version}")

test_imports = [
    ("EQMetadata", "from NodeRAG.standards.eq_metadata import EQMetadata"),
    ("Neo4j Adapter", "from NodeRAG.storage.neo4j_adapter import Neo4jAdapter"),
    ("Pinecone Adapter", "from NodeRAG.storage.pinecone_adapter import PineconeAdapter"),
    ("Transaction Manager", "from NodeRAG.storage.transactions.transaction_manager import TransactionManager"),
    ("Components", "from NodeRAG.src.component import Entity, document, Semantic_unit"),
]

for name, import_statement in test_imports:
    try:
        exec(import_statement)
        print(f"✅ {name}: Import successful")
    except ImportError as e:
        print(f"❌ {name}: Import failed - {e}")
    except Exception as e:
        print(f"❌ {name}: Unexpected error - {type(e).__name__}: {e}")
