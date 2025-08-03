#!/usr/bin/env python3
"""Test direct component imports to avoid igraph conflicts"""

import sys
sys.path.insert(0, '.')

try:
    from NodeRAG.standards.eq_metadata import EQMetadata
    print("✅ EQMetadata import successful")
    
    from NodeRAG.src.component.unit import Unit_base
    print("✅ Unit_base import successful")
    
    from NodeRAG.src.component.entity import Entity
    print("✅ Entity import successful")
    
    from NodeRAG.src.component.document import document
    print("✅ document import successful")
    
    from NodeRAG.src.component.semantic_unit import Semantic_unit
    print("✅ Semantic_unit import successful")
    
    from NodeRAG.src.component.text_unit import Text_unit
    print("✅ Text_unit import successful")
    
    from NodeRAG.src.component.attribute import Attribute
    print("✅ Attribute import successful")
    
    print("\n🎉 All direct component imports successful!")
    
except Exception as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)
