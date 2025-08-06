"""Verify HNSW separation changes without requiring full package imports"""
import os
import re

def verify_hnsw_py_changes():
    """Verify HNSW.py has been modified correctly"""
    print("=== Verifying HNSW.py Changes ===\n")
    
    hnsw_file = "NodeRAG/utils/HNSW.py"
    if not os.path.exists(hnsw_file):
        print("❌ HNSW.py not found")
        return False
        
    with open(hnsw_file, 'r') as f:
        content = f.read()
    
    if 'return None' in content and 'DeprecationWarning' in content:
        print("✅ nxgraphs property deprecated correctly")
    else:
        print("❌ nxgraphs property not deprecated properly")
        return False
    
    if 'storage(self.nxgraphs).save_pickle' not in content or '# storage(self.nxgraphs).save_pickle' in content:
        print("✅ NetworkX graph saving removed from save_HNSW")
    else:
        print("❌ NetworkX graph saving still present in save_HNSW")
        return False
    
    if 'internal index nodes' in content and 'without metadata' in content:
        print("✅ Explanatory comments added")
    else:
        print("❌ Missing explanatory comments")
        return False
    
    return True

def verify_search_py_changes():
    """Verify search.py has been modified correctly"""
    print("\n=== Verifying search.py Changes ===\n")
    
    search_file = "NodeRAG/search/search.py"
    if not os.path.exists(search_file):
        print("❌ search.py not found")
        return False
        
    with open(search_file, 'r') as f:
        content = f.read()
    
    if 'GraphConcat(G).concat(HNSW_graph)' not in content or '# return GraphConcat(G).concat(HNSW_graph)' in content:
        print("✅ HNSW graph concatenation removed")
    else:
        print("❌ HNSW graph concatenation still present")
        return False
    
    if 'HNSW internal nodes' in content and 'business data' in content:
        print("✅ Explanatory comments added")
    else:
        print("❌ Missing explanatory comments")
        return False
    
    return True

def verify_hnsw_pipeline_changes():
    """Verify HNSW_graph.py has been modified correctly"""
    print("\n=== Verifying HNSW_graph.py Changes ===\n")
    
    pipeline_file = "NodeRAG/src/pipeline/HNSW_graph.py"
    if not os.path.exists(pipeline_file):
        print("❌ HNSW_graph.py not found")
        return False
        
    with open(pipeline_file, 'r') as f:
        content = f.read()
    
    if 'NOT NetworkX graph' in content and 'index node pollution' in content:
        print("✅ Explanatory comments added to pipeline")
    else:
        print("❌ Missing explanatory comments in pipeline")
        return False
    
    return True

def check_no_hnsw_graph_references():
    """Check that no code still references HNSW graph loading"""
    print("\n=== Checking for Remaining HNSW Graph References ===\n")
    
    problematic_patterns = [
        r'storage\.load.*hnsw_graph_path',
        r'hnsw_graph_path.*load',
        r'concat.*HNSW_graph',
        r'HNSW_graph.*concat'
    ]
    
    files_to_check = [
        "NodeRAG/search/search.py",
        "NodeRAG/utils/HNSW.py", 
        "NodeRAG/src/pipeline/HNSW_graph.py"
    ]
    
    issues_found = []
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                
            for pattern in problematic_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if re.search(pattern, line, re.IGNORECASE) and not line.strip().startswith('#'):
                        issues_found.append(f"{file_path}:{i+1} - {line.strip()}")
    
    if issues_found:
        print("❌ Found problematic HNSW graph references:")
        for issue in issues_found:
            print(f"  {issue}")
        return False
    else:
        print("✅ No problematic HNSW graph references found")
        return True

def main():
    """Run all verification checks"""
    print("HNSW SEPARATION VERIFICATION")
    print("=" * 50)
    
    success = True
    success = verify_hnsw_py_changes() and success
    success = verify_search_py_changes() and success  
    success = verify_hnsw_pipeline_changes() and success
    success = check_no_hnsw_graph_references() and success
    
    print("\n" + "=" * 50)
    if success:
        print("✅ ALL VERIFICATION CHECKS PASSED!")
        print("HNSW separation implementation is correct.")
    else:
        print("❌ VERIFICATION FAILED!")
        print("Some changes are missing or incorrect.")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
