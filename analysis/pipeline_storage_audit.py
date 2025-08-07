"""
Audit all storage operations in Graph_pipeline and related files
"""
import ast
import os
from pathlib import Path
from typing import List, Dict, Any
import json

class StorageOperationAuditor:
    """Identify all storage operations that need migration"""
    
    def __init__(self):
        self.operations = {
            'file_reads': [],
            'file_writes': [],
            'pickle_operations': [],
            'parquet_operations': [],
            'json_operations': [],
            'storage_wrapper_calls': [],
            'direct_file_operations': []
        }
    
    def audit_pipeline_files(self) -> Dict[str, Any]:
        """Audit all pipeline files for storage operations"""
        pipeline_dir = Path('NodeRAG/src/pipeline')
        
        for py_file in pipeline_dir.glob('*.py'):
            if py_file.name.startswith('__'):
                continue
                
            with open(py_file, 'r') as f:
                source = f.read()
                tree = ast.parse(source)
            
            self._analyze_file(py_file.name, tree)
        
        return self.generate_audit_report()
    
    def _analyze_file(self, filename: str, tree: ast.AST):
        """Analyze a single file for storage operations"""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if (isinstance(node.func, ast.Attribute) and 
                    isinstance(node.func.value, ast.Call) and
                    isinstance(node.func.value.func, ast.Name) and
                    node.func.value.func.id == 'storage'):
                    self.operations['storage_wrapper_calls'].append({
                        'file': filename,
                        'line': node.lineno,
                        'method': node.func.attr,
                        'context': ast.unparse(node)
                    })
                
                elif (isinstance(node.func, ast.Attribute) and 
                      isinstance(node.func.value, ast.Name) and
                      node.func.value.id == 'storage'):
                    self.operations['storage_wrapper_calls'].append({
                        'file': filename,
                        'line': node.lineno,
                        'method': node.func.attr,
                        'context': ast.unparse(node)
                    })
                
                elif isinstance(node.func, ast.Name) and node.func.id == 'open':
                    self.operations['direct_file_operations'].append({
                        'file': filename,
                        'line': node.lineno,
                        'type': 'open',
                        'context': ast.unparse(node)
                    })
                
                elif (isinstance(node.func, ast.Attribute) and 
                      isinstance(node.func.value, ast.Name) and
                      node.func.value.id == 'json'):
                    self.operations['json_operations'].append({
                        'file': filename,
                        'line': node.lineno,
                        'method': node.func.attr,
                        'context': ast.unparse(node)
                    })
    
    def generate_audit_report(self) -> Dict[str, Any]:
        """Generate comprehensive audit report"""
        total_operations = sum(len(ops) for ops in self.operations.values() if isinstance(ops, list))
        
        report = {
            'total_storage_operations': total_operations,
            'breakdown': {
                'storage_wrapper_calls': len(self.operations['storage_wrapper_calls']),
                'direct_file_operations': len(self.operations['direct_file_operations']),
                'json_operations': len(self.operations['json_operations'])
            },
            'operations': self.operations,
            'migration_required': total_operations > 0,
            'files_to_migrate': list(set(op['file'] for ops in self.operations.values() 
                                       if isinstance(ops, list) for op in ops))
        }
        
        with open('pipeline_storage_audit.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        return report

if __name__ == "__main__":
    auditor = StorageOperationAuditor()
    report = auditor.audit_pipeline_files()
    print(f"Found {report['total_storage_operations']} storage operations across {len(report['files_to_migrate'])} files")
    print("Audit report saved to pipeline_storage_audit.json")
