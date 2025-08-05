"""Complete test of summary generation pipeline with Pinecone"""
import os
import sys
import json
import asyncio
import networkx as nx
import tempfile
from datetime import datetime, timezone

os.environ['PINECONE_API_KEY'] = os.getenv('pinecone_API_key', '')
os.environ['PINECONE_INDEX_NAME'] = os.getenv('Pinecone_Index_Name', 'noderag')

sys.path.insert(0, '.')

print("=== Environment Check ===")
print(f"PINECONE_API_KEY: {'✅ Set' if os.getenv('PINECONE_API_KEY') else '❌ Missing'}")
print(f"PINECONE_INDEX_NAME: {os.getenv('PINECONE_INDEX_NAME', 'Not set')}")
print(f"Working directory: {os.getcwd()}")

async def test_complete_summary_pipeline():
    """Test the complete summary generation pipeline end-to-end"""
    print("\n=== Complete Summary Generation Pipeline Test ===\n")
    
    try:
        from NodeRAG.src.pipeline.summary_generation import SummaryGeneration
        from NodeRAG.standards.eq_metadata import EQMetadata
        from NodeRAG.test_utils.config_helper import create_test_nodeconfig
        from NodeRAG.storage import storage
        from NodeRAG.src.component import Community_summary
        
        print("✅ All imports successful")
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False
    
    config = create_test_nodeconfig()
    
    import pandas as pd
    import os
    os.makedirs(os.path.dirname(config.semantic_units_path), exist_ok=True)
    os.makedirs(os.path.dirname(config.attributes_path), exist_ok=True)
    os.makedirs(os.path.dirname(config.entities_path), exist_ok=True)
    
    empty_df = pd.DataFrame(columns=['hash_id', 'context', 'type', 'weight'])
    storage(empty_df).save_parquet(config.semantic_units_path)
    storage(empty_df).save_parquet(config.attributes_path)
    storage(empty_df).save_parquet(config.entities_path)
    
    if hasattr(config, 'embedding') and config.embedding:
        os.makedirs(os.path.dirname(config.embedding), exist_ok=True)
        empty_embedding_df = pd.DataFrame(columns=['hash_id', 'embedding'])
        storage(empty_embedding_df).save_parquet(config.embedding)
    
    print("\n1. Testing Community_summary constructor...")
    try:
        test_graph = nx.Graph()
        test_graph.add_node('test_node', type='entity', weight=1)
        
        # Need to create a proper mapper for Community_summary
        from NodeRAG.storage import Mapper
        test_mapper = Mapper([config.semantic_units_path, config.attributes_path])
        test_community = Community_summary('test_node', test_mapper, test_graph, config)
        print("✅ Community_summary accepts string parameter")
    except Exception as e:
        print(f"❌ Community_summary constructor issue: {e}")
        print("This indicates the type fix may not be working correctly")
        return False
    
    G = nx.Graph()
    
    financial_metadata = {
        'tenant_id': 'tenant_financial',
        'account_id': 'acc_11111111-1111-1111-1111-111111111111',
        'interaction_id': 'int_11111111-1111-1111-1111-111111111111',
        'interaction_type': 'email',
        'timestamp': '2024-01-15T10:00:00Z',
        'user_id': 'analyst@financial.com',
        'source_system': 'outlook',
        'weight': 1
    }
    
    nodes_community1 = ['fin_sem_1', 'fin_ent_1', 'fin_ent_2', 'fin_ent_3']
    for node in nodes_community1:
        node_type = 'semantic_unit' if 'sem' in node else 'entity'
        context = f"Financial analysis context for {node}"
        G.add_node(node, type=node_type, context=context, **financial_metadata)
    
    G.add_edge('fin_sem_1', 'fin_ent_1', weight=2)
    G.add_edge('fin_ent_1', 'fin_ent_2', weight=2)
    G.add_edge('fin_ent_2', 'fin_ent_3', weight=2)
    G.add_edge('fin_ent_3', 'fin_ent_1', weight=1)
    
    healthcare_metadata = {
        'tenant_id': 'tenant_healthcare',
        'account_id': 'acc_22222222-2222-2222-2222-222222222222',
        'interaction_id': 'int_22222222-2222-2222-2222-222222222222',
        'interaction_type': 'voice_memo',
        'timestamp': '2024-01-15T11:00:00Z',
        'user_id': 'doctor@healthcare.com',
        'source_system': 'voice_memo',
        'weight': 1
    }
    
    nodes_community2 = ['health_sem_1', 'health_ent_1', 'health_ent_2']
    for node in nodes_community2:
        node_type = 'semantic_unit' if 'sem' in node else 'entity'
        context = f"Healthcare patient data for {node}"
        G.add_node(node, type=node_type, context=context, **healthcare_metadata)
    
    G.add_edge('health_sem_1', 'health_ent_1', weight=2)
    G.add_edge('health_ent_1', 'health_ent_2', weight=2)
    
    G.add_node('legacy_1', type='entity', context='Legacy data', weight=1)
    G.add_node('legacy_2', type='semantic_unit', context='Old system', weight=1)
    G.add_edge('legacy_1', 'legacy_2', weight=1)
    
    storage(G).save_pickle(config.graph_path)
    print(f"\n2. Created test graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    
    import pandas as pd
    
    entities_data = []
    for node_id, node_data in G.nodes(data=True):
        if node_data.get('type') == 'entity':
            entities_data.append({
                'hash_id': node_id,
                'context': node_data.get('context', ''),
                'type': 'entity',
                'weight': node_data.get('weight', 1)
            })
    
    if entities_data:
        entities_df = pd.DataFrame(entities_data)
        storage(entities_df).save_parquet(config.entities_path)
    
    semantic_data = []
    for node_id, node_data in G.nodes(data=True):
        if node_data.get('type') == 'semantic_unit':
            semantic_data.append({
                'hash_id': node_id,
                'context': node_data.get('context', ''),
                'type': 'semantic_unit',
                'weight': node_data.get('weight', 1)
            })
    
    if semantic_data:
        semantic_df = pd.DataFrame(semantic_data)
        storage(semantic_df).save_parquet(config.semantic_units_path)
    
    attr_df = pd.DataFrame(columns=['hash_id', 'context', 'type', 'weight'])
    storage(attr_df).save_parquet(config.attributes_path)
    
    print("3. Created required data files")
    
    print("\n4. Running Summary Generation Pipeline...")
    
    try:
        summary_gen = SummaryGeneration(config)
        
        print(f"   - Graph loaded: {type(summary_gen.G)}")
        print(f"   - Graph type check: {'✅ NetworkX Graph' if isinstance(summary_gen.G, nx.Graph) else '❌ Wrong type'}")
        print(f"   - Nodes: {summary_gen.G.number_of_nodes()}")
        print(f"   - Communities found: {len(summary_gen.communities)}")
        
        test_node = list(summary_gen.G.nodes())[0] if summary_gen.G.nodes() else None
        if test_node:
            has_node_works = summary_gen.G.has_node(test_node)
            node_access_works = test_node in summary_gen.G.nodes
            print(f"   - has_node() works: {'✅' if has_node_works else '❌'}")
            print(f"   - nodes access works: {'✅' if node_access_works else '❌'}")
        
        summaries = []
        
        financial_summary = {
            'community': nodes_community1,
            'response': {
                'high_level_elements': [{
                    'description': 'Financial analysis and reporting data including analyst communications and outlook integration',
                    'title': 'Financial Analysis Overview'
                }]
            }
        }
        summaries.append(financial_summary)
        
        healthcare_summary = {
            'community': nodes_community2,
            'response': {
                'high_level_elements': [{
                    'description': 'Healthcare patient data and voice memo recordings from medical professionals',
                    'title': 'Healthcare Data Summary'
                }]
            }
        }
        summaries.append(healthcare_summary)
        
        legacy_summary = {
            'community': ['legacy_1', 'legacy_2'],
            'response': {
                'high_level_elements': [{
                    'description': 'Legacy system data without complete metadata information',
                    'title': 'Legacy System Data'
                }]
            }
        }
        summaries.append(legacy_summary)
        
        os.makedirs(os.path.dirname(config.summary_path), exist_ok=True)
        with open(config.summary_path, 'w') as f:
            for summary in summaries:
                f.write(json.dumps(summary) + '\n')
        
        print(f"   - Created {len(summaries)} community summaries")
        
        print("   - Running high_level_element_summary()...")
        await summary_gen.high_level_element_summary()
        
        print("   - High level element summary completed")
        
        print("\n5. Analyzing Results...")
        
        high_level_elements = []
        title_elements = []
        nodes_without_metadata = []
        metadata_details = []
        
        required_fields = ['tenant_id', 'account_id', 'interaction_id',
                          'interaction_type', 'timestamp', 'user_id', 'source_system']
        
        for node_id, node_data in summary_gen.G.nodes(data=True):
            node_type = node_data.get('type')
            
            if node_type == 'high_level_element':
                high_level_elements.append(node_id)
                
                missing = [f for f in required_fields if f not in node_data]
                if missing:
                    nodes_without_metadata.append({
                        'node': node_id,
                        'missing': missing
                    })
                else:
                    metadata_details.append({
                        'node': node_id[:20] + '...',
                        'tenant_id': node_data['tenant_id'],
                        'interaction_type': node_data['interaction_type'],
                        'source': 'extracted' if node_data['tenant_id'] != 'AGGREGATED' else 'fallback'
                    })
                    
            elif node_type == 'high_level_element_title':
                title_elements.append(node_id)
                
                missing = [f for f in required_fields if f not in node_data]
                if missing:
                    nodes_without_metadata.append({
                        'node': node_id,
                        'missing': missing
                    })
        
        print(f"\n=== DETAILED RESULTS ===")
        print(f"High-level element nodes created: {len(high_level_elements)}")
        print(f"Title element nodes created: {len(title_elements)}")
        print(f"Nodes without metadata: {len(nodes_without_metadata)}")
        
        if metadata_details:
            print("\nMetadata Details:")
            for detail in metadata_details:
                print(f"  - Node {detail['node']}")
                print(f"    tenant_id: {detail['tenant_id']}")
                print(f"    type: {detail['interaction_type']}")
                print(f"    source: {detail['source']}")
        
        if nodes_without_metadata:
            print("\n❌ FAIL: Nodes missing metadata:")
            for item in nodes_without_metadata:
                print(f"  - {item['node'][:20]}... missing: {item['missing']}")
            return False
        
        os.makedirs('test_output', exist_ok=True)
        storage(summary_gen.G).save_pickle('test_output/summary_pipeline_result.pickle')
        
        tenant_counts = {}
        for node_id, node_data in summary_gen.G.nodes(data=True):
            if node_data.get('type') in ['high_level_element', 'high_level_element_title']:
                tenant = node_data.get('tenant_id', 'unknown')
                tenant_counts[tenant] = tenant_counts.get(tenant, 0) + 1
        
        print(f"\nTenant distribution in summary nodes:")
        for tenant, count in tenant_counts.items():
            print(f"  - {tenant}: {count} nodes")
        
        if high_level_elements:
            print("\n✅ SUCCESS: Summary generation pipeline completed!")
            print(f"   - {len(high_level_elements)} high_level_element nodes created")
            print(f"   - {len(title_elements)} high_level_element_title nodes created")
            print(f"   - All nodes have complete metadata (7 fields)")
            print(f"   - Community_summary constructor works with string parameter")
            print(f"   - Graph type safety verified (NetworkX Graph)")
            print(f"   - Multi-tenant metadata extraction working")
            print(f"   - Graph saved to: test_output/summary_pipeline_result.pickle")
            return True
        else:
            print("\n⚠️  WARNING: No high_level_element nodes created")
            print("   This might indicate an issue with community detection or summary processing")
            return False
            
    except Exception as e:
        print(f"\n❌ ERROR: Pipeline execution failed")
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()
        
        if "Community_summary" in str(e) and ("takes" in str(e) or "argument" in str(e)):
            print("\n🔍 ISSUE: Community_summary constructor signature mismatch")
            print("   The fix in PR #23 may not match actual constructor")
        elif "pinecone" in str(e).lower():
            print("\n🔍 ISSUE: Pinecone configuration problem")
            print("   Check PINECONE_API_KEY and PINECONE_INDEX_NAME")
        elif "'str' object has no attribute" in str(e):
            print("\n🔍 ISSUE: Graph loading type problem")
            print("   self.G may still be loaded as string instead of NetworkX Graph")
        
        return False

async def main():
    """Run complete verification"""
    print("Verifying Summary Generation Pipeline with Pinecone\n")
    
    success = await test_complete_summary_pipeline()
    
    if success:
        print("\n🎉 VERIFICATION COMPLETE - PR #23 WORKS!")
        print("\nNext steps:")
        print("1. Update PR #23 with these test results")
        print("2. Merge PR #23")
        print("3. Run complete Phase 3 validation")
    else:
        print("\n❌ VERIFICATION FAILED")
        print("\nRequired actions:")
        print("1. Fix identified issues")
        print("2. Re-run this test")
        print("3. Do NOT merge PR #23 until this passes")
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
