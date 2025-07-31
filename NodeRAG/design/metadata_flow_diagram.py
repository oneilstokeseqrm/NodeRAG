"""
Generate visual diagram of metadata flow through NodeRAG
Run: python metadata_flow_diagram.py
"""
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

def create_metadata_flow_diagram():
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    
    doc_color = '#E8F4FD'
    semantic_color = '#B8E0FF'
    entity_color = '#FFE4B5'
    rel_color = '#FFB6C1'
    attr_color = '#DDA0DD'
    comm_color = '#98FB98'
    
    doc_box = FancyBboxPatch((1, 8), 12, 1.5, 
                              boxstyle="round,pad=0.1",
                              facecolor=doc_color, edgecolor='black', linewidth=2)
    ax.add_patch(doc_box)
    ax.text(7, 8.75, 'DOCUMENT\n(All 8 Required Fields)', 
            ha='center', va='center', fontsize=12, weight='bold')
    
    semantic_positions = [(1, 6), (5, 6), (9, 6)]
    for i, pos in enumerate(semantic_positions):
        box = FancyBboxPatch(pos, 3.5, 1.2,
                             boxstyle="round,pad=0.1",
                             facecolor=semantic_color, edgecolor='black')
        ax.add_patch(box)
        ax.text(pos[0]+1.75, pos[1]+0.6, f'Semantic Unit {i+1}\n(All fields + chunk)',
                ha='center', va='center', fontsize=10)
    
    entity_positions = [(0.5, 4), (2.5, 4), (5.5, 4), (7, 4), (10, 4)]
    entity_names = ['Entity A', 'Entity B', 'Entity A', 'Entity C', 'Entity B']
    for pos, name in zip(entity_positions, entity_names):
        color = entity_color if 'A' in name or 'C' in name else '#FFDAB9'
        box = FancyBboxPatch(pos, 2, 0.8,
                             boxstyle="round,pad=0.05",
                             facecolor=color, edgecolor='black')
        ax.add_patch(box)
        ax.text(pos[0]+1, pos[1]+0.4, f'{name}\n(No text)',
                ha='center', va='center', fontsize=9)
    
    rel_box = FancyBboxPatch((3.5, 4), 2, 0.8,
                             boxstyle="round,pad=0.05",
                             facecolor=rel_color, edgecolor='black')
    ax.add_patch(rel_box)
    ax.text(4.5, 4.4, 'Rel A→B\n(No text)',
            ha='center', va='center', fontsize=9)
    
    attr_positions = [(1, 2), (6, 2), (10, 2)]
    attr_names = ['Attr(Entity A)', 'Attr(Entity B)', 'Attr(Entity C)']
    for pos, name in zip(attr_positions, attr_names):
        box = FancyBboxPatch(pos, 3, 0.8,
                             boxstyle="round,pad=0.05",
                             facecolor=attr_color, edgecolor='black')
        ax.add_patch(box)
        ax.text(pos[0]+1.5, pos[1]+0.4, f'{name}\n(Aggregated IDs)',
                ha='center', va='center', fontsize=9)
    
    comm_box = FancyBboxPatch((4, 0.2), 6, 1,
                              boxstyle="round,pad=0.1",
                              facecolor=comm_color, edgecolor='black', linewidth=2)
    ax.add_patch(comm_box)
    ax.text(7, 0.7, 'COMMUNITY\n(tenant_id + aggregated IDs)',
            ha='center', va='center', fontsize=11, weight='bold')
    
    for pos in semantic_positions:
        arrow = FancyArrowPatch((7, 8), (pos[0]+1.75, pos[1]+1.2),
                                arrowstyle='->', mutation_scale=20,
                                color='black', alpha=0.6)
        ax.add_patch(arrow)
    
    connections = [
        ((2.75, 6), (1.5, 4.8)),
        ((2.75, 6), (3.5, 4.8)),
        ((6.75, 6), (6.5, 4.8)),
        ((10.75, 6), (11, 4.8))
    ]
    for start, end in connections:
        arrow = FancyArrowPatch(start, end,
                                arrowstyle='->', mutation_scale=15,
                                color='black', alpha=0.5)
        ax.add_patch(arrow)
    
    ax.plot([1.5, 6.5], [4, 4], 'k--', alpha=0.3, linewidth=2)
    ax.text(4, 3.5, 'Deduplication', ha='center', fontsize=8, style='italic')
    
    arrow = FancyArrowPatch((2.5, 3.5), (2.5, 2.8),
                            arrowstyle='->', mutation_scale=15,
                            color='black', alpha=0.5)
    ax.add_patch(arrow)
    
    for pos in attr_positions:
        arrow = FancyArrowPatch((pos[0]+1.5, 2), (7, 1.2),
                                arrowstyle='->', mutation_scale=15,
                                color='black', alpha=0.5)
        ax.add_patch(arrow)
    
    legend_elements = [
        mpatches.Patch(color=doc_color, label='Document (All fields)'),
        mpatches.Patch(color=semantic_color, label='Semantic Unit (All fields)'),
        mpatches.Patch(color=entity_color, label='Entity (No text)'),
        mpatches.Patch(color=rel_color, label='Relationship (No text)'),
        mpatches.Patch(color=attr_color, label='Attribute (Aggregated)'),
        mpatches.Patch(color=comm_color, label='Community (Tenant only)')
    ]
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1.02, 1))
    
    ax.set_title('EQ Metadata Flow Through NodeRAG Graph', fontsize=16, weight='bold', pad=20)
    ax.text(0.5, -0.5, 'Note: Duplicate entities are detected via deterministic IDs', 
            ha='left', fontsize=9, style='italic')
    
    ax.set_xlim(-0.5, 14)
    ax.set_ylim(-1, 10)
    ax.axis('off')
    
    plt.tight_layout()
    plt.savefig('metadata_flow_diagram.png', dpi=300, bbox_inches='tight')
    plt.savefig('metadata_flow_diagram.pdf', bbox_inches='tight')
    print("Diagrams saved to NodeRAG/design/")

if __name__ == "__main__":
    create_metadata_flow_diagram()
