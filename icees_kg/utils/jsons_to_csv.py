"""This is a throwaway script that converts jsonl files to csvs."""
import jsonlines
import pandas as pd

# print("Converting Nodes to csvs")
# # NODES
# node_file = './build/p_val_nodes.jsonl'
# node_list = []
# with open(node_file, 'r') as nf:
#     node_reader = jsonlines.Reader(nf)
#     for node in iter(node_reader):
#         node['category'] = '|'.join(node['category'])
#         node['equivalent_identifiers'] = '|'.join(node.get('equivalent_identifiers', []))
#         node_list.append(node)

# node_df = pd.DataFrame.from_dict(node_list)

# node_out_path = './build/p_val_nodes.csv'
# with open(node_out_path, 'w') as write_tsv:
#     write_tsv.write(node_df.to_csv(index=False))

print("Converting Edges to csvs")
# EDGES
edge_file = './build/Asthma_UNC_PEGS_patient_2010_v4_binned_deidentified_edges.jsonl'
edge_list = []
with open(edge_file, 'r') as ef:
    edge_reader = jsonlines.Reader(ef)
    for edge in edge_reader.iter():
        print(edge)
        edge_list.append(edge)

# edge_df = pd.DataFrame.from_dict(edge_list)

# edge_out_path = './build/p_val_edges.csv'
# with open(edge_out_path, 'w') as write_tsv:
#     write_tsv.write(edge_df.to_csv(index=False))
