import jsonlines
import pandas as pd

print("Converting Nodes to tsvs")
# NODES
node_file = './build/p_val_nodes.json'
node_list = []
with open(node_file, 'r') as nf:
    node_reader = jsonlines.Reader(nf)
    for node in iter(node_reader):
        node['category'] = '|'.join(node['category'])
        node_list.append(node)

node_df = pd.DataFrame.from_dict(node_list)

node_out_path = './build/p_val_nodes.tsv'
with open(node_out_path, 'w') as write_tsv:
    write_tsv.write(node_df.to_csv(sep='\t', index=False))

print("Converting Edges to tsvs")
# EDGES
edge_file = './build/p_val_edges.json'
edge_list = []
with open(edge_file, 'r') as ef:
    edge_reader = jsonlines.Reader(ef)
    for num, edge in enumerate(iter(edge_reader)):
        edge_list.append(edge)

edge_df = pd.DataFrame.from_dict(edge_list)

edge_out_path = './build/p_val_edges.tsv'
with open(edge_out_path, 'w') as write_tsv:
    write_tsv.write(edge_df.to_csv(sep='\t', index=False))
