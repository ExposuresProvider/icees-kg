import csv
from dotenv import load_dotenv
import json
import logging
import numpy as np
import os
from pathlib import Path
import requests
from scipy.stats import chi2_contingency
import sys
from tqdm import tqdm
import yaml

from Data_services.Common.kgxmodel import kgxnode, kgxedge
from Data_services.Common.kgx_file_writer import KGXFileWriter
from Data_services.Common.kgx_file_normalizer import remove_orphan_nodes

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s: %(levelname)s/%(name)s]: %(message)s",
    handlers=[
        logging.FileHandler("./logs/icees_kg.log"),
        logging.StreamHandler(sys.stdout),
    ]
)
LOGGER = logging.getLogger("make_jsons.py")

data_path = os.getenv('DATA_PATH', None)
assert data_path is not None, 'An environmental variable called DATA_PATH is required.'
data_csvs = Path(data_path).glob('*.csv')

features_yml = os.getenv('FEATURES_YAML', None)
assert features_yml is not None, 'An environmental variable called FEATURES_YAML is required.'

identifiers_yml = os.getenv('IDENTIFIERS_YAML', None)
assert identifiers_yml is not None, 'An environmental variable called IDENTIFIERS_YAML is required.'

node_norm = os.getenv('NODE_NORM', 'https://nodenormalization-sri.renci.org/get_normalized_nodes')

NORMALIZE = True

data = []
LOGGER.info('Loading data files...')
for data_csv in tqdm(data_csvs):
    with open(data_csv, 'r') as f:
        c_data = [row for row in csv.DictReader(f)]
        data.extend(c_data)

with open(features_yml, 'r') as f:
    full_features = yaml.safe_load(f)
    features = full_features['patient']

with open(identifiers_yml, 'r') as f:
    full_identifiers = yaml.safe_load(f)
    identifiers = full_identifiers['patient']

LOGGER.info('Done loading files!')

data_columns = list(data[0].keys())
data_column_info = dict()
for feature, feature_info in features.items():

    # Figure out if this feature is in the data_columns
    try:
        feature_column_index = data_columns.index(feature)
    except ValueError:
        # This yaml feature is not found in the data csv
        continue

    # Double check that the features match
    column = data_columns[feature_column_index]
    if not column == feature:
        raise Exception('How did we get here?')

    # Set the current column information
    if feature_info.get('enum', None):
        column_info = {
            'name': column,
            'enum': feature_info['enum'],
            'range': len(feature_info['enum']),
            'min': 0,
            'max': len(feature_info['enum']) - 1,
            'is_integer': False,
            'categories': feature_info.get('categories', [])
        }
    else:
        # No enum, probably an integer and may have min and max
        if feature_info.get('type', None) == 'integer':
            if feature_info.get('minimum', None) and feature_info.get('maximum', None):
                column_info = {
                    'name': column,
                    'enum': [],
                    'range': feature_info['maximum'] - feature_info['minimum'] + 1,
                    'min': feature_info['minimum'],
                    'max': feature_info['maximum'],
                    'is_integer': True,
                    'categories': feature_info.get('categories', [])
                }
            else:
                continue
        else:
            continue

    data_column_info[column] = column_info

data_np = np.full((len(data), len(data_column_info.keys())), np.nan)
for i_col, (column, column_info) in enumerate(data_column_info.items()):
    data_col_np = data_np[:, i_col]
    if column_info['is_integer']:
        for i_row, d in enumerate(data):
            if d.get(column, None):
                data_col_np[i_row] = d[column]
            # else leave nan
    else:  # enum
        val_map = {v: i for i, v in enumerate(column_info['enum'])}
        for i_row, d in enumerate(data):
            if d.get(column, None):
                data_col_np[i_row] = val_map.get(d[column], np.nan)
            # else leave nan

    # The above does this in place
    # data_np[:,i_col] = data_col_np


# Some features are all empty? So let's not worry about them
is_useful_feature = np.sum(np.isfinite(data_np), axis=0) > 0
useful_features = [k for i, k in enumerate(data_column_info.keys()) if is_useful_feature[i]]


def get_feature_count_matrix(x1, x2, u_x1, u_x2, feature_1, feature_2):
    # TODO: This won't handle cases where there are invalid values in the data
    feat_count_mat = np.zeros((len(u_x2), len(u_x1)))

    errors = False
    for cx1, cx2 in zip(x1, x2):
        if (not np.isnan(cx1)) and (not np.isnan(cx2)):
            try:
                i_x1 = np.where(u_x1 == cx1)[0][0]
                i_x2 = np.where(u_x2 == cx2)[0][0]

                feat_count_mat[i_x2, i_x1] += 1  # x2 should be on the rows and x1 on the cols
            except IndexError as e:
                errors = True
                pass
    if errors:
        LOGGER.warning(f"Computed count matrix with errors for {feature_1} and {feature_2}")

    row_count = np.sum(feat_count_mat, axis=0)
    row_density = row_count / np.sum(row_count)

    # This is a little confusing because when you sum through the rows, you have something the length of the columns
    col_summary = [{"frequency": row_count[ind], "percentage": row_count[ind]} for ind in range(row_count.size)],  # cols will refer to feature_a (x1)

    col_count = np.sum(feat_count_mat, axis=1)
    col_density = col_count / np.sum(col_count)

    # This is a little confusing because when you sum through the columns, you have something the length of the rows
    row_summary = [{"frequency": col_count[ind], "percentage": col_density[ind]} for ind in range(col_count.size)],  # rows will refer to feature_b (x2)

    return feat_count_mat, row_summary, col_summary


def get_feature_info_from_column_info(column_info):
    """Get feature qualifiers from column info."""
    if column_info['is_integer']:
        feature_qualifiers = [
            {"operator": "=", "value": i + column_info['min']}
            for i in range(column_info['range'])
        ]
    else:  # enum
        feature_qualifiers = [
            {"operator": "=", "value": k}
            for k in column_info['enum']
        ]

    return {
        'feature_name': column_info['name'],
        'feature_qualifiers': feature_qualifiers
    }


def get_feature_matrix_json(count_mat):
    """Get feature matrix from count matrix."""
    row_counts = np.sum(count_mat, axis=1)
    total = np.sum(row_counts)
    col_counts = np.sum(count_mat, axis=0)

    matrix_dict = []
    for i in range(count_mat.shape[0]):
        row = []
        for j in range(count_mat.shape[1]):
            v = count_mat[i, j]
            row.append({
                "frequency": v,
                "row_percentage": v / row_counts[i],
                "col_percentage": v / col_counts[j],
                "total_percentage": v / total,
            })
        matrix_dict.append(row)

    return matrix_dict


node_list = []
edge_list = []
node_dict = {}
normalized_nodes = {}

LOGGER.info('Creating nodes and edges...')
for i_col, (i_column, i_column_info) in enumerate(tqdm(data_column_info.items())):

    # Don't worry about the features that are always empty
    if not (i_column in useful_features):
        continue
    i_identifiers = identifiers.get(i_column, None)
    if i_identifiers is None:
        continue

    feature_description_1 = get_feature_info_from_column_info(i_column_info)
    x1 = data_np[:, i_col]
    if i_column_info['is_integer']:
        u_x1 = [ind + j_column_info['min'] for ind in range(i_column_info['range'])]
    else:  # enum
        u_x1 = list(range(len(i_column_info['enum'])))

    # DEBUG: save names of normalized nodes
    if NORMALIZE:
        body = {'curies': i_identifiers}
        try:
            response = requests.post(node_norm, json=body)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOGGER.warning(f"Failed to contact node norm for: {i_column}")
        try:
            normalized_identifiers = response.json()
            try:
                normalized_nodes[i_column] = [node['id'].get('label', '') for node in normalized_identifiers.values() if node is not None]
            except Exception as e:
                LOGGER.warning('Unable to parse normalized nodes:', e)
            for curie, node in normalized_identifiers.items():
                if node is not None:
                    node_dict[curie] = {
                        'name': node['id'].get('label', ''),
                        'equivalent_identifiers': node.get('equivalent_identifiers', []),
                        'categories': set(node.get('type', [])),
                    }
                    if 'information_content' in node:
                        node_dict[curie]['information_content'] = node['information_content']
        except Exception as e:
            LOGGER.error(f"Failed to parse node norm response: {e}")

    for j_col, (j_column, j_column_info) in enumerate(data_column_info.items()):
        if j_col <= i_col:
            # Do upper triangle only
            continue
        # Don't worry about the features that are always empty
        if not (j_column in useful_features):
            continue
        j_identifiers = identifiers.get(j_column, None)
        # print(j_identifiers)
        if j_identifiers is None:
            continue

        # DEBUG: save names of normalized nodes
        if NORMALIZE:
            body = {'curies': j_identifiers}
            try:
                response = requests.post(node_norm, json=body)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                LOGGER.warning(f"Failed to contact node norm for: {i_column}")
            try:
                normalized_identifiers = response.json()
                try:
                    normalized_nodes[j_column] = [node['id'].get('label', '') for node in normalized_identifiers.values() if node is not None]
                except Exception as e:
                    LOGGER.warning('Unable to parse normalized nodes:', e)
                for curie, node in normalized_identifiers.items():
                    if node is not None:
                        node_dict[curie] = {
                            'name': node['id'].get('label', ''),
                            'equivalent_identifiers': node.get('equivalent_identifiers', []),
                            'categories': set(node.get('type', [])),
                        }
                        if 'information_content' in node:
                            node_dict[curie]['information_content'] = node['information_content']
            except Exception as e:
                LOGGER.error(f"Failed to parse node norm response: {e}")

        feature_description_2 = get_feature_info_from_column_info(j_column_info)
        x2 = data_np[:, j_col]
        if j_column_info['is_integer']:
            u_x2 = [ind + j_column_info['min'] for ind in range(j_column_info['range'])]
        else:  # enum
            u_x2 = list(range(len(j_column_info['enum'])))

        # Calculate stats for i_col and j_col
        # x1 is the column_sum
        # x2 is the row_sum
        count_mat, row_summary, col_summary = get_feature_count_matrix(x1, x2, u_x1, u_x2, i_column, j_column)
        chi_squared, p, *_ = chi2_contingency(count_mat + np.finfo(np.float32).eps, correction=False)

        # Package edge properties
        edge_props = {
            'feature_a': feature_description_1,
            'feature_b': feature_description_2,
            'feature_matrix': get_feature_matrix_json(count_mat),
            'rows': row_summary,
            'columns': col_summary,
            "total": np.sum(count_mat),
            "chi_squared": chi_squared,
            "p_value": p,
            "icees_subject_feature": i_column,
            "icees_object_feature": j_column
        }

        for i_id in i_identifiers:
            for j_id in j_identifiers:
                # TODO: handle cases where identifiers aren't normalized by node norm
                i_col_set = set(i_column_info['categories'])
                j_col_set = set(j_column_info['categories'])

                if i_id in node_dict:
                    node_dict[i_id]['categories'].union(i_col_set)
                else:
                    node_dict[i_id] = {'categories': i_col_set}

                if j_id in node_dict:
                    node_dict[j_id]['categories'].union(j_col_set)
                else:
                    node_dict[j_id] = {'categories': j_col_set}

                new_edge = kgxedge(
                    subject_id=i_id,
                    object_id=j_id,
                    predicate="biolink:related_to",
                    edgeprops=edge_props,
                )
                edge_list.append(new_edge)

LOGGER.info('Writing files...')

if not os.path.exists('./build'):
    os.makedirs('./build')

if NORMALIZE:
    with open('./build/normalized_nodes.json', 'w') as f:
        json.dump(normalized_nodes, f)

for node, node_vals in node_dict.items():
    node_list.append(kgxnode(
        node,
        name=node_vals.get('name', ''),
        categories=list(node_vals['categories']),
        nodeprops={
            'equivalent_identifiers': node_vals.get('equivalent_identifiers', []),
            # TODO: include information content if it exists
        },
    ))

# Write json output output
nodes_output_file_path = './build/p_val_nodes.json'
edges_output_file_path = './build/p_val_edges.json'

with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:

    # for each edge captured
    for edge in edge_list:
        # write out the edge data
        file_writer.write_kgx_edge(edge)

    # for each node captured
    for node in node_list:
        # write out the node
        file_writer.write_kgx_node(node)

orphan_nodes_removed = remove_orphan_nodes(nodes_output_file_path, edges_output_file_path)
