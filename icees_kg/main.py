import csv
from datetime import datetime
from dotenv import load_dotenv
import logging
import numpy as np
import os
from pathlib import Path
import sys
from tqdm import tqdm
import yaml

from Common.kgxmodel import kgxnode, kgxedge
from Common.kgx_file_writer import KGXFileWriter
from Common.kgx_file_normalizer import remove_unconnected_nodes

from utils.node_lookup import node_lookup
from utils.get_features import get_feature_info_from_column_info, get_edge_stats

load_dotenv()

now = datetime.now()
now_string = now.strftime('%Y_%m_%d_%H_%M_%S')

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s: %(levelname)s/%(name)s]: %(message)s",
    handlers=[
        logging.FileHandler(f"../logs/icees_kg_{now_string}.log"),
        logging.StreamHandler(sys.stdout),
    ]
)
LOGGER = logging.getLogger(__name__)

data_path = os.getenv('DATA_PATH', None)
assert data_path is not None, 'An environment variable called DATA_PATH is required.'
data_csvs = Path(data_path).glob('*.csv')

features_yml = os.getenv('FEATURES_YAML', None)
assert features_yml is not None, 'An environment variable called FEATURES_YAML is required.'

dataset_name = os.getenv('DATASET_NAME', None)
assert dataset_name is not None, 'An environment variable called DATASET_NAME is required.'

# create output folder
if not os.path.exists('../build'):
    os.makedirs('../build')
LOGGER.info(f'Running precompute for dataset: {dataset_name}')
LOGGER.info('Loading data files...')

with open(features_yml, 'r') as f:
    full_features = yaml.safe_load(f)
    features = full_features['patient']

for data_csv in tqdm(data_csvs):
    data = []
    # get database identifier, i.e. asthma_cohort_2010
    data_id = data_csv.stem
    year = data_id.split('_')[4]
    icees_cohort_identifier = f'{data_id}|{dataset_name}|{year}|{now_string}'

    with open(data_csv, 'r') as f:
        c_data = [row for row in csv.DictReader(f)]
        data.extend(c_data)

    LOGGER.info('Making data columns...')
    data_columns = list(data[0].keys())
    data_column_info = dict()
    for feature, feature_info in features.items():

        # Figure out if this feature is in the data_columns
        try:
            feature_column_index = data_columns.index(feature)
        except ValueError:
            # This yaml feature is not found in the data csv
            continue

        if 'name_lookup' not in feature_info:
            # we need a search term in order to include in kg
            continue

        # Double check that the features match
        column = data_columns[feature_column_index]
        if not column == feature:
            raise Exception('How did we get here?')

        # Set the current column information
        if feature_info.get('enum', None):
            # make sure enums are the same order every time
            feature_info["enum"].sort()
            column_info = {
                'name': column,
                'enum': feature_info['enum'],
                'range': len(feature_info['enum']),
                'min': 0,
                'max': len(feature_info['enum']) - 1,
                'is_integer': False,
                'categories': feature_info.get('categories', []),
                'name_lookup': feature_info.get('name_lookup', []),
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
                        'categories': feature_info.get('categories', []),
                        'name_lookup': feature_info.get('name_lookup', []),
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
                    if d[column] == 'Missing':
                        continue
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

    LOGGER.info('Getting useful features...')
    # Some features are all empty? So let's not worry about them
    is_useful_feature = np.sum(np.isfinite(data_np), axis=0) > 0
    useful_features = [k for i, k in enumerate(data_column_info.keys()) if is_useful_feature[i]]
    LOGGER.info(f'There are {len(useful_features)} useful features')

    node_list = []
    edge_list = []
    node_dict = {}

    LOGGER.info('Creating nodes and edges...')
    columns = data_column_info.items()
    for i_col, (i_column, i_column_info) in enumerate(tqdm(columns)):
        # Don't worry about the features that are always empty
        if not (i_column in useful_features):
            continue

        # resolve and normalize any search terms
        if not ('name_lookup' in i_column_info):
            continue

        i_normalized_nodes = {}
        for node_search in i_column_info['name_lookup']:
            i_normalized_nodes.update(node_lookup(node_search['search_term'], node_search['limit']))

        if not i_normalized_nodes:
            # normalized_nodes could be empty dict
            continue

        for curie, node in i_normalized_nodes.items():
            if "categories" in node and "categories" in i_column_info:
                # add hard coded categories from features yaml file
                node["categories"].extend(i_column_info["categories"])
                node["categories"] = list(set(node["categories"]))
            node_dict[curie] = node

        feature_description_1 = get_feature_info_from_column_info(i_column_info)
        x1 = data_np[:, i_col]
        if i_column_info['is_integer']:
            u_x1 = [ind + j_column_info['min'] for ind in range(i_column_info['range'])]
        else:  # enum
            u_x1 = list(range(len(i_column_info['enum'])))

        for j_col, (j_column, j_column_info) in enumerate(columns):
            if j_col <= i_col:
                # Do upper triangle only
                continue

            # Don't worry about the features that are always empty
            if not (j_column in useful_features):
                continue

            # name resolve any search terms
            if not ('name_lookup' in j_column_info):
                continue

            j_normalized_nodes = {}
            for node_search in j_column_info['name_lookup']:
                normalized_node = node_lookup(node_search['search_term'], node_search['limit'])
                j_normalized_nodes.update(normalized_node)

            if not j_normalized_nodes:
                # normalized_nodes could be empty dict
                continue

            for curie, node in j_normalized_nodes.items():
                if "categories" in node and "categories" in j_column_info:
                    # add hard coded categories from features yaml file
                    node["categories"].extend(j_column_info["categories"])
                    node["categories"] = list(set(node["categories"]))
                node_dict[curie] = node

            feature_description_2 = get_feature_info_from_column_info(j_column_info)
            x2 = data_np[:, j_col]
            if j_column_info['is_integer']:
                u_x2 = [ind + j_column_info['min'] for ind in range(j_column_info['range'])]
            else:  # enum
                u_x2 = list(range(len(j_column_info['enum'])))

            LOGGER.info(f"{feature_description_1['feature_name']} -> {feature_description_2['feature_name']}")
            # Calculate stats for i_col and j_col
            # x1 is the column_sum
            # x2 is the row_sum
            try:
                predicate, edge_stats = get_edge_stats(x1, x2, u_x1, u_x2, i_column, j_column, i_column_info, j_column_info)
            except Exception as e:
                LOGGER.error(f"Error making edge stats: {e}")
                continue

            # if edge_stats["chi_squared_p"] > 0.5:
            #     # discard any edges that aren't significant
            #     continue

            # Package edge properties
            edge_props = {
                'biolink:supporting_data_source': 'https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES',
                'terms_and_conditions_of_use': 'https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES-and-ICEES-KG-Terms-and-Conditions-of-Use',
                'icees_cohort_identifier': icees_cohort_identifier,
                'subject_feature_name': feature_description_1['feature_name'],
                'object_feature_name': feature_description_2['feature_name'],
            }
            edge_props.update(edge_stats)

            for i_id in i_normalized_nodes.keys():
                for j_id in j_normalized_nodes.keys():

                    new_edge = kgxedge(
                        subject_id=i_id,
                        object_id=j_id,
                        predicate=predicate,
                        primary_knowledge_source="infores:automat-icees-kg",
                        edgeprops=edge_props,
                    )
                    edge_list.append(new_edge)

    LOGGER.info('Writing files...')

    for node, node_vals in node_dict.items():
        node_list.append(kgxnode(
            node,
            name=node_vals.get('name', ''),
            categories=node_vals.get('categories', []),
            nodeprops={
                'equivalent_identifiers': node_vals.get('equivalent_identifiers', []),
                # TODO: include information content if it exists
            },
        ))

    # Write json output output
    nodes_output_file_path = f'../build/{data_id}_nodes.jsonl'
    edges_output_file_path = f'../build/{data_id}_edges.jsonl'

    with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:

        # for each edge captured
        for edge in edge_list:
            # write out the edge data
            file_writer.write_kgx_edge(edge)

        # for each node captured
        for node in node_list:
            # write out the node
            file_writer.write_kgx_node(node)

    orphan_nodes_removed = remove_unconnected_nodes(nodes_output_file_path, edges_output_file_path)

LOGGER.info('All done!')
