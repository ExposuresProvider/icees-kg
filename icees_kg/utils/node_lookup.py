from dotenv import load_dotenv
import logging
import requests
import os

load_dotenv()

node_norm = os.getenv('NODE_NORM', 'https://nodenormalization-sri.renci.org/get_normalized_nodes')
name_resolver = os.getenv('NAME_RESOLVER', 'https://name-resolution-sri.renci.org/lookup')

LOGGER = logging.getLogger(__name__)


def node_lookup(search_term: str, limit: int) -> dict:
    """Get normalized nodes from a search term."""
    params = {'string': search_term, 'limit': limit}
    try:
        response = requests.post(name_resolver, params=params, data=None)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        LOGGER.warning(f'Failed to get identifiers for {search_term}')
        return {}
    response_json = response.json()
    i_identifiers = [result["curie"] for result in response_json]
    if not len(i_identifiers):
        LOGGER.warning(f'No identifiers found for {search_term}')
        return {}

    body = {'curies': i_identifiers}
    try:
        response = requests.post(node_norm, json=body)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        LOGGER.warning(f"Failed to contact node normalizer for {search_term}")
        return {}

    nodes = {}
    try:
        normalized_identifiers = response.json()
        for node in normalized_identifiers.values():
            if node is not None:
                preferred_identifier = node["id"]["identifier"]
                nodes[preferred_identifier] = {
                    'name': node['id'].get('label', ''),
                    'equivalent_identifiers': [eq_identifier['identifier'] for eq_identifier in node.get('equivalent_identifiers', [])],
                    'categories': node.get('type', []),
                }
                if 'information_content' in node:
                    nodes[preferred_identifier]['information_content'] = node['information_content']
    except Exception as e:
        LOGGER.error(f"Failed to parse node norm response: {e}")
        return {}

    return nodes
