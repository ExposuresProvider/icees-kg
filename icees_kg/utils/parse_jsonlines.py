"""This is a throwaway script that parses jsonl files for various purposes."""
import jsonlines
import json
from pathlib import Path

build_folder = '../../build/'
# file_path = './build/Asthma_UNC_PEGS_patient_2019_v4_binned_deidentified_edges.jsonl'
# file_path = './build/asthma_2019_test_two_edges.jsonl'
# file_path = './build/Asthma_UNC_PEGS_patient_2019_v4_binned_deidentified_nodes.jsonl'
data_csvs = Path(build_folder).glob('Asthma_*_edges.jsonl')


def get_jsonl_files():
    data_csvs = Path(build_folder).glob('*.jsonl')
    print((' ').join(str(path.stem) + '.jsonl' for path in data_csvs))


def fix_edge_files():
    for file_path in data_csvs:
        print(file_path)
        with jsonlines.open(file_path) as reader:
            new_path = build_folder + file_path.stem + '_new.jsonl'
            with jsonlines.open(new_path, mode='w') as writer:
                for edge in reader:
                    # fix cohort identifier
                    # identifier_list = edge['icees_cohort_identifier'].split('|')
                    # identifier_list[1] = 'dili'
                    # edge['icees_cohort_identifier'] = ('|').join(identifier_list)
                    # print(edge['icees_cohort_identifier'])

                    # convert contingency matrices
                    # print(type(edge['contingency:matrices'][0]))
                    matrices = edge['contingency:matrices'][0]
                    edge['biolink:supporting_data_source'] = 'https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES'
                    edge['terms_and_conditions_of_use'] = 'https://github.com/NCATSTranslator/Translator-All/wiki/ICEES--and-ICEES-KG-Terms-and-Conditions-of-Use'
                    edge['subject_feature_name'] = matrices['feature_a']['feature_name']
                    edge['object_feature_name'] = matrices['feature_b']['feature_name']
                    edge["p_value"] = matrices['p_value']
                    edge["chi_squared"] = matrices['chi_squared']
                    edge["total"] = matrices['total']
                    del edge['contingency:matrices']
                    writer.write(edge)


def parse_edge_files():
    # feature_b = set()
    for file_path in data_csvs:
        with jsonlines.open(file_path) as reader:
            for edge in reader:
                # print((edge['icees_cohort_identifier']).split('|'))
                identifier_list = edge['icees_cohort_identifier'].split('|')
                identifier_list[1] = 'pcd'
                edge['icees_cohort_identifier'] = ('|').join(identifier_list)
                print(edge['icees_cohort_identifier'])
                # if obj['object'] == 'MONDO:0004979' and obj['subject'] in ['UNII:90Z2UF0E52', 'PUBCHEM.COMPOUND:5865', 'PUBCHEM.COMPOUND:6918554', 'PUBCHEM.COMPOUND:82153', 'CHEBI:5956', 'PUBCHEM.COMPOUND:4091', 'PUBCHEM.COMPOUND:657309', 'PUBCHEM.COMPOUND:441335', 'PUBCHEM.COMPOUND:24823', 'CHEBI:5147', 'PUBCHEM.COMPOUND:11954221', 'PUBCHEM.COMPOUND:3348', 'PUBCHEM.COMPOUND:5311101', 'UNII:E6582LOH6V', 'PUBCHEM.COMPOUND:3410', 'PUBCHEM.COMPOUND:3100', 'MESH:C000589846', 'PUBCHEM.COMPOUND:2678', 'UNII:0DHU5B8D6V', 'PUBCHEM.COMPOUND:6918155', 'PUBCHEM.COMPOUND:2771', 'UNII:YO7261ME24', 'PUBCHEM.COMPOUND:20469', 'CHEBI:3723', 'PUBCHEM.COMPOUND:18546548', 'PUBCHEM.COMPOUND:23205324', 'PUBCHEM.COMPOUND:12559814', 'PUBCHEM.COMPOUND:87421662', 'PUBCHEM.COMPOUND:88836296', 'PUBCHEM.COMPOUND:87649514', 'PUBCHEM.COMPOUND:20545201', 'PUBCHEM.COMPOUND:118558265', 'PUBCHEM.COMPOUND:6451318', 'PUBCHEM.COMPOUND:12750199', 'PUBCHEM.COMPOUND:122446005', 'PUBCHEM.COMPOUND:88400771', 'PUBCHEM.COMPOUND:53400411', 'PUBCHEM.COMPOUND:13418382', 'PUBCHEM.COMPOUND:87178856', 'PUBCHEM.COMPOUND:54529281', 'PUBCHEM.COMPOUND:87690418', 'PUBCHEM.COMPOUND:88094765', 'PUBCHEM.COMPOUND:87645729', 'PUBCHEM.COMPOUND:88847889', 'PUBCHEM.COMPOUND:70426224', 'PUBCHEM.COMPOUND:53421521', 'PUBCHEM.COMPOUND:21477940', 'PUBCHEM.COMPOUND:70251418', 'PUBCHEM.COMPOUND:117873387', 'PUBCHEM.COMPOUND:10219496', 'PUBCHEM.COMPOUND:132939818', 'PUBCHEM.COMPOUND:22643854', 'PUBCHEM.COMPOUND:23496436', 'PUBCHEM.COMPOUND:23338344', 'PUBCHEM.COMPOUND:69335021', 'PUBCHEM.COMPOUND:131852944', 'PUBCHEM.COMPOUND:18318603', 'PUBCHEM.COMPOUND:88676740', 'PUBCHEM.COMPOUND:87183070', 'PUBCHEM.COMPOUND:89844269', 'PUBCHEM.COMPOUND:22643862', 'PUBCHEM.COMPOUND:87650023', 'PUBCHEM.COMPOUND:87179199', 'PUBCHEM.COMPOUND:88440815', 'PUBCHEM.COMPOUND:6455115', 'PUBCHEM.COMPOUND:87225224', 'PUBCHEM.COMPOUND:69190897', 'PUBCHEM.COMPOUND:88727041', 'PUBCHEM.COMPOUND:88745611', 'PUBCHEM.COMPOUND:88844677', 'PUBCHEM.COMPOUND:87153227', 'PUBCHEM.COMPOUND:87673592', 'PUBCHEM.COMPOUND:20446821', 'PUBCHEM.COMPOUND:88625096', 'PUBCHEM.COMPOUND:88065107', 'PUBCHEM.COMPOUND:87207076', 'PUBCHEM.COMPOUND:12824984', 'PUBCHEM.COMPOUND:70582067', 'PUBCHEM.COMPOUND:15797016', 'PUBCHEM.COMPOUND:87303545', 'PUBCHEM.COMPOUND:88422839', 'PUBCHEM.COMPOUND:22098083', 'PUBCHEM.COMPOUND:119095354', 'PUBCHEM.COMPOUND:118558255', 'PUBCHEM.COMPOUND:42626763', 'PUBCHEM.COMPOUND:20139699', 'PUBCHEM.COMPOUND:21469906', 'PUBCHEM.COMPOUND:18994493', 'PUBCHEM.COMPOUND:70132562', 'PUBCHEM.COMPOUND:17866343', 'PUBCHEM.COMPOUND:88436588', 'PUBCHEM.COMPOUND:67056260', 'PUBCHEM.COMPOUND:86732912', 'PUBCHEM.COMPOUND:20239257', 'PUBCHEM.COMPOUND:67825128', 'PUBCHEM.COMPOUND:20556755', 'PUBCHEM.COMPOUND:68137605', 'PUBCHEM.COMPOUND:66686277', 'PUBCHEM.COMPOUND:9942123', 'PUBCHEM.COMPOUND:70136144', 'PUBCHEM.COMPOUND:88192804', 'PUBCHEM.COMPOUND:88077846', 'PUBCHEM.COMPOUND:19083246', 'PUBCHEM.COMPOUND:21653308', 'PUBCHEM.COMPOUND:1119', 'PUBCHEM.COMPOUND:88502608', 'PUBCHEM.COMPOUND:18421317', 'PUBCHEM.COMPOUND:21917443', 'PUBCHEM.COMPOUND:91223041', 'PUBCHEM.COMPOUND:67690104', 'PUBCHEM.COMPOUND:19710773', 'PUBCHEM.COMPOUND:87899360', 'PUBCHEM.COMPOUND:87195987', 'PUBCHEM.COMPOUND:70133875', 'PUBCHEM.COMPOUND:66637664', 'PUBCHEM.COMPOUND:70113048', 'PUBCHEM.COMPOUND:23346066', 'PUBCHEM.COMPOUND:22401931', 'PUBCHEM.COMPOUND:118856517', 'PUBCHEM.COMPOUND:22119633', 'CHEBI:35196', 'PUBCHEM.COMPOUND:3032552', 'PUBCHEM.COMPOUND:241', 'PUBCHEM.COMPOUND:177', 'CHEBI:22712', 'MESH:D052638', 'PUBCHEM.COMPOUND:145068', 'PUBCHEM.COMPOUND:281', 'PUBCHEM.COMPOUND:3083544', 'UNII:QF8SVZ843E', 'PUBCHEM.COMPOUND:2083']:
                #     feature = json.loads(obj['contingency:matrices'])[0]
                #     feature_b.add(feature['feature_b']['feature_name'])
                #     feature_b.add(feature['feature_a']['feature_name'])
                #     # print(f"{feature['feature_a']['feature_name']}, {feature['feature_b']['feature_name']}")
                #     # print(obj['contingency:matrices'])
    # print(list(feature_b))


get_jsonl_files()
# fix_edge_files()
# parse_edge_files()
