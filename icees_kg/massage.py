import glob
import json
import jsonlines


edge_files = glob.glob("build/*_edges.jsonl")
node_files = glob.glob("build/*_nodes.jsonl")
release_version = "1.5.0"

jsonl = []
edges = {}
for file in edge_files:
    with jsonlines.open(file) as reader:
        for line in reader:
            edge_id = f"{line['subject']}-{line['predicate']}>{line['object']}"
            edges[edge_id] = edges.get(edge_id, {
                "subject": line["subject"],
                "predicate": line["predicate"],
                "object": line["object"],
                "attributes": [
                    {
                        "attribute_type_id": "biolink:primary_knowledge_source",
                        "value": line["biolink:primary_knowledge_source"],
                    },
                    {
                        "attribute_type_id": "biolink:has_supporting_study_result",
                        "value": line["biolink:has_supporting_study_result"],
                    },
                    {
                        "attribute_type_id": "terms_and_conditions_of_use",
                        "value": line["terms_and_conditions_of_use"],
                    },
                    {
                        "attribute_type_id": "subject_feature_name",
                        "value": line["subject_feature_name"],
                    },
                    {
                        "attribute_type_id": "object_feature_name",
                        "value": line["object_feature_name"],
                    },
                ],
            })
            exists = False
            for attribute in edges[edge_id]["attributes"]:
                if attribute["attribute_type_id"] == line["icees_cohort_identifier"]:
                    exists = True
                    attribute["attributes"] += [
                        {
                            "attribute_type_id": "chi_squared_statistic",
                            "value": line["chi_squared_statistic"],
                        },
                        {
                            "attribute_type_id": "chi_squared_dof",
                            "value": line["chi_squared_dof"],
                        },
                        {
                            "attribute_type_id": "chi_squared_p",
                            "value": line["chi_squared_p"],
                        },
                        {
                            "attribute_type_id": "total_sample_size",
                            "value": line["total_sample_size"],
                        },
                    ]
                    if "fisher_exact_odds_ratio" in line:
                        attribute["attributes"] += [
                            {
                                "attribute_type_id": "fisher_exact_odds_ratio",
                                "value": line["fisher_exact_odds_ratio"],
                            },
                            {
                                "attribute_type_id": "fisher_exact_p",
                                "value": line["fisher_exact_p"],
                            },
                            {
                                "attribute_type_id": "log_odds_ratio",
                                "value": line["log_odds_ratio"],
                            },
                            {
                                "attribute_type_id": "log_odds_ratio_95_ci",
                                "value": line["log_odds_ratio_95_ci"],
                            },
                        ]
            if not exists:
                attributes = [
                    {
                        "attribute_type_id": "chi_squared_statistic",
                        "value": line["chi_squared_statistic"],
                    },
                    {
                        "attribute_type_id": "chi_squared_dof",
                        "value": line["chi_squared_dof"],
                    },
                    {
                        "attribute_type_id": "chi_squared_p",
                        "value": line["chi_squared_p"],
                    },
                    {
                        "attribute_type_id": "total_sample_size",
                        "value": line["total_sample_size"],
                    },
                ]
                if "fisher_exact_odds_ratio" in line:
                    attributes += [
                        {
                            "attribute_type_id": "fisher_exact_odds_ratio",
                            "value": line["fisher_exact_odds_ratio"],
                        },
                        {
                            "attribute_type_id": "fisher_exact_p",
                            "value": line["fisher_exact_p"],
                        },
                        {
                            "attribute_type_id": "log_odds_ratio",
                            "value": line["log_odds_ratio"],
                        },
                        {
                            "attribute_type_id": "log_odds_ratio_95_ci",
                            "value": line["log_odds_ratio_95_ci"],
                        },
                    ]
                edges[edge_id]["attributes"].append({
                    "attribute_type_id": "icees_cohort_identifier",
                    "value": line["icees_cohort_identifier"],
                    "attributes": attributes,
                })
for edge in edges.values():
    edge["attributes"] = json.dumps(edge["attributes"])
    jsonl.append(edge)

with jsonlines.open(f"releases/{release_version}/edges.jsonl", "w") as writer:
  writer.write_all(jsonl)

ids = set()
jsonl = []
for file in node_files:
  with jsonlines.open(file) as reader:
    for line in reader:
      if line["id"] not in ids:
        ids.add(line["id"])
        jsonl.append(line)

with jsonlines.open(f"releases/{release_version}/nodes.jsonl", "w") as writer:
  writer.write_all(jsonl)
