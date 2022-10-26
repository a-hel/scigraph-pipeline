from datetime import datetime

from itertools import groupby


def stage_nodes(data):
    for preferred, nodes in groupby(data, key=lambda x: x.preferred):
        all_nodes = list(set(nodes))
        first_node = all_nodes[0]
        concept_data = {
            "node_id": first_node.id,
            "cui": first_node.cui,
            "name": first_node.preferred.title(),
            "_date_added": datetime.now(),
        }
        yield {"concept_nodes": concept_data}
        all_nodes.sort(key=lambda x: x.matched.casefold())
        for name, nodes in groupby(all_nodes, key=lambda x: x.matched.casefold()):
            for node in nodes:
                synonym_data = {
                    "cui": node.cui,
                    "name": node.matched.title(),
                    "_date_added": datetime.now(),
                }
                synonym_edge = {
                    "id_left": node.id,
                    "id_right": first_node.id,
                    "_date_added": datetime.now(),
                }
                break
            yield {"synonym_nodes": synonym_data, "synonym_edges": synonym_edge}


def stage_edges(data):
    for record in data:
        predicate_edge = {
            "edge_id": record.id,
            "name": record.predicate,
            "doi": record.summary_id.article_id.doi,
            "summary": record.summary_id.summary,
            "conclusion": record.summary_id.conclusion,
            "cui_left": record.cui_left,
            "cui_right": record.cui_right,
            "_version": "",
            "_date_added": datetime.now(),
        }
        yield predicate_edge