from logging import Logger

import spacy
import networkx as nx

from .spacy_pipeline import claucy, information_extractor  # noqa
from scispacy.linking import EntityLinker  # noqa

logger = Logger(__name__)


def triples_to_graph(triples) -> nx.Graph:

    G = nx.DiGraph()
    for triple in triples:
        if triple is None:
            continue
        if len(triple.subject.ents) == 0 or len(triple.object_.ents) == 0:
            continue
        subjects = triple.subject
        for subject in subjects.ents:
            G.add_node(subject, node_type="concept", label=subject.text)
        ordered_ents = (
            subjects.ents.copy()
        )  # TODO check that dependency tree is correct
        ordered_ents.reverse()
        for i in range(len(ordered_ents) - 1):
            G.add_edge(ordered_ents[i], ordered_ents[i + 1], edge_type="_REL")
        objects = triple.object_
        for object_ in objects.ents:
            G.add_node(object_, node_type="concept")
            G.add_edge(
                object_, subjects.ents[-1], edge_type="_VERB", name=triple.verb.text
            )
    return G


def _match_terms(records, nlp, name="conclusions"):
    linker = nlp.get_pipe("scispacy_linker")
    for e, record in enumerate(records):
        if e > 10:
            break
        doi = record.summary_id.article_id.doi
        summary_id = record.summary_id
        doc = nlp(record.conclusion)
        if not doc._.triples:
            continue
        staging_graph = triples_to_graph(doc._.triples)
        if nx.is_empty(staging_graph):
            continue
        graph_nodes = list(staging_graph.nodes)
        node_objs = _to_nodes(graph_nodes, linker, summary_id)
        graph_edges = list(staging_graph.edges(data=True))
        edge_objs = _to_edges(graph_edges, record, doi)
        yield {"nodes": node_objs, "edges": edge_objs}


def _predicate_edge(start, end, data, record, doi):
    for start_cui, _ in start._.kb_ents:
        for end_cui, _ in end._.kb_ents:
            edge_data = {
                "summary_id": record.summary_id,
                "node_left": start_cui,
                "node_right": end_cui,
                "edge_type": data["edge_type"],
                "attributes": {
                    "name": data["name"],
                    "doi": doi,
                    "conclusion": record.conclusion,
                    "summary": record.summary_id.summary,
                },
            }
            yield edge_data


def _relational_edge(start, end, data, record, doi):
    for start_cui, _ in start._.kb_ents:
        for end_cui, _ in end._.kb_ents:
            edge_data = {
                "summary_id": record.summary_id,
                "node_left": start_cui,
                "node_right": end_cui,
                "edge_type": data["edge_type"],
                "attributes": {"doi": doi},
            }
            yield edge_data


def _synonym_edge(start, end, data, record, doi):
    for _ in range(1):
        edge_data = {
            "summary_id": record.summary_id,
            "node_left": start.text,
            "node_right": end.text,
            "edge_type": data["edge_type"],
            "attributes": {},
        }
        yield edge_data


def _to_edges(graph_edges, record, doi):
    converters = {
        "_VERB": _predicate_edge,
        "_REL": _relational_edge,
        "_SYN": _synonym_edge,
    }
    for start, end, data in graph_edges:
        converter = converters[data["edge_type"]]
        yield from converter(start, end, data, record, doi)


def _to_nodes(graph_nodes, linker, summary_id):
    for node in graph_nodes:
        for concept in node._.kb_ents:
            cui = concept[0]
            node_data = linker.kb.cui_to_entity[cui]
            node = {
                "summary_id": summary_id,
                "cui_or_name": node_data.concept_id,
                "node_type": "concept",
                "attributes": {
                    "canonical_name": node_data.canonical_name,
                    "cui": node_data.concept_id,
                    "definition": node_data.definition,
                },
            }
            yield node
            for alias in node_data.aliases:
                synonym_node = {
                    "summary_id": summary_id,
                    "cui_or_name": node_data.concept_id,
                    "node_type": "synonym",
                    "attributes": {"synonym": alias},
                }
                yield synonym_node


def extract_triples(simplified_summaries, spacy_model="en_core_sci_scibert"):
    allowed_models = [
        "en_core_sci_sm",
        "en_core_sci_md",
        "en_core_sci_lg",
        "en_core_sci_scibert",
    ]
    if spacy_model not in allowed_models:
        raise ValueError(
            f"Model '{spacy_model}' is not applicable for this task."
            + f"Allowed models are: '{', '.join(allowed_models)}'."
        )
    nlp = spacy.load(spacy_model)
    nlp.add_pipe("claucy")
    nlp.add_pipe("InformationExtractor", after="claucy")
    nlp.add_pipe(
        "scispacy_linker",
        config={
            "resolve_abbreviations": False,
            "linker_name": "umls",
            "max_entities_per_mention": 1,
        },
    )
    logger.info(f"NLP loaded, using model '{spacy_model}'.")
    yield from _match_terms(simplified_summaries, nlp)
