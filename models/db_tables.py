from typing import List, Dict
from datetime import datetime

from pony.orm import (
    Database as PonyDatabase,
    Required,
    Optional,
    commit,
    PrimaryKey,
    Set,
    select,
    db_session,
)

db = PonyDatabase()


class Article(db.Entity):
    _table_ = ("SciGraphPipeline", "articles")
    id = PrimaryKey(int, auto=True)
    doi = Required(str, index=True)
    uri = Required(str)
    summaries = Set("Summary", reverse="article_id")
    abbreviations = Set("Abbreviation", reverse="article_id")


class Summary(db.Entity):
    _table_ = ("SciGraphPipeline", "summaries")
    id = PrimaryKey(int, auto=True)
    article_id = Required(Article, reverse="summaries")
    summary = Required(str)
    conclusion = Required(str)
    date_added = Required(datetime)
    scitldr_version = Required(str)
    error = Optional(str)

    nodes = Set("Node", reverse="summary_id")
    edges = Set("Edge", reverse="summary_id")
    # conclusions = Set("Conclusion", reverse="summary_id", lazy=False)
    simple_conclusions = Set("SimpleConclusions", reverse="summary_id", lazy=False)
    simple_substituted_conclusions = Set(
        "SimpleSubstitutedConclusions", reverse="summary_id", lazy=False
    )
    #    named_entities = Set("NamedEntity", reverse="summary_id")
    abbreviations = Set("Abbreviation", reverse="summary_id")


# class Conclusion(db.Entity):
#     _table_ = ("SciGraphPipeline", "conclusions")
#     id = PrimaryKey(int, auto=True)
#     summary_id = Required(Summary, reverse="conclusions")
#     conclusion = Required(str)
#     date_added = Required(datetime)
#     simple_conclusions = Set("SimpleConclusions", reverse="conclusion_id", lazy=False)


class Abbreviation(db.Entity):
    _table_ = ("SciGraphPipeline", "abbreviations")
    id = PrimaryKey(int, auto=True)
    article_id = Optional(Article, reverse="abbreviations")
    summary_id = Optional(Summary, reverse="abbreviations")
    doi = Optional(str)
    abbreviation = Required(str)
    meaning = Required(str)


class SimpleConclusions(db.Entity):
    _table_ = ("SciGraphPipeline", "simple_conclusions")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="simple_conclusions")
    conclusion = Required(str, lazy=False)
    date_added = Required(datetime)
    muss_version = Required(str)
    error = Optional(str)
    simple_substituted_conclusions = Set(
        "SimpleSubstitutedConclusions", reverse="simple_conclusion_id"
    )


class SimpleSubstitutedConclusions(db.Entity):
    _table_ = ("SciGraphPipeline", "simple_substituted_conclusions")
    id = PrimaryKey(int, auto=True)
    simple_conclusion_id = Required(
        SimpleConclusions, reverse="simple_substituted_conclusions"
    )
    summary_id = Required(Summary, reverse="simple_substituted_conclusions")
    conclusion = Required(str)
    date_added = Required(datetime)
    error = Optional(str)
    named_entities = Set("NamedEntity", reverse="ss_conclusion_id")


class NamedEntity(db.Entity):
    _table_ = ("SciGraphPipeline", "named_entities")
    id = PrimaryKey(int, auto=True)
    ss_conclusion_id = Required(SimpleSubstitutedConclusions, reverse="named_entities")
    matched_term = Required(str)
    preferred_term = Required(str)
    cui = Required(str)
    metamap_version = Required(str)


class Node(db.Entity):
    _table_ = ("SciGraphPipeline", "nodes")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="nodes")
    cui = Required(str, unique=False)
    matched = Optional(str)
    preferred = Required(str)
    concept_nodes = Set("ConceptNode", reverse="node_id")


class ConceptNode(db.Entity):
    _table_ = ("SciGraphPipeline", "concept_nodes")
    id = PrimaryKey(int, auto=True)
    node_id = Required(Node, reverse="concept_nodes")
    cui = Required(str, unique=True)
    name = Required(str)
    _version = Optional(str)
    _date_added = Required(datetime)


class SynonymNode(db.Entity):
    _table_ = ("SciGraphPipeline", "synonym_nodes")
    id = PrimaryKey(int, auto=True)
    node_id = Required(int, unique=True)
    cui = Required(str)
    name = Required(str)
    _version = Optional(str)
    _date_added = Required(datetime)


class Edge(db.Entity):
    _table_ = ("SciGraphPipeline", "edges")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="edges")
    predicate = Required(str)
    cui_left = Required(str)
    cui_right = Required(str)
    predicate_edges = Set("PredicateEdge", reverse="edge_id")


class PredicateEdge(db.Entity):
    _table_ = ("SciGraphPipeline", "predicate_edges")
    id = PrimaryKey(int, auto=True)
    edge_id = Required(Edge, reverse="predicate_edges")
    name = Required(str)
    doi = Required(str, unique=True)
    summary = Required(str)
    conclusion = Required(str)
    cui_left = Required(str)
    cui_right = Required(str)
    _version = Optional(str)
    _date_added = Required(datetime)


class SynonymEdge(db.Entity):
    _table_ = ("SciGraphPipeline", "synonym_edges")
    id = PrimaryKey(int, auto=True)
    node_id_left = Required(int, unique=True)
    node_id_right = Required(int)
    name_left = Required(str)
    name_right = Required(str)
    _version = Optional(str)
    _date_added = Required(datetime)


class Log(db.Entity):
    _table_ = ("SciGraphPipeline", "log")
    id = PrimaryKey(int, auto=True)
    table = Required(str)
    last_processed = Required(int)
    timestamp = Required(datetime)