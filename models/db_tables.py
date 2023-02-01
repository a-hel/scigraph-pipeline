from datetime import datetime

from pony.orm import (
    Database as PonyDatabase,
    Required,
    Optional,
    PrimaryKey,
    Set,
    Json,
    composite_key,
)

db: PonyDatabase = PonyDatabase()
DB_SCHEMA: str = "SciGraphPipeline"


class Article(db.Entity):
    _table_ = (DB_SCHEMA, "articles")
    id = PrimaryKey(int, auto=True)
    doi = Required(str, index=True)
    uri = Required(str)
    summaries = Set("Summary", reverse="article_id")
    abbreviations = Set("Abbreviation", reverse="article_id")


class Summary(db.Entity):
    _table_ = (DB_SCHEMA, "summaries")
    id = PrimaryKey(int, auto=True)
    article_id = Required(Article, reverse="summaries")
    summary = Required(str)
    conclusion = Required(str)
    date_added = Required(datetime)
    scitldr_version = Required(str)
    error = Optional(str)

    nodes = Set("Node", reverse="summary_id")
    edges = Set("Edge", reverse="summary_id")
    simple_conclusions = Set("SimpleConclusions", reverse="summary_id", lazy=False)
    simple_substituted_conclusions = Set(
        "SimpleSubstitutedConclusions", reverse="summary_id", lazy=False
    )
    abbreviations = Set("Abbreviation", reverse="summary_id")


class Abbreviation(db.Entity):
    _table_ = (DB_SCHEMA, "abbreviations")
    id = PrimaryKey(int, auto=True)
    article_id = Optional(Article, reverse="abbreviations")
    summary_id = Optional(Summary, reverse="abbreviations")
    doi = Optional(str)
    abbreviation = Required(str)
    meaning = Required(str)


class SimpleConclusions(db.Entity):
    _table_ = (DB_SCHEMA, "simple_conclusions")
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
    _table_ = (DB_SCHEMA, "simple_substituted_conclusions")
    id = PrimaryKey(int, auto=True)
    simple_conclusion_id = Required(
        SimpleConclusions, reverse="simple_substituted_conclusions"
    )
    summary_id = Required(Summary, reverse="simple_substituted_conclusions")
    conclusion = Required(str)
    date_added = Required(datetime)
    error = Optional(str)


class Node(db.Entity):
    _table_ = (DB_SCHEMA, "nodes")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="nodes")
    node_type = Required(str, unique=False)
    cui_or_name = Required(str, unique=False)
    attributes = Required(Json)


class Edge(db.Entity):
    _table_ = (DB_SCHEMA, "edges")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="edges")
    node_left = Required(str)
    node_right = Required(str)
    edge_type = Required(str)
    attributes = Required(Json)


class Log(db.Entity):
    _table_ = (DB_SCHEMA, "log")
    id = PrimaryKey(int, auto=True)
    table = Required(str)
    last_processed = Required(int)
    timestamp = Required(datetime)
