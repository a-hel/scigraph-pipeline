"""Database interface"""

import json
import logging
from datetime import datetime
from typing import List, Dict
from pony.orm import (
    Database as PonyDatabase,
    Required,
    Optional,
    commit,
    PrimaryKey,
    Set,
    select,
)
from pony.orm.core import EntityMeta

from ..utils.logging import logger

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
    conclusions = Set("Conclusion", reverse="summary_id", lazy=False)
    simple_conclusions = Set("SimpleConclusions", reverse="summary_id", lazy=False)
    simple_substituted_conclusions = Set(
        "SimpleSubstitutedConclusions", reverse="summary_id", lazy=False
    )
    #    named_entities = Set("NamedEntity", reverse="summary_id")
    abbreviations = Set("Abbreviation", reverse="summary_id")


class Conclusion(db.Entity):
    _table_ = ("SciGraphPipeline", "conclusions")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="conclusions")
    conclusion = Required(str)
    date_added = Required(datetime)
    simple_conclusions = Set("SimpleConclusions", reverse="conclusion_id", lazy=False)


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
    conclusion_id = Required(Conclusion, reverse="simple_conclusions")
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


class Edge(db.Entity):
    _table_ = ("SciGraphPipeline", "edges")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="edges")
    predicate = Required(str)
    cui_left = Required(str)
    cui_right = Required(str)


class Log(db.Entity):
    _table_ = ("SciGraphPipeline", "log")
    id = PrimaryKey(int, auto=True)
    table = Required(str)
    last_processed = Required(int)
    timestamp = Required(datetime)


class Database:
    def __init__(
        self,
        db,
        host="localhost",
        user="postgres",
        port=5432,
        password=None,
        database="SciGraph_staging",
    ):
        self.db = db
        if self.db.provider is None:
            self.db.bind(
                provider="postgres",
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
            )
            self.db.generate_mapping(create_tables=True)
        else:
            logger.debug("Using previously bound database")
        self.articles = Article
        self.abbreviations = Abbreviation
        self.summaries = Summary
        self.conclusions = Conclusion
        self.simple_conclusions = SimpleConclusions
        self.simple_substituted_conclusions = SimpleSubstitutedConclusions
        self.named_entities = NamedEntity
        self.nodes = Node
        self.edges = Edge
        self.logs = Log
        logger.debug(f"Connected to database:\t{user}@{host}:{port}/{database}")

    # def __del__(self):
    #    self.db.disconnect()

    def _commit(self, table, last_record):
        commit()
        checkpoint = {
            "table": ".".join(table._table_),
            "last_processed": last_record.id,
            "timestamp": datetime.now(),
        }
        self.logs(**checkpoint)
        commit()

    # @db_session
    def _add_record(self, data, table, periodic_commit=50):
        last_record = type("placeholder", (), {"id": 0})
        if True:
            for e, elem in enumerate(data):
                last_record = table(**elem)
                if not e % periodic_commit:
                    logging.info("Processing %s: %s" % (e, last_record))
                    self._commit(table, last_record)
            # finally:
            self._commit(table, last_record)
        print("Finished cycle")
        return last_record.id

    def get_summaries(self):
        elems = select(c for c in self.summaries if c.named_entities.id)

        # elems = select((c.id, c.conclusion, c.named_entities.matched_term, c.named_entities.preferred_term) for c in self.summaries if c.named_entities.id)
        yield from elems

    def add_record(self, data, table, periodic_commit=50):
        if isinstance(table, (str, EntityMeta)):
            return self._add_record(data, table, periodic_commit)
        elif isinstance(table, list):
            for elem in data:
                for tbl in table:
                    filtered_records = elem[tbl._table_[-1]]
                    last_id = self._add_record(filtered_records, tbl, periodic_commit)
            return last_id
        raise TypeError("Expected table, name, or dict  got %s" % type(table))

    def get_by_id(self, table, id):
        return table[id]

    def get_records(self, table, mode="all", downstream=None, order_by=None):
        if isinstance(table, str):
            table = getattr(self, table)
        if not isinstance(table, EntityMeta):
            raise (ValueError, "No table with with that name")
        select_functions = {
            "all": self._get_all_records,
            "unprocessed": self._get_unprocessed_records,
            "newer": self._get_newer_records,
        }
        try:
            select_function = select_functions[mode]
        except KeyError as e:
            raise KeyError(
                f"Unknown mode '{mode}'. Allowed values are {','.join(select_functions.keys())}"
            )
        elems = select_function(table, downstream)
        if order_by is not None:
            column = getattr(table, order_by)
            elems = elems.order_by(column)
        yield from elems

    def _get_all_records(self, table, downstream):
        elems = select(c for c in table)
        return elems

    def _get_unprocessed_records(self, table, downstream):
        elems = select(c for c in table if not getattr(c, downstream._table_[-1]).id)
        return elems

    def _get_newer_records(self, table, downstream):
        elems = select(
            c
            for c in table
            if c.date_added > min(getattr(c, downstream._table_[-1]).date_added)
        )
        return elems

    def add_articles(self, data):
        last_id = self._add_record(data, self.articles, periodic_commit=1000)
        return {"article_id": last_id}

    def add_summary(self, data):
        last_id = self._add_record(data, self.summaries, periodic_commit=100)
        return {"summary_id": last_id}

    def add_named_entity(self, data):
        last_id = self._add_record(data, self.named_entities, periodic_commit=100)
        return {"named_entity_id": last_id}

    @staticmethod
    def from_config(path="../config/dev.json", key="postgres"):
        with open(path, "r") as f:
            config = json.load(f)
        if key is not None:
            config = config[key]
            logger.debug(f"Loaded Postgres config section {key} form file {path}.")
        else:
            logger.debug(f"Loaded Postgres config form file {path}.")
        return Database(db, **config)
