import csv
import json
import logging
from datetime import date, datetime
from typing import List, Dict
from pony.orm import (
    Database,
    Required,
    Optional,
    commit,
    PrimaryKey,
    Set,
    db_session,
    select,
)

db = Database()


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
    simple_conclusions = Set("SimpleConclusions", reverse="summary_id", lazy=False)
    named_entities = Set("NamedEntity", reverse="summary_id")


class Abbreviation(db.Entity):
    _table_ = ("SciGraphPipeline", "abbreviations")
    id = PrimaryKey(int, auto=True)
    article_id = Required(Article, reverse="abbreviations")
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


class NamedEntity(db.Entity):
    _table_ = ("SciGraphPipeline", "named_entities")
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse="named_entities")
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


class RecordPointer:
    def __init__(self, db, refs: Dict[str, int]):
        # self._validate(refs)
        self.refs = {getattr(db, tbl): idx for tbl, idx in refs.items()}
        self.db = db
        self.tables = [".".join(table._table_) for table in self.refs]

    def _validate(self, refs):
        dbs = {id(ref._database_) for ref in refs}
        if len(dbs) > 1:
            raise AttributeError("All tables must come from the same database.")

    @db_session
    def get(self):
        data = {}
        for table, id in self.refs.items():
            elem = table[id]
            data[".".join(table._table_)] = elem
        return data


class Pony:
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
                database=database,
            )
            self.db.generate_mapping(create_tables=True)
        #else:
        #    logger.debug('Using previously bound database')
        self.articles = Article
        self.abbreviations = Abbreviation
        self.summaries = Summary
        self.simple_conclusions = SimpleConclusions
        self.named_entities = NamedEntity
        self.nodes = Node
        self.edges = Edge
        self.logs = Log
        logging.debug("Database %s initialized." % (database))

    def _commit(self, table, last_record):
        commit()
        checkpoint = {
            "table": ".".join(table._table_),
            "last_processed": last_record.id,
            "timestamp": datetime.now(),
        }
        self.logs(**checkpoint)
        commit()

    @db_session()
    def _add_record(self, data, table, periodic_commit=50):
        last_record = type("placeholder", (), {"id": 0})
        # try:
        if True:
            for e, elem in enumerate(data):
                last_record = table(**elem)
                if not e % periodic_commit:
                    logging.info("Processing %s: %s" % (e, last_record))
                    self._commit(table, last_record)
            # finally:
            self._commit(table, last_record)
        return last_record.id

    def _get_record(self, table):
        elems = select(c for c in table)
        yield from elems

    def get_summaries(self):
        elems = select(c for c in self.summaries if c.named_entities.id)

        # elems = select((c.id, c.conclusion, c.named_entities.matched_term, c.named_entities.preferred_term) for c in self.summaries if c.named_entities.id)
        yield from elems

    def add_record(self, data, table, periodic_commit=50):
        return self._add_record(data, table, periodic_commit)

    # @db_session
    def get_records(self, table, run_all=False, downstream=None):
        if run_all:
            yield from self._get_record(table)
        else:
            yield from self._get_unprocessed_records(table, downstream)

    def _get_unprocessed_records(self, table, downstream):
        if not isinstance(downstream, dict):
            downstream = {"downstream": downstream}
        downstream = list(downstream.values())[
            0
        ]  # TODO fix to check for all downstream tables
        elems = select(
            c for c in table if not getattr(c, downstream._table_[-1]).id
        )  # .is_empty()
        yield from elems

    def add_articles(self, data):
        last_id = self._add_record(data, self.articles, periodic_commit=1000)
        return {"article_id": last_id}

    def add_summary(self, data):
        last_id = self._add_record(data, self.summaries, periodic_commit=100)
        return {"summary_id": last_id}

    def add_named_entity(self, data):
        last_id = self._add_record(data, self.named_entities, periodic_commit=100)
        return {"named_entity_id": last_id}


def get_database(config="config.json"):
    with open(config, "r") as f:
        cfg = json.load(f)
    postgres_cfg = cfg.get("postgres")
    postgres_cfg = {
        key: postgres_cfg.get(key) for key in ["host", "user", "port", "password"]
    }
    pony = Pony(db, **postgres_cfg)
    return pony


def _read_file(fname):
    with open(fname) as csvfile:
        reader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(reader, None)
        for doi, summary, conclusion in reader:
            data = {
                "doi": doi,
                "summary": summary,
                "conclusion": conclusion,
                "date_added": datetime.now(),
                "scitldr_version": "0.0.0",
            }
            yield data


def files_to_db(fnames):
    db = get_database()
    for fname in fnames:
        reader = _read_file(fname)
    db.add_summary(reader)
