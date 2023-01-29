"""Database interface"""

import json
import logging
from datetime import datetime
from pony.orm import (
    commit,
    select,
    db_session,
)
from pony.orm.core import EntityMeta

from utils.run_modes import RunModes
from utils.logging import PipelineLogger

from models.db_tables import (
    db,
    Article,
    Summary,
    Abbreviation,
    SimpleConclusions,
    SimpleSubstitutedConclusions,
    NamedEntity,
    Node,
    Edge,
    ConceptNode,
    SynonymNode,
    PredicateEdge,
    SynonymEdge,
    Log,
)

logger = PipelineLogger("Postgres")


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
        self.simple_conclusions = SimpleConclusions
        self.simple_substituted_conclusions = SimpleSubstitutedConclusions
        self.named_entities = NamedEntity
        self.nodes = Node
        self.concept_nodes = ConceptNode
        self.synonym_nodes = SynonymNode
        self.edges = Edge
        self.predicate_edges = PredicateEdge
        self.synonym_edges = SynonymEdge
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
        for e, elem in enumerate(data):
            try:
                last_record = table(**elem)
            except TypeError as e:
                print("***")
                print(elem)
                print(f"***{table}***")
                raise (e)
            if not e % periodic_commit:
                # logging.debug("Processing %s: %s" % (e, last_record))
                self._commit(table, last_record)
        # finally:
        self._commit(table, last_record)
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
                    try:
                        filtered_records = [elem[tbl._table_[-1]]]
                    except KeyError:
                        continue
                    for filtered_record in filtered_records:
                        last_id = self._add_record(filtered_record, tbl, periodic_commit)
            return last_id
        raise TypeError("Expected table, name, or dict  got %s" % type(table))

    def get_by_id(self, table, id):
        return table[id]

    def _build_query(self, table, mode, downstream=None, order_by=None):
        if isinstance(table, str):
            table = getattr(self, table)
        if not isinstance(table, EntityMeta):
            raise (ValueError, "No table with with that name")
        select_functions = {
            "ALL": self._get_all_records,
            "FRESH": self._get_unprocessed_records,
            "NEWER": self._get_newer_records,
        }
        try:
            select_function = select_functions[mode.name]
        except KeyError as e:
            raise KeyError(
                f"Unknown mode '{mode}'. Allowed values are {','.join(select_functions.keys())}"
            )
        query = select_function(table, downstream)
        if order_by is not None:
            column = getattr(table, order_by)
            query = query.order_by(column)
        return query

    def get_records(
        self, table, mode: RunModes = RunModes.ALL, downstream=None, order_by=None
    ):
        elems = self._build_query(
            table=table, mode=mode, downstream=downstream, order_by=order_by
        )
        yield from elems

    def get_unique_nodes(self):
        nodes = select((n.cui, n.matched, n.preferred) for n in self.nodes).order_by(
            lambda cui, matched, preferred: preferred
        )
        yield from nodes

    def get_unique_edges(self):
        edges = select(e for e in self.edges)
        yield from edges

    @db_session
    def count_records(self, table, mode, downstream=None):
        query = self._build_query(
            table=table, mode=mode, downstream=downstream, order_by=None
        )
        n_elems = query.count()
        return n_elems

    def _get_all_records(self, table, downstream):
        elems = select(c for c in table)
        return elems

    def _get_unprocessed_records(self, table, downstream):
        if isinstance(downstream, list):
            downstream = downstream[0]
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
