from functools import lru_cache
import os
import csv
import tempfile
from itertools import groupby
from datetime import datetime

import more_itertools

from connectors.neo4j import Node, Edge
from pony.orm import db_session
from utils.run_modes import RunModes
from utils.logging import PipelineLogger

logger = PipelineLogger("Neo4j")


class GraphWriter:
    def __init__(self, db, graph_db):
        self.db = db
        self.graph_db = graph_db

        self.elem_types = {
            "concept_nodes": self.load_concept_nodes,
            "synonym_nodes": self.load_synonym_nodes,
            "predicate_edges": self.load_predicate_edges,
            "synonym_edges": self.load_synonym_edges,
        }

    def _format_value(self, val, quote="'"):

        if isinstance(val, str):
            val = val.replace("'", '"')
            val = f"{quote}{val}{quote}"
        return val

    def _serialize_props(self, data, quote=True):
        quote = "'" if quote else ""
        if not data:
            datastring = ""
        else:
            labels = [
                f"{label}:{self._format_value(value, quote=quote)}"
                for label, value in data.items()
                if value is not None
            ]
            datastring = " {%s}" % ", ".join(labels)
        return datastring

    @property
    @lru_cache
    def _load_date(self):
        date = datetime.now().strftime("%Y-%m-%d")
        return date

    def load_concept_nodes(self):
        def format_data(record):
            data = {
                "node_id": record.node_id,
                "cui": record.cui,
                "name": record.name,
                "_version": record._version,
                "date_added": record._date_added,
            }
            return data

        data = {
            "node_id": "row.node_id",
            "cui": "row.cui",
            "name": "row.name",
            "date_added": self._load_date,
        }
        datastring = self._serialize_props(data, quote=False)
        source_table = "concept_nodes"
        batch_stmt = f"MERGE (a:concept {datastring})"
        columns = data.keys()
        return source_table, batch_stmt, format_data, columns

    def load_synonym_nodes(self):
        def format_data(record):
            data = {
                "node_id": record.node_id,
                "cui": record.cui,
                "name": record.name,
                "_version": record._version,
                "date_added": record._date_added,
            }
            return data

        data = {
            "node_id": "row.node_id",
            "cui": "row.cui",
            "name": "row.name",
            "date_added": self._load_date,
        }
        datastring = self._serialize_props(data, quote=False)
        source_table = "synonym_nodes"
        batch_stmt = f"MERGE (a:synonym {datastring})"
        columns = data.keys()
        return source_table, batch_stmt, format_data, columns

    def load_predicate_edges(self):
        def format_data(record):
            data = {
                "name": record.name,
                "conclusion": record.conclusion,
                "summary": record.summary,
                "doi": record.doi,
                "predicate": record.predicate,
                "cui_left": record.cui_left,
                "cui_right": record.cui_right,
            }
            return data

        data = {
            "name": "row.name",
            "doi": "row.doi",
            "summary": "row.summary",
            "conclusion": "row.conclusion",
            "date_added": self._load_date,
        }
        datastring = self._serialize_props(data, quote=False)
        source_table = "synonym_nodes"
        batch_stmt = f"""
    MATCH
        (a:concept),
        (b:concept) 
    WHERE 
        a.cui = row.cui_left AND 
        b.cui = row.cui_right
    MERGE 
        (a)-[r:_VERB {datastring}]->(b)
    RETURN type(r)
        """
        columns = data.keys() + ["cui_left", "cui_right"]
        return source_table, batch_stmt, format_data, columns

    def load_synonym_edges(self):
        def format_data(record):
            data = {"id_left": record.id_left, "id_right": record.id_right}
            return data

        data = {"date_added": self._load_date}
        datastring = self._serialize_props(data, quote=False)
        source_table = "synonym_nodes"
        batch_stmt = f"""
    MATCH
        (a:synonym),
        (b:concept) 
    WHERE 
        a.node_left = row.id_left
        b.node_right = row.id_right
    MERGE 
        (a)-[r:_SYN {datastring}]->(b)
    RETURN type(r)
        """
        columns = data.keys() + ["id_left", "id_right"]
        return source_table, batch_stmt, format_data, columns

    @db_session
    def add_edges(self, write=False, batch_size=10_000):
        records = self.db.get_unique_edges()
        self.batch_load_edges(
            edge_type="_VERB", edges=records, batch_size=batch_size, write=write
        )

    @db_session
    def add_concepts(self, write=False, batch_size=10_000):
        records = self.db.get_records("concept_nodes")
        elem_type = "concept_nodes"
        self.batch_load(
            elem_type=elem_type, data=records, write=write, batch_size=batch_size
        )

    @db_session
    def add_synonyms(self, write=False, batch_size=10_000):
        records = self.db.get_records("synonym_nodes")
        elem_type = "synonym_nodes"
        self.batch_load(
            elem_type=elem_type, data=records, write=write, batch_size=batch_size
        )

    @db_session
    def add_predicates(self, write=False, batch_size=10_000):
        records = self.db.get_records("predicate_nodes")
        elem_type = "predicate_nodes"
        self.batch_load(
            elem_type=elem_type, data=records, write=write, batch_size=batch_size
        )

    @db_session
    def add_synonyms_edges(self, write=False, batch_size=10_000):
        records = self.db.get_records("synonym_edges")
        elem_type = "synonym_edges"
        self.batch_load(
            elem_type=elem_type, data=records, write=write, batch_size=batch_size
        )

    def _load_batch(self, batch, elem_specs, temp_dir, write, importdiroffset):
        source_table, batch_stmt, format_data, columns = elem_specs()
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            encoding="utf-8",
            delete=False,
            newline=os.linesep,
            dir=temp_dir,
        ) as f:
            temp_file = os.path.join(temp_dir, f.name)
            logger.debug(f"Created temporary file at '{temp_file}'")

            writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(columns)
            for record in batch:
                data = format_data(record)
                row = [data[col] for col in columns]
                writer.writerow(row)
        temp_file_relative = temp_file[importdiroffset + 1 :]
        query = (
            f'LOAD CSV WITH HEADERS FROM "file:///{temp_file_relative}" AS row '
            + batch_stmt
        )
        if write:
            result = self.graph_db.query(query, out="list")
            return result
        else:
            print(query)

    def batch_load(self, elem_type, data, write=True, batch_size=5000):
        neo4j_import_dir = self.graph_db.import_dir
        logger.debug(f"Found import directory at '{neo4j_import_dir}'.")
        try:
            elem_specs = self.elem_types[elem_type]
        except KeyError:
            raise KeyError(
                f"Invalid element type: '{elem_type}'. Allowed are '{', '.join(self.elem_types.keys())}'"
            )
        with tempfile.TemporaryDirectory(dir=neo4j_import_dir) as temp_dir:
            for batch in more_itertools.ichunked(data, batch_size):
                result = self._load_batch(
                    batch,
                    elem_specs,
                    temp_dir,
                    write,
                    importdiroffset=len(neo4j_import_dir),
                )
                logger.debug(
                    f"Loaded {len(result)} new records (batch size = {batch_size})."
                )

    def migrate(self, mode="Rebuild"):
        pass
