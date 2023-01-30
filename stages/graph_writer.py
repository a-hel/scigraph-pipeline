from functools import lru_cache
import os
import csv
import tempfile
from itertools import groupby
from datetime import datetime

import more_itertools

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
            "relational_edges": self.load_rel_edges,
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
            attr = record.attributes
            data = {
                "node_id": record.id,
                "cui": record.cui_or_name,
                "name": attr["canonical_name"],
                "definition": attr["definition"],
            }

            return data

        data = {
            "node_id": "row.node_id",
            "cui": "row.cui",
            "name": "row.name",
            "definition": "row.definition",
            "date_added": self._load_date,
        }
        datastring = self._serialize_props(data, quote=False)
        batch_stmt = f"MERGE (a:concept {datastring})"
        columns = ["node_id", "cui", "name", "definition"]
        return batch_stmt, format_data, columns

    def load_synonym_nodes(self):
        def format_data(record):
            data = {
                "node_id": record.id,
                "cui": record.cui_or_name,
                "name": record.attributes["synonym"],
            }
            return data

        data = {
            "node_id": "row.node_id",
            "cui": "row.cui",
            "name": "row.name",
            "date_added": self._load_date,
        }
        datastring = self._serialize_props(data, quote=False)
        batch_stmt = f"MERGE (a:synonym {datastring})"
        columns = ["node_id", "cui", "name"]
        return batch_stmt, format_data, columns

    def load_predicate_edges(self):
        def format_data(record):
            attr = record.attributes
            data = {
                "name": attr["name"],
                "conclusion": attr["conclusion"],
                "summary": attr["summary"],
                "doi": attr["doi"],
                "predicate": attr["name"],
                "cui_left": record.node_left,
                "cui_right": record.node_right,
            }
            return data

        data = {
            "name": "row.name",
            "doi": "row.doi",
            "predicate": "row.predicate",
            "summary": "row.summary",
            "conclusion": "row.conclusion",
            "date_added": self._load_date,
        }
        datastring = self._serialize_props(data, quote=False)
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
        columns = [
            "cui_left",
            "cui_right",
            "name",
            "doi",
            "predicate",
            "summary",
            "conclusion",
        ]
        return batch_stmt, format_data, columns

    def load_rel_edges(self):
        def format_data(record):
            data = {
                "cui_left": record.node_left,
                "cui_right": record.node_right,
                "doi": record.attributes["doi"],
            }
            return data

        data = {"date_added": self._load_date, "doi": "row.doi"}
        datastring = self._serialize_props(data, quote=False)
        batch_stmt = f"""
    MATCH
        (a:concept),
        (b:concept) 
    WHERE 
        a.cui = row.cui_left AND 
        b.cui = row.cui_right
    MERGE 
        (a)-[r:_REL {datastring}]->(b)
    RETURN type(r)
        """
        columns = ["cui_left", "cui_right", "doi"]
        return batch_stmt, format_data, columns

    @db_session
    def add_edges(self, write=False, batch_size=10_000):
        elem_types = {"_VERB": "predicate_edges", "_REL": "relational_edges"}
        records = self.db.get_records("edges", order_by="edge_type")
        for group, recs in groupby(records, key=lambda x: x.edge_type):
            elem_type = elem_types[group]
            self.batch_load(
                elem_type=elem_type, data=recs, write=write, batch_size=batch_size
            )
        self.add_synonyms_edges()

    @db_session
    def add_nodes(self, write=False, batch_size=10_000):
        elem_types = {"concept": "concept_nodes", "synonym": "synonym_nodes"}
        records = self.db.get_records("nodes", order_by="node_type")
        for group, recs in groupby(records, key=lambda x: x.node_type):
            elem_type = elem_types[group]
            self.batch_load(
                elem_type=elem_type, data=recs, write=write, batch_size=batch_size
            )

    def add_synonyms_edges(self, write=False, batch_size=10_000):
        stmt = """
    MATCH
        (a:synonym),
        (b:concept) 
    WHERE 
        a.cui = b.cui
    MERGE 
        (a)-[r:_SYN]->(b)
    RETURN type(r)
        """
        self.graph_db.query(stmt)

    def _load_batch(self, batch, elem_specs, temp_dir, write, importdiroffset):
        batch_stmt, format_data, columns = elem_specs()
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
                    f"Loaded {len(result or [])} new records (batch size = {batch_size})."
                )

    def migrate(self, mode="Rebuild"):
        pass
