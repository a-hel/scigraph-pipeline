from functools import lru_cache
import os
import csv
import tempfile
from itertools import groupby
from datetime import datetime
from datetime import datetime
from abc import ABC
from pathlib import Path
from typing import List
import shutil

import more_itertools

from utils.run_modes import RunModes
from utils.logging import PipelineLogger
from stages.utils import git_hash

logger = PipelineLogger("Neo4j")

DATEFORMAT: str = "%Y-%m-%d"
DEBUG = False
GIT_VERSION = git_hash()


@lru_cache
def _load_date() -> str:
    date = datetime.now().strftime(DATEFORMAT)
    return date


class GraphDBInterface(ABC):
    name: str = None
    props: list = None
    stmt: str = None
    label: str = None
    data: dict = None
    columns: list = None

    def format_data(self, record):
        pass


class ConceptNodeIF(GraphDBInterface):
    def __init__(self):
        self.name = "Concept node"
        self.props = ""
        self.stmt = f"""
    MERGE (a:concept {{cui: row.cui}})
        SET a.name = row.name
        SET a.date_added = date("{_load_date()}")
        SET a.version = row.version
        SET a.definition = row.definition
        """
        self.columns = ["node_id", "cui", "name", "definition", "version"]

    def format_data(self, record) -> dict:
        attr = record.attributes
        data = {
            "node_id": record.id,
            "cui": record.cui_or_name,
            "name": attr["canonical_name"],
            "definition": attr["definition"],
            "version": GIT_VERSION,
        }
        return data


class SynonymNodeIF(GraphDBInterface):
    def __init__(self):
        self.name = "Synonym node"
        self.stmt = f"""
        MERGE (a:synonym {{cui: row.cui, name: row.name, date_added: date("{_load_date()}"), version: row.version}})
        RETURN count(a)
        """
        self.source_table = "synonym_nodes"
        self.columns = ["cui", "name", "version"]

    def format_data(self, record) -> dict:
        data = {
            "cui": record.cui_or_name,
            "name": record.cui_or_name,
            "version": GIT_VERSION,
        }
        return data


class PredicateEdgeIF(GraphDBInterface):
    def __init__(self):
        self.name = "Predicate edge"
        self.source_table = "edges"
        self.label = "_VERB"
        self.stmt = f"""
    MATCH
        (a:concept),
        (b:concept) 
    WHERE 
        b.cui = row.cui_left AND 
        a.cui = row.cui_right
    MERGE 
        (a)-[r:{self.label} {{doi:row.doi}}]->(b)
        SET r.name = row.name
        SET r.summary = row.summary
        SET r.conclusion = row.conclusion
        SET r.predicate = row.predicate
        SET r.date_added = date("{_load_date()}")
        SET r.version = row.version
    RETURN
        count(r)
        
        """
        self.columns = [
            "name",
            "conclusion",
            "summary",
            "doi",
            "predicate",
            "cui_left",
            "cui_right",
            "version",
        ]

    def format_data(self, record) -> dict:
        attr = record.attributes
        data = {
            "name": attr["name"],
            "conclusion": attr["conclusion"],
            "summary": attr["summary"],
            "doi": attr["doi"],
            "predicate": attr["name"],
            "cui_left": record.node_left,
            "cui_right": record.node_right,
            "version": GIT_VERSION,
        }
        return data


class RelationalEdgeIF(GraphDBInterface):
    def __init__(self):
        self.name = "Relational edge"
        self.source_table = "edges"
        self.label = "_REL"
        self.stmt = f"""
    MATCH
        (a:concept),
        (b:concept) 
    WHERE 
        b.cui = row.cui_left AND 
        a.cui = row.cui_right
    MERGE 
        (a)-[r:{self.label} {{doi:row.doi}}]->(b)
        SET r.date_added = date("{_load_date()}")
        SET r.version = row.version
    RETURN
        count(r)
        
        """
        self.columns = [
            "doi",
            "cui_left",
            "cui_right",
            "version",
        ]

    def format_data(self, record) -> dict:
        attr = record.attributes
        data = {
            "doi": attr["doi"],
            "cui_left": record.node_left,
            "cui_right": record.node_right,
            "version": GIT_VERSION,
        }
        return data


class GraphWriter:
    def __init__(self, db, graph_db):
        self.db = db
        self.graph_db = graph_db

    def _format_value(self, val, quote="'"):

        if isinstance(val, str):
            val = val.replace("'", '"')
            val = f"{quote}{val}{quote}"
        elif isinstance(val, datetime):
            val = val.strftime(DATEFORMAT)
        return val

    def _add_elems(self, db_interface, records, write=False, batch_size=5000):
        with self.db.session_handler():
            source_table = db_interface.source_table
            records = self.db.get_records(source_table)
            self.batch_load(
                db_interface=db_interface,
                data=records,
                write=write,
                batch_size=batch_size,
            )

    def add_edges(self, write=False, batch_size=10_000):
        interfaces = {"_VERB": PredicateEdgeIF, "_REL": RelationalEdgeIF}
        with self.db.session_handler():
            records = self.db.get_records("edges", order_by="edge_type")
            for group, recs in groupby(records, key=lambda x: x.edge_type):
                interface = interfaces[group]()
                self.batch_load(
                    db_interface=interface,
                    data=recs,
                    write=write,
                    batch_size=batch_size,
                )
        self.add_synonyms_edges(write=write, batch_size=batch_size)

    def add_nodes(self, write=False, batch_size=10_000):
        interfaces = {"concept": ConceptNodeIF, "synonym": SynonymNodeIF}
        with self.db.session_handler():
            records = self.db.get_records("nodes", order_by="node_type")
            for group, recs in groupby(records, key=lambda x: x.node_type):
                interface = interfaces[group]()
                self.batch_load(
                    db_interface=interface,
                    data=recs,
                    write=write,
                    batch_size=batch_size,
                )

    def add_synonyms_edges(self, write=False, batch_size=0):
        stmt = """
    MATCH
        (a:synonym),
        (b:concept) 
    WHERE
        a.cui = b.cui
    MERGE 
        (a)-[r:_SYN ]->(b)
    RETURN
        count(r)"""
        if write:
            self.graph_db.query(stmt)
        else:
            print(stmt)

    def _load_batch(
        self, batch, db_interface: GraphDBInterface, temp_dir: Path, write: bool
    ):
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            encoding="utf-8",
            delete=False,
            newline=os.linesep,
            dir=temp_dir,
        ) as f:
            temp_file = temp_dir / f.name
            logger.debug(f"Created temporary file at '{temp_file}'")

            writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(db_interface.columns)
            for record in batch:
                data = db_interface.format_data(record)
                row = [data[col] for col in db_interface.columns]
                writer.writerow(row)
        temp_file_relative = Path(*temp_file.parts[-2:])
        # datastring = self._serialize_props(db_interface.data, quote=False)
        query = (
            f'LOAD CSV WITH HEADERS FROM "file:///{temp_file_relative}" AS row '
            + db_interface.stmt
        )

        if DEBUG:
            shutil.copyfile(src=temp_file, dst=f"debug_{_load_date()}.csv")
        if write:
            result = self.graph_db.query(query, out="list")
            return result
        else:
            logger.info(query)

    def _count_results(self, n_results: List["Record"]) -> str:
        try:
            n = "%d" % n_results[0].value()
        except (IndexError, AttributeError):
            n = "n/a"
        return n

    def batch_load(
        self,
        db_interface: GraphDBInterface,
        data,
        write: bool = True,
        batch_size: int = 5000,
    ):
        neo4j_import_dir = self.graph_db.import_dir
        logger.debug(f"Found import directory at '{neo4j_import_dir}'.")
        with tempfile.TemporaryDirectory(dir=neo4j_import_dir) as temp_dir:
            temp_dir_path = Path(temp_dir)
            for batch in more_itertools.ichunked(data, batch_size):
                n_results = self._load_batch(batch, db_interface, temp_dir_path, write)
                logger.debug(
                    f"Loaded {self._count_results(n_results)} new records (batch size = {batch_size})."
                )
