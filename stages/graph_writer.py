from functools import lru_cache
import os
import csv
import tempfile
from datetime import datetime
from abc import ABC
from pathlib import Path
from typing import List
import shutil

import more_itertools

from utils.logging import PipelineLogger
from stages.utils import git_hash

logger = PipelineLogger("Neo4j")

DATEFORMAT: str = "%Y-%d-%m"
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
    source_table: str = None
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
        """
        self.source_table = "concept_nodes"
        self.columns = ["cui", "name", "date_added", "version"]

    def format_data(self, record) -> dict:
        data = {
            "cui": record.cui,
            "name": record.name,
            "version": record._version or GIT_VERSION,
            "date_added": record._date_added.strftime(DATEFORMAT),
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
            "cui": record.cui,
            "name": record.name,
            "version": record._version or GIT_VERSION,
            "date_added": record._date_added.strftime(DATEFORMAT),
        }
        return data


class PredicateEdgeIF(GraphDBInterface):
    def __init__(self):
        self.name = "Predicate edge"
        self.source_table = "predicate_edges"
        self.stmt = f"""
    MATCH
        (a:concept),
        (b:concept) 
    WHERE 
        a.cui = row.cui_left AND 
        b.cui = row.cui_right
    MERGE 
        (a)-[r:_VERB {{doi:row.doi}}]->(b)
        SET r.name = row.name
        SET r.summary = row.summary
        SET r.conclusion = row.conclusion
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
            "cui_left",
            "cui_right",
            "version",
        ]

    def format_data(self, record) -> dict:
        data = {
            "name": record.name,
            "conclusion": record.conclusion,
            "summary": record.summary,
            "doi": record.doi,
            "cui_left": record.cui_left,
            "cui_right": record.cui_right,
            "date_added": record._date_added.strftime(DATEFORMAT),
            "version": record._version or GIT_VERSION,
        }
        return data


class GraphWriter:
    def __init__(self, db: "Database", graph_db: "GraphDatabase"):
        self.db = db
        self.graph_db = graph_db

    def _format_value(self, val, quote="'"):

        if isinstance(val, str):
            val = val.replace("'", '"')
            val = f"{quote}{val}{quote}"
        elif isinstance(val, datetime):
            val = val.strftime(DATEFORMAT)
        return val

    def _add_elems(self, db_interface, write=False, batch_size=5000):
        source_table = db_interface.source_table
        records = self.db.get_records(source_table)
        self.batch_load(
            db_interface=db_interface, data=records, write=write, batch_size=batch_size
        )

    def add_concepts(self, write=False, batch_size=5000):
        interface = ConceptNodeIF()
        self._add_elems(db_interface=interface, write=write, batch_size=batch_size)

    def add_synonyms(self, write=False, batch_size=5000):
        interface = SynonymNodeIF()
        self._add_elems(db_interface=interface, write=write, batch_size=batch_size)

    def add_predicates(self, write=False, batch_size=200):
        interface = PredicateEdgeIF()
        self._add_elems(db_interface=interface, write=write, batch_size=batch_size)

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
