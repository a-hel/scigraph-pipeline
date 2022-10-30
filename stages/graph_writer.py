from functools import lru_cache
import os
import csv
import tempfile
from itertools import groupby
from datetime import datetime
from abc import ABC
from pathlib import Path

import more_itertools

from connectors.neo4j import Node, Edge
from pony.orm import commit, db_session, rollback
from utils.run_modes import RunModes
from utils.logging import PipelineLogger

logger = PipelineLogger("Neo4j")


@lru_cache
def _load_date() -> str:
    date = datetime.now().strftime("%Y-%m-%d")
    return date


class GraphDBInterface(ABC):
    name: str = None
    props: list = None
    stmt: str = None
    source_table: str = None
    label: str = None
    data: dict = None

    def format_data(self, record):
        pass


class ConceptNodeIF(GraphDBInterface):
    def __init__(self):
        self.name = "Concept node"
        self.props = ""
        self.stmt = "MERGE (a:concept {datastring}) return count(a)"
        self.data = {
            "node_id": "row.node_id",
            "cui": "row.cui",
            "name": "row.name",
            "date_added": _load_date(),
        }
        self.source_table = "concept_nodes"
        self.columns = list(self.data.keys())

    def format_data(self, record) -> dict:
        data = {
            "node_id": record.node_id,
            "cui": record.cui,
            "name": record.name,
            "_version": record._version,
            "date_added": record._date_added,
        }
        return data


class SynonymNode(GraphDBInterface):
    def __init__(self):
        self.name = "Synonym node"
        self.props = ""
        self.stmt = "MERGE (a:synonym {datastring})"
        self.data = {
            "node_id": "toInteger(row.node_id)",
            "cui": "row.cui",
            "name": "row.name",
            "date_added": _load_date(),
        }
        self.source_table = "synonym_nodes"

    def format_data(self, record) -> dict:
        data = {
            "node_id": record.node_id,
            "cui": record.cui,
            "name": record.name,
            "_version": record._version,
            "date_added": record._date_added,
        }
        return data


class PredicateEdge(GraphDBInterface):
    def __init__(self):
        self.name = "Predicate edge"
        self.data = {
            "name": "row.name",
            "doi": "row.doi",
            "summary": "row.summary",
            "conclusion": "row.conclusion",
            "date_added": _load_date(),
        }
        self.source_table = "predicate_edges"
        self.stmt = """
    MATCH
        (a:concept),
        (b:concept) 
    WHERE 
        a.cui = row.cui_left AND 
        b.cui = row.cui_right
    MERGE 
        (a)-[r:_VERB {datastring}]->(b)
        
        """

    def format_data(self, record) -> dict:
        data = {
            "name": record.name,
            "conclusion": record.conclusion,
            "summary": record.summary,
            "doi": record.doi,
            "predicate": record.name,
            "cui_left": record.cui_left,
            "cui_right": record.cui_right,
            "date_added": record._date_added,
        }
        return data


class SynonymEdge(GraphDBInterface):
    def __init__(self):
        self.name = "Synonym edge"
        self.data = {"date_added": _load_date()}
        self.source_table = "synonym_edges"
        self.stmt = """
    MATCH
        (a:synonym),
        (b:concept) 
    WHERE
        a.node_id = toInteger(row.id_left) AND
        b.node_id = toInteger(row.id_right)
    MERGE 
        (a)-[r:_SYN {datastring}]->(b)
        """

    def format_data(self, record) -> dict:
        data = {
            "id_left": record.id_left,
            "id_right": record.id_right,
            "name_left": record.name_left,
            "name_right": record.name_right,
            "date_added": record._date_added,
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
        return val

    def _serialize_props(self, data: dict, quote: bool = True) -> str:
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

    def add_edges(self, write=False, batch_size=10_000):
        records = self.db.get_unique_edges()
        self.batch_load_edges(
            edge_type="_VERB", edges=records, batch_size=batch_size, write=write
        )

    def _add_elems(self, db_interface, write=False, batch_size=5000):
        source_table = db_interface.source_table
        records = self.db.get_records(source_table)
        self.batch_load(
            db_interface=db_interface, data=records, write=write, batch_size=batch_size
        )

    def add_concepts(self, write=False, batch_size=5000):
        interface = ConceptNodeIF()
        self._add_elems(db_interface=interface, write=write, batch_size=batch_size)

    @db_session
    def add_synonyms(self, write=False, batch_size=5000):
        records = self.db.get_records("synonym_nodes")
        elem_type = "synonym_nodes"
        self.batch_load(
            elem_type=elem_type, data=records, write=write, batch_size=batch_size
        )

    @db_session
    def add_predicates(self, write=False, batch_size=200):
        records = self.db.get_records("predicate_edges")
        elem_type = "predicate_edges"
        self.batch_load(
            elem_type=elem_type, data=records, write=write, batch_size=batch_size
        )

    @db_session
    def add_synonyms_edges(self, write=False, batch_size=200):
        records = self.db.get_records("synonym_edges")
        elem_type = "synonym_edges"
        self.batch_load(
            elem_type=elem_type, data=records, write=write, batch_size=batch_size
        )

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
        datastring = self._serialize_props(db_interface.data, quote=False)
        query = (
            f'LOAD CSV WITH HEADERS FROM "file:///{temp_file_relative}" AS row '
            + db_interface.stmt.format(datastring=datastring)
        )
        if write:
            result = self.graph_db.query(query, out="list")
            return result
        else:
            print(query)

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
                    f"Loaded {n_results or 'n/a'} new records (batch size = {batch_size})."
                )
                break

    def migrate(self, mode="Rebuild"):
        pass
