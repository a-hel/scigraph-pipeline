import os
import csv
import tempfile
from itertools import groupby

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
        self.node_props = {"synonym": ["cui", "name"], "concept": ["cui", "name"]}
        self.edge_props = {"_REL": ["doi", "summary", "conclusion"], "_SYN": []}

    def _format_value(self, val):
        if isinstance(val, str):
            val = val.replace("'", '"')
            return f"'{val}'"
        return val

    def _serialize_props(self, data):
        if not data:
            datastring = ""
        else:
            labels = [
                f"{label}:{self._format_value(value)}"
                for label, value in data.items()
                if value is not None
            ]
            datastring = " {%s}" % ", ".join(labels)
        return datastring

    @db_session
    def add_edges(self, write=False):
        records = self.db.get_unique_edges()
        self.batch_load_edges(edge_type="_REL", edges=records, batch_size=10)

    def edge_create_stmt(
        self,
        edgetype,
        nodetype_left,
        nodetype_right,
        join_key_left,
        join_key_right,
        join_value_left,
        join_value_right,
        _props,
    ):
        datastring = self._serialize_props(_props)
        stmt = f"""
    MATCH
        (a:{nodetype_left}),
        (b:{nodetype_right}) 
    WHERE 
        a.{join_key_left} = '{join_value_left}' AND 
        b.{join_key_right} = '{join_value_right}'
    MERGE 
        (a)-[r:{edgetype}{datastring}]->(b)
    RETURN type(r)
    """
        return stmt

    def node_create_stmt(self, nodetype, _props):

        datastring = self._serialize_props(_props)
        stmt = f"""
    MERGE (a:{nodetype} {datastring})"
        """
        return stmt

    def _load_batch(self, batch, stmt_func, columns, temp_dir, write):
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            encoding="utf-8",
            delete=False,
            newline=os.linesep,
            dir=temp_dir,
        ) as f:
            temp_file = os.path.join(temp_dir, f.name)

            writer = csv.writer(f)
            writer.writerow(columns)
            for record in batch:
                data = {
                    "conclusion": record.summary_id.conclusion,
                    "summary": record.summary_id.summary,
                    "doi": record.summary_id.article_id.doi,
                    "predicate": record.predicate,
                }
                elem = Edge(record.cui_left, record.cui_right, "_REL", data=data)
                row = [elem[col] for col in columns]
                writer.writerow(row)
                print(",".join(row))
        query = f"LOAD CSV WITH HEADERS FROM {temp_file}" + stmt_func
        if write:
            self.graph_db.query(query)
        else:
            print(query)

    def batch_load_nodes(self, node_type, nodes, batch_size=10_000, write=True):
        fields = ["nodetype"]
        try:
            props = self.node_props[node_type]

        except KeyError:
            raise KeyError(
                f"Invalid node type: '{node_type}. Allowed are '{', '.join(self.node_props.keys())}'"
            )
        fields_data = {col: f"row.{col}" for col in fields}
        props_data = {col: f"row.{col}" for col in props}
        data = fields_data
        data["_props"] = props_data
        stmt_func = self.node_create_stmt(**data)
        columns = self.node_props
        with tempfile.TemporaryDirectory() as temp_dir:
            for batch in more_itertools.ichunked(nodes, batch_size):
                self._load_batch(batch, stmt_func, columns, temp_dir, write)

    def batch_load_edges(self, edge_type, edges, batch_size=10_000, write=True):
        fields = [
            "edgetype",
            "nodetype_left",
            "nodetype_right",
            "join_key_left",
            "join_key_right",
            "join_value_left",
            "join_value_right",
        ]
        try:
            props = self.edge_props[edge_type]
        except KeyError:
            raise KeyError(
                f"Invalid edge type: '{edge_type}. Allowed are '{', '.join(self.edge_props.keys())}'"
            )
        fields_data = {col: f"row.{col}" for col in fields}
        props_data = {col: f"row.{col}" for col in props}
        data = fields_data
        data["_props"] = props_data
        stmt_func = self.edge_create_stmt(**data)
        columns = self.node_props
        with tempfile.TemporaryDirectory() as temp_dir:
            for batch in more_itertools.ichunked(edges, batch_size):
                self._load_batch(batch, stmt_func, columns, temp_dir, write)
                break


@db_session
def add_nodes(db, graph_db, write=False):
    records = db.get_unique_nodes()
    for preferred, nodes in groupby(records, key=lambda x: x[2]):
        all_nodes = list(set(nodes))
        cui, matched, preferred = all_nodes[0]
        preferred_data = {"cui": cui, "name": preferred}
        preferred_node = Node(nodetype="concept", data=preferred_data)
        if write:
            graph_db.add_node(preferred_node)
        for node in all_nodes:
            cui, matched, preferred = node
            synonym_data = {"cui": cui, "name": matched}
            synonym_node = Node(nodetype="synonym", data=synonym_data)
            synonym_edge = Edge(preferred_node, synonym_node, "_SYN")
            if write:
                graph_db.add_node(synonym_node)
                graph_db.add_edge(synonym_edge)
