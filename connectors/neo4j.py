import json

from typing import Optional

from neo4j import GraphDatabase
from pydantic import BaseModel, validator

from utils.logging import PipelineLogger

logger = PipelineLogger("Neo4j")


class NodeData(BaseModel):
    cui: str
    name: str
    version: Optional[str] = "test"

    @validator("name")
    def escape_chars(cls, val):
        val = val.replace("'", " ")
        return val


class RelationshipEdgeData(BaseModel):
    doi: str
    summary: str
    conclusion: str
    predicate: str
    version: Optional[str] = "test"


class SynonymEdgeData(BaseModel):
    version: Optional[str] = "test"


class GraphDB:
    def __init__(self, **config):
        self.username = config.get("username")
        self.dbname = config.get("database")
        self.host = config.get("host")
        self.port = config.get("port")
        self.uri = "bolt://%s:%s/%s" % (self.host, self.port, self.dbname)
        # self.uri = "bolt://%s:%s" % (self.host, self.port)
        logger.debug("Connecting to %s" % self.uri)
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.username, config.get("password")),
            encrypted=config.get("encryption", True),
        )
        with self.driver.session() as session:
            _ = session.run("match(n) return n;")

    def _as_graph(self, res):
        return res.graph()

    def _as_list(self, res):
        return list(res)

    def query(self, query, out="graph", **kwargs):
        output = {"graph": self._as_graph, "list": self._as_list}[out]
        logger.debug("Running query: <%s>" % query)
        with self.driver.session() as session:
            try:
                result = session.run(query, **kwargs)
            except Exception as e:
                logger.error(
                    f"Query could not complete successfully:\n\n{query}\n\n{e}"
                )
                raise e
            result = output(result)
        return result

    def add_node(self, node):
        query = node.create_stmt()
        result = self.query(query, **node.data.dict())
        return result

    def add_edge(self, edge):
        query = edge.create_stmt()
        result = self.query(query, **edge.data.dict() or {})
        return result

    @staticmethod
    def from_config(path="../config/dev.json", key="neo4j"):
        with open(path, "r") as f:
            config = json.load(f)
        if key is not None:
            config = config[key]
            logger.debug(f"Loaded Neo4jconfig section {key} form file {path}.")
        else:
            logger.debug(f"Loaded Neo4jconfig form file {path}.")
        return GraphDB(**config)


class Edge:
    def __init__(self, start, end, edgetype, data={}, match_on=["name", "name"]):
        data_models = {"_SYN": SynonymEdgeData, "_REL": RelationshipEdgeData}
        self.edgetype = edgetype
        try:
            data_model = data_models[self.edgetype]
        except KeyError:
            raise KeyError(f"Unsuported edge type: '{self.edgetype}'.")
        self.data = data_model(**data).dict()
        self.start = start
        self.end = end
        self.match_on_left, self.match_on_right = match_on
        if self.match_on_left not in self.start.data.dict():
            raise ValueError(
                "Unable to match on '%s'. Value not in left node (%s)."
                % (self.match_on_left, ", ".join(self.start.data.keys()))
            )
        if self.match_on_right not in self.end.data.dict():
            raise ValueError(
                "Unable to match on '%s'. Value not in right node" % self.match_on_right
            )
        self.nodetype_left = self.start.nodetype
        self.nodetype_right = self.end.nodetype
        self.join_key_left = self.match_on_left
        self.join_key_right = self.match_on_right
        self.join_value_left = self.start.data[self.match_on_left]
        self.join_value_right = self.end.data[self.match_on_right]

    def __repr__(self):
        rep = f"Edge('{self.start.data.name}', '{self.end.data.name}', edgetype='{self.edgetype}', data={self.data.__repr__()}, match_on=['{self.match_on_left}', '{self.match_on_left}']"
        return rep

    def __getitem__(self, key):
        try:
            item = getattr(self, key)
        except AttributeError:
            item = self.data[key]
        return item

    def update(self, data):
        self.data.update(data)

    def is_synonym(self):
        return self.edgetype == "_SYN"

    def _format_value(self, val):
        if isinstance(val, str):
            val = val.replace("'", '"')
            return f"'{val}'"
        return val

    def create_stmt(self):
        if not self.data:
            datastring = ""
        else:
            labels = [
                f"{label}:{self._format_value(value)}"
                for label, value in self.data.dict().items()
            ]
            datastring = " {%s}" % ", ".join(labels)
        query = f"""MATCH
(a:{self.start.nodetype}),
(b:{self.end.nodetype}) WHERE 
a.{self.match_on_left} = '{self.start.data.dict()[self.match_on_left]}' AND 
b.{self.match_on_right} = '{self.end.data.dict()[self.match_on_right]}'
MERGE (a)-[r:{self.edgetype}{datastring}]->(b)
RETURN type(r)"""
        return query


class Node:
    def __init__(self, nodetype, data={}):
        self.nodetype = nodetype
        self.data = NodeData(**data).dict()

    def __repr__(self):
        rep = f"Node(nodetype='{self.nodetype}', data={self.data.__repr__()})"
        return rep

    def __getitem__(self, key):
        try:
            item = getattr(self, key)
        except AttributeError:
            item = self.data[key]
        return item

    def set_type(self, nodetype):
        self.nodetype = nodetype

    def update(self, data):
        self.data.update(data)

    def _format_value(self, val):
        if isinstance(val, str):
            val = val.replace("'", '"')
            return f"'{val}'"
        return val

    def create_stmt(self):
        if not self.data:
            datastring = ""
        else:
            labels = [
                f"{label}:{self._format_value(value)}"
                for label, value in self.data.dict().items()
            ]
            datastring = " {%s}" % ", ".join(labels)
        query = f"MERGE (a:{self.nodetype} {datastring})"
        return query
