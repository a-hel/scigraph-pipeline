from neo4j import GraphDatabase


class DbDriver:
    def __init__(
        self, username, password, dbname="scigraph", host="localhost", port="7687"
    ):
        self.username = username
        self.dbname = dbname
        self.host = host
        self.port = port
        self.uri = "bolt://%s:%s/%s/data" % (host, port, dbname)
        self.driver = GraphDatabase.driver(self.uri, auth=(username, password))
        with self.driver.session() as session:
            _ = session.run("match(n) return n;")

    def _run_query(self, query, **kwargs):
        with self.driver.session() as session:
            result = session.run(query, **kwargs)
        return result

    def add_node(self, node):
        query = node.create_stmt()
        result = self._run_query(query, **node.data)
        return result

    def add_edge(self, edge):
        query = edge.create_stmt()
        result = self._run_query(query, **edge.data or {})
        return result


class Edge:
    def __init__(self, start, end, edgetype, data={}, match_on=["name", "name"]):
        self.edgetype = edgetype
        self.data = data
        self.start = start
        self.end = end
        self.match_on_left, self.match_on_right = match_on
        if self.match_on_left not in self.start.data:
            raise ValueError(
                "Unable to match on %s. Value not in left node" % self.match_on_left
            )
        if self.match_on_right not in self.end.data:
            raise ValueError(
                "Unable to match on %s. Value not in right node" % self.match_on_right
            )

    def __repr__(self):
        rep = f"Edge('{self.start.nodetype}', '{self.end.nodetype}', edgetype='{self.edgetype}', data={self.data.__repr__()}, match_on=['{self.match_on_left}', '{self.match_on_left}']"
        return rep

    def update(self, data):
        self.data.update(data)

    def is_synonym(self):
        return self.edgetype == "_SYN"

    def create_stmt(self):
        if not self.data:
            datastring = ""
        else:
            labels = ["{label}:${label}".format(label=label) for label in self.data]
            datastring = " {%s}" % ", ".join(labels)
        query = f"""MATCH
(a:{self.start.nodetype}),
(b:{self.end.nodetype}) WHERE 
a.{self.match_on_left} = '{self.start.data[self.match_on_left]}' AND 
b.{self.match_on_right} = '{self.end.data[self.match_on_right]}'
MERGE (a)-[r:{self.edgetype}{datastring}]->(b)
RETURN type(r)"""
        return query


class Node:
    def __init__(self, nodetype, data={}):
        self.nodetype = nodetype
        self.data = data

    def __repr__(self):
        rep = f"Node(nodetype='{self.nodetype}', data={self.data.__repr__()})"
        return rep

    def set_type(self, nodetype):
        self.nodetype = nodetype

    def update(self, data):
        self.data.update(data)

    def create_stmt(self):
        if not self.data:
            datastring = ""
        else:
            labels = ["{label}:${label}".format(label=label) for label in self.data]
            datastring = " {%s}" % ", ".join(labels)
        query = f"MERGE (a:{self.nodetype} {datastring})"
        return query
