import logging
import json

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class DbDriver:
    def __init__(self, **config):
        self.username = config.get("username")
        self.dbname = config.get("dbname")
        self.host = config.get("host")
        self.port = config.get("port")
        self.uri = "bolt://%s:%s/%s" % (self.host, self.port, self.dbname)
        self.uri = "bolt://%s:%s" % (self.host, self.port)
        logger.debug("Connecting to %s" % self.uri)
        self.driver = GraphDatabase.driver(
            self.uri, auth=(self.username, config.get("password")), encrypted=True
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

    @staticmethod
    def from_config(path="../config/dev.json", key="neo4j"):
        with open(path, "r") as f:
            config = json.load(f)
        if key is not None:
            config = config[key]
            logger.debug(f"Loaded Neo4jconfig section {key} form file {path}.")
        else:
            logger.debug(f"Loaded Neo4jconfig form file {path}.")
        return DbDriver(**config)
