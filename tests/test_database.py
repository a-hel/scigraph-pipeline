import os
import pytest
from connectors.neo4j import GraphDB


@pytest.fixture
def graph_db():
    db = GraphDB.from_config(path="config/dev.json", key="neo4j_staging")
    return db


@pytest.fixture
def random_concept(graph_db):
    query = "MATCH (n:concept) RETURN n LIMIT 1"
    result = graph_db.query(query)
    concept = list(result.nodes)[0]
    return concept


@pytest.fixture
def random_synonym(graph_db):
    query = "MATCH (n:synonym) RETURN n LIMIT 1"
    result = graph_db.query(query)
    synonym = list(result.nodes)[0]
    return synonym


@pytest.fixture
def random_edge(graph_db):
    query = "MATCH p=()-[r:`_REL`]->() RETURN p LIMIT 1"
    result = graph_db.query(query)
    edge = list(result.relationships)[0]
    return edge


def test_get_concept(graph_db, random_concept):
    stmt = """MATCH (c1:synonym)-[:`_SYN`]->(c2:concept)
WHERE toLower(c1.name) STARTS WITH $concept
RETURN ID(c1) AS id, c1.name AS name, c2.cui AS cui LIMIT 5
UNION
MATCH (c1:concept)
WHERE toLower(c1.name) STARTS WITH $concept
RETURN ID(c1) AS id, c1.name AS name, c1.cui AS cui LIMIT 5"""
    concept = random_concept["name"]
    results = graph_db.query(stmt, concept=concept.lower(), out="list")
    assert len(results) > 0
    result = results[0]
    for key in ["id", "cui", "name"]:
        assert key in result.keys()
    assert isinstance(result["id"], int)
    assert isinstance(result["name"], str)
    assert isinstance(result["cui"], str)
    assert result["name"].casefold().startswith(concept.casefold())


def test_get_synonym(graph_db, random_synonym):
    concept = random_synonym["name"]
    stmt = """MATCH (c1:synonym)-[:`_SYN`]->(c2:concept)
WHERE toLower(c1.name) STARTS WITH $concept
RETURN ID(c1) AS id, c1.name AS name, c2.cui AS cui LIMIT 5
UNION
MATCH (c1:concept)
WHERE toLower(c1.name) STARTS WITH $concept
RETURN ID(c1) AS id, c1.name AS name, c1.cui AS cui LIMIT 5"""
    results = graph_db.query(stmt, concept=concept.lower(), out="list")
    for result in results:
        for key in ["id", "cui", "name"]:
            assert key in result.keys()
        assert isinstance(result["id"], int)
        assert isinstance(result["name"], str)
        assert isinstance(result["cui"], str)
        # assert concept.casefold() != result["name"].casefold()
        assert random_synonym["cui"].casefold() == result["cui"].casefold()


def test_neighbors_by_cui(graph_db, random_concept):
    cui = random_concept["cui"]
    stmt = "MATCH (c1:concept)-[r:`_REL`|`_VERB`]- (c2:concept {cui: $cui}) return c1, r, c2"
    results = graph_db.query(stmt, cui=cui, out="list")
    assert len(results) > 0


def test_get_edge_data(graph_db, random_edge):
    doi = random_edge["doi"]
    stmt = """MATCH () -[r:`_VERB` {doi: $doi}]-> () RETURN r"""
    result = graph_db.query(stmt, doi=doi)
    assert len(result.relationships) >= 1
    assert len(result.nodes) >= 2


def test_path_by_doi(graph_db, random_edge):
    maxlength = 3
    doi = random_edge["doi"]
    stmt = """MATCH p=(:concept)
-[:`_REL`*0..%s {doi:$doi}]-> 
(:concept)
-[r:`_VERB`*1{doi:$doi}]->(
:concept)
-[:`_REL`*0..%s {doi:$doi}]-> 
(:concept) 
RETURN RELATIONSHIPS(p) as edges, NODES(p) as nodes""" % (
        maxlength,
        maxlength,
    )
    result = graph_db.query(stmt, doi=doi)
    nodes = result.nodes
    edges = result.relationships
    assert len(nodes) >= 2
    assert len(edges) * 2 >= len(nodes)


def run_tests():
    res = pytest.main(["tests/test_database.py"])

    print(res)
    return res
