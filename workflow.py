"""
    Main Flyte workflow, from reading plain-text articles
    to uploading them to the cloud.

    Currently only runs locally, rework is needed to run in flyte
    cluster.

    Run with
    ```
    pyflyte run workflow.py:wf --mode ALL|NEW|FRESH|ONE --write True|False
    ```

    2022 Andreas Helfenstein
"""

import os
from typing import List, Dict

from dotenv import load_dotenv
from tqdm import tqdm
from flytekit import task, workflow
from flytekit.extras.tasks.shell import OutputLocation, ShellTask

from connectors.postgres import Database
from connectors.neo4j import GraphDB
from pipeline import PipelineStep

from stages.article_parser import load_article

# from stages.summarizer import summarize_articles
from stages.abbreviation_finder import find_abbreviations
from stages.sentence_simplyfier import simplify_sentences

from stages.abbreviation_substituter import substitute_abbreviations

from stages.triple_extractor import extract_triples
from stages.graph_preparer import stage_nodes, stage_edges
from stages.graph_writer import GraphWriter

# from tests.test_database import run_tests

from utils.logging import PipelineLogger

logger = PipelineLogger("Workflow")

load_dotenv()


@task
def summarize_article_task(mode: str = "NEWER", write: bool = False) -> List[dict]:

    db = Database.from_config(path=os.getenv("CONFIG_PATH"))
    ap = PipelineStep(
        fn=summarize_articles,
        db=db,
        upstream="articles",
        downstream="summaries",
        name="Summarize",
    )
    articles = ap.run_all(mode=mode, write=write)
    for article in articles:
        pass


@task
def find_abbreviation_task(mode: str = "NEWER", write: bool = False) -> List[Dict]:
    db = Database.from_config(path=os.getenv("CONFIG_PATH"))
    af = PipelineStep(
        fn=find_abbreviations,
        db=db,
        upstream="articles",
        downstream="abbreviations",
        name="Abbreviations",
    )
    abbrevs = af.run_all(mode=mode, write=write)
    for abbrev in abbrevs:
        pass


@task
def simplify_conclusions_task(mode: str = "NEWER", write: bool = False) -> List[Dict]:
    db = Database.from_config(path=os.getenv("CONFIG_PATH"))
    sf = PipelineStep(
        fn=simplify_sentences,
        db=db,
        upstream="summaries",
        downstream="simple_conclusions",
        name="Simplify",
    )
    simple_conclusions = sf.run_all(mode=mode, write=write)
    for simple_conclusion in simple_conclusions:
        pass


@task
def substitute_abbreviation_task(mode: str = "NEWER", write: bool = False) -> None:
    db = Database.from_config(path=os.getenv("CONFIG_PATH"))
    sa = PipelineStep(
        fn=substitute_abbreviations,
        db=db,
        upstream="simple_conclusions",
        downstream="simple_substituted_conclusions",
    )
    simple_substituted_conclusions = sa.run_all(mode=mode, write=write)
    for simple_substituted_conclusion in simple_substituted_conclusions:
        pass


@task
def extract_triples_task(mode: str = "NEWER", write: bool = False) -> None:
    db = Database.from_config(path=os.getenv("CONFIG_PATH"))
    tr = PipelineStep(
        fn=extract_triples,
        db=db,
        upstream="simple_substituted_conclusions",
        downstream=["nodes", "edges"],
    )
    triples = tr.run_all(mode=mode, write=write)
    for triple in triples:
        pass


@task
def add_to_staging_task(mode: str = "NEWER", write: bool = False) -> None:
    db = Database.from_config(path=os.getenv("CONFIG_PATH"))
    st = PipelineStep(
        fn=stage_nodes,
        db=db,
        upstream="nodes",
        downstream=["concept_nodes", "synonym_nodes", "synonym_edges"],
    )
    elems = st.run_all(mode=mode, write=write, order_by="preferred")
    for elem in elems:
        pass
    # st = PipelineStep(
    #     fn=stage_edges, db=db, upstream="edges", downstream="predicate_edges"
    # )
    elems = st.run_all(mode=mode, write=write)
    for elem in elems:
        pass


@task
def export_to_graph_task(mode: str = "NEWER", write: bool = False) -> None:
    db = Database.from_config(path=os.getenv("CONFIG_PATH"))
    graph_db = GraphDB.from_config(path=os.getenv("CONFIG_PATH"), key="neo4j_staging")
    graph_writer = GraphWriter(db=db, graph_db=graph_db)
    graph_writer.add_nodes(write=write)
    graph_writer.add_edges(write=write)


verify_graph = ShellTask(
    name="pytest_run",
    debug=True,
    script="""
    export STANFORDNLP_TEST_HOME=stanfordnlp_test
    python -m pytest --rootdir=. tests
    """,
)


@task
def migrate_graph(mode: str = "NEWER", drop: bool = False) -> None:
    source_db = GraphDB.from_config(path=os.getenv("CONFIG_PATH"), key="neo4j_staging")
    target_db = GraphDB.from_config(
        path=os.getenv("CONFIG_PATH"), key="neo4j_production"
    )


@workflow
def wf(mode: str = "FRESH", write: bool = False) -> None:
    logger.setLevel("DEBUG")
    logger.info(f"Start workflow with mode {mode} (write={write}).")
    # summary = summarize_article_task(mode=mode, write=write)
    # abbrevs = find_abbreviation_task(mode=mode, write=write)
    # simple_conclusions = simplify_conclusions_task(mode=mode, write=write)
    # subs = substitute_abbreviation_task(mode=mode, write=write)
    triples = extract_triples_task(mode=mode, write=write)
    # staged = add_to_staging_task(mode=mode, write=write)
    # graph = export_to_graph_task(mode=mode, write=write)
    # test_results = verify_graph()

    return None


# pyflyte run workflow.py:wf --mode FRESH --write False
