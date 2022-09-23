from database import get_database, Pony
from dotenv import load_dotenv

from tqdm import tqdm
from flytekit import task, workflow

from stages.article_parser import load_article
from stages.abbreviation_finder import find_abbreviations
from stages.extract_ner import recognize_named_entities
from stages.abbreviation_substituter import substitute_abbreviations
from stages.triple_extractor import extract_triples
from typing import List, Dict

from stages.pipeline_step import PipelineStep

load_dotenv()


@task
def find_abbreviation_task(articles: List[Dict[str, str]]) -> List[Dict]:
    db = get_database()
    af = PipelineStep(fn=find_abbreviations, db=db, downstream="abbreviations")
    ids = [abbr for abbr in af.run_all(articles, write=True)]
    return ids


@task
def load_file_task(article_id: int) -> List[dict]:

    db = get_database()
    ap = PipelineStep(fn=load_article, db=db, upstream="articles")
    article = ap.run_once(iter(range(article_id, article_id + 1)), write=False)
    # TODO: Write record to db
    art = article.__next__()
    return [art]


@task
def temptask(article_id: int) -> None:
    db = get_database()
    ap = PipelineStep(fn=load_article, db=db, upstream="articles")
    af = PipelineStep(fn=find_abbreviations, db=db, downstream="abbreviations")
    articles = ap.run_once(iter(range(article_id, article_id + 1)), write=False)

    ids = [abbr for abbr in af.run_all(articles, write=True)]
    return None


@task
def ner_task() -> None:
    db = get_database()
    af = PipelineStep(
        fn=recognize_named_entities,
        db=db,
        upstream="simple_substituted_conclusions",
        downstream="named_entities",
    )
    for e, elem in enumerate(af.run_all(data=1, write=True)):
        if not e % 10000:
            print("Processing entry %s" % e)

@task
def substitute_abbreviation_task() -> None:
    db = get_database()
    sa = PipelineStep(
        fn=substitute_abbreviations,
        db=db,
        upstream="abbreviations",
        downstream="simple_substituted_conclusions"
    )
    for e, elem in tqdm(enumerate(sa.run_all(data=1, write=True, order_by="summary_id"))):
        pass

@task
def extract_triples_task() -> None:
    db = get_database()
    sa = PipelineStep(
        fn=extract_triples,
        db=db,
        upstream="simple_substituted_conclusions",
        downstream=["nodes", "edges"]
    )
    for e, elem in enumerate(sa.run_all(data=1, write=True)):
        if not e % 10000:
            print("processing entry %s" % e)
        pass

@workflow
def wf(idx: int = 1398855) -> None:
    # article_source = {"articles": idx}
    # articles = load_file_task(article_id=idx)
    # abbrevs = find_abbreviation_task(articles=articles)
    ners = extract_triples_task()
    #subs = substitute_abbreviation_task()
    return None

# Next: substitute abbrevs in simple conclusions
# Then: extract ner from that

# pyflyte run workflow.py:wf --idx 1398855

#SELECT pg_reload_conf()