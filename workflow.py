from database import RecordPointer, get_database, Pony

from flytekit import task, workflow

from stages.article_parser import load_article
from stages.abbreviation_finder import find_abbreviations
from typing import List, Dict

from stages.pipeline_step import PipelineStep

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
    article = ap.run_once(iter(range(article_id, article_id+1)), write=False)
    # TODO: Write record to db
    art = article.__next__()
    return [art]

@workflow
def wf(idx: int = 1398855) -> List[Dict[str, str]]:
    article_source = {"articles": idx}
    articles = load_file_task(article_id=idx)
    abbrevs = find_abbreviation_task(articles=articles)
    return abbrevs

# pyflyte run workflow.py:wf --idx 1398855