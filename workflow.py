from database import RecordPointer, get_database, Pony

from flytekit import task, workflow

from stages.article_parser import parse_file
from stages.abbreviation_finder import find_abbreviations
from typing import List, Dict

from stages.pipeline_step import PipelineStep

@task
def find_abbreviation_task(articles: List[Dict[str, str]]) -> List[Dict]:
    db = get_database()
    af = PipelineStep(fn=find_abbreviations, db=db, downstream="abbreviations")
   # doi = article["doi"]
    ids = [abbr for abbr in af.run_all(articles, write=True)]
    return ids

@task
def load_file_task(source_tables: Dict[str, int]) -> List[dict]:

    db = get_database()
    article_ptr = RecordPointer(db=db, refs=source_tables)
    table_name = article_ptr.tables[0]
    article = article_ptr.get()
    doi = article[table_name].doi
    article_id = article[table_name].id
    article = parse_file(path="/Users/andreashelfenstein/Documents/Work/redcurrant/sciserve.nosync/data/articles", 
    filename="PMC3584426.nxml", lookup={"PMC3584426": "fake.doi"})
    # TODO: Write record to db
    art = article[0]
    art["id"] = article_id
    return [art]

@workflow
def wf(idx: int = 1398855) -> List[Dict[str, str]]:
    article_source = {"articles": idx}
    articles = load_file_task(source_tables=article_source)
    abbrevs = find_abbreviation_task(articles=articles)
    return abbrevs

# pyflyte run workflow.py:wf --idx 1398855