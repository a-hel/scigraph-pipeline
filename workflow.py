from database import RecordPointer, get_database, Pony

from flytekit import task, workflow

from stages.article_parser import parse_file
from typing import List, Dict


@task
def load_file(source_tables: Dict[str, int]) -> dict:

    db = get_database()
    article_ptr = RecordPointer(db=db, refs=source_tables)
    table_name = article_ptr.tables[0]
    article = article_ptr.get()
    doi = article[table_name].doi
    article = parse_file(
        path="/Users/andreashelfenstein/Documents/Work/redcurrant/sciserve.nosync/data/articles",
        filename="PMC3584426.nxml",
        lookup={"PMC3584426": "fake.doi"},
    )
    # TODO: Write record to db
    return article[0]


@workflow
def wf(idx: int = 1398855) -> dict:
    article_source = {"articles": idx}
    article = load_file(source_tables=article_source)
    return article


# pyflyte run workflow.py:wf --idx 1398855
