import os
from numba import jit

from dotenv import load_dotenv

from pipeline_step import PipelineStep
import database

from stages import parse_articles, summarize_articles

load_dotenv()
db = database.get_database()

folder = "../data/articles"
step1 = PipelineStep(fn=parse_articles(folder), db=db)
step2 = PipelineStep(fn=summarize_articles(), db=db, downstream=db.summaries)

output1 = step1.run_once(data=(None for _ in range(10)), write=False)
output2 = step2.run_all(data=[output1], write=False)

for op in output2:
    print(op)
