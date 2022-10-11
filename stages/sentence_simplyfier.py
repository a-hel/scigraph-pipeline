from datetime import datetime
import itertools
import sys

sys.path.append("stages/spacy_pipeline/muss")

import muss
from muss.simplify import Simplifier

from .utils import batched
from utils.logging import PipelineLogger

logger = PipelineLogger("Simplify")

def remove_stopclauses(text, stop_clauses):
        for stopclause in stop_clauses:
            if text.casefold().startswith(stopclause.casefold()):
                text = text[len(stopclause) :]
            #text = text.remove_prefix(stopclause)
        return text.strip().capitalize()

def simplify(model, sents):
    yield from model.run(sents)

def run_model(data, model_name, batch_size=1000):
    muss_version = "1.0"  # muss.__version__
    total_processed = 0
    stop_clauses = [
        "conclusions",
        "conclusion",
        "in summary",
        "in conclusion",
        "our research shows that",
        "we demonstrated that",
        "we show that",
        "we found that",
        "in fact,",
        "in addition",]
    model = Simplifier(model_name)
    logger.info(f"MUSS model '{model_name}' loaded.")
    for batch in batched(data, batch_size):

        total_processed += batch_size
        if total_processed >= 8 * batch_size:
            model = Simplifier(model_name)
            logger.debug(f"MUSS model '{model_name}' reloaded.")
            total_processed = batch_size
        b1, b2 = itertools.tee(batch, 2)
        conclusions = (b.conclusion for b in b1)
        clean_conclusions = (remove_stopclauses(conclusion, stop_clauses) for conclusion in conclusions)
        for sent, elem in zip(simplify(model=model, sents=clean_conclusions), b2):
            data = {
                "summary_id": elem.id,
                "muss_version": muss_version,
                "conclusion": sent,
                "date_added": datetime.now()
            }
            yield data


def simplify_sentences(data):
    model_name = "muss_en_mined"
    yield from run_model(data, model_name=model_name)

