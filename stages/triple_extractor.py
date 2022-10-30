from itertools import product
from logging import Logger
import spacy

from .spacy_pipeline import claucy, information_extractor  # noqa

logger = Logger(__name__)


def _extract_triples(records, nlp):
    for record in records:
        data = {
            "id": record.simple_substituted_conclusions.id,
            "summary_id": record.id,
            "ne": record.simple_substituted_conclusions.named_entities,
        }
        texts = record.simple_substituted_conclusions.conclusion
        # if texts.is_empty():
        #    continue
        for text in texts:
            break
        doc = nlp(text)
        for clause, triple in zip(doc._.clauses, doc._.triples):
            if triple is None:
                continue
            elem = {"clause": clause, "triple": triple}
            elem.update(**data)
            yield elem


def _match_terms(records, nlp, name="conclusions"):
    for elem in _extract_triples(records, nlp):
        ne = elem["ne"]
        summary_id = elem["summary_id"]
        if not ne:
            continue
        matches = elem["triple"].match(ne)
        if matches is None:
            continue
        matches_left, verb, matches_right = matches
        if not all([matches_left, verb, matches_right]):
            continue
        nodes_left = [
            {
                "summary_id": summary_id,
                "cui": match_left.cui,
                "matched": match_left.matched_term,
                "preferred": match_left.preferred_term,
            }
            for match_left in matches_left
        ]
        nodes_right = [
            {
                "summary_id": summary_id,
                "cui": match_right.cui,
                "matched": match_right.matched_term,
                "preferred": match_right.preferred_term,
            }
            for match_right in matches_right
        ]
        edges = (
            {
                "summary_id": summary_id,
                "predicate": " ".join(verb),
                "cui_left": match_left.cui,
                "cui_right": match_right.cui,
            }
            for match_left, match_right in product(matches_left, matches_right)
        )

        yield {"nodes": nodes_left + nodes_right, "edges": edges}


def extract_triples(summaries):
    nlp = spacy.load("en_core_web_trf")
    nlp.add_pipe("claucy")
    nlp.add_pipe("InformationExtractor", after="claucy")
    logger.debug("NLP loaded")
    yield from _match_terms(summaries, nlp)
