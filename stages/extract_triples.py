
"""

"""

from numpy import record
import spacy

from .spacy_pipeline import claucy, information_extractor




def extract_triples(records, name="conclusions"):
    nlp = spacy.load("en_core_web_trf")
    nlp.add_pipe('claucy')
    nlp.add_pipe("InformationExtractor", after="claucy")
    for record in records:
        data = {"id": record.id,
        "ne": record.named_entities}
        text = record.conclusion
        if text is None:
            continue
        doc = nlp(text)
        for clause, triple in zip(doc._.clauses, doc._.triples):
            elem = {"clause": clause, "triple": triple}
            elem.update(**data)
            yield elem

def match_terms(records, name="conclusions"):
    for elem in extract_triples(records, name=name):
        ne = elem["ne"]
        summary_id = elem["summary"].id
        if not ne:
            continue
        matches = elem["triple"].match(ne)
        if matches is None:
            continue
        match_left, verb, match_right = matches
        node_left = {"summary_id": summary_id, "cui": match_left.cui, "matched": match_left.matched, "preferred": match_left.preferred}
        node_right = {"summary_id": summary_id, "cui": match_right.cui, "matched": match_right.matched, "preferred": match_right.preferred}
        edge = {"summary_id": summary_id, "predicate": verb.predicate, "cui_left": match_left.cui, "cui_right": match_right.cui }
        yield {"nodes": (node_left, node_right), "edges": (edge)}
