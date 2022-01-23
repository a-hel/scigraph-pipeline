"""

"""

from itertools import product
import spacy

from .base import PipelineStep
from .spacy_pipeline import claucy, information_extractor  # noqa


class TripleExtractor(PipelineStep):
    upstream = "summaries"
    downstream = {"nodes": "nodes", "edges": "edges"}
    nlp = spacy.load("en_core_web_trf")
    nlp.add_pipe("claucy")
    nlp.add_pipe("InformationExtractor", after="claucy")

    def _extract_triples(self, records):
        for record in records:
            data = {"id": record.id, "ne": record.named_entities}
            texts = record.simple_conclusions.conclusion
            # if texts.is_empty():
            #    continue
            for text in texts:
                doc = self.nlp(text)
                for clause, triple in zip(doc._.clauses, doc._.triples):
                    if triple is None:
                        continue
                    elem = {"clause": clause, "triple": triple}
                    elem.update(**data)
                    yield elem

    def _match_terms(self, records, name="conclusions"):
        for elem in self._extract_triples(records):
            ne = elem["ne"]
            summary_id = elem["id"]
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

    def _run_once(self, record):
        raise NotImplementedError("")

    def _run(self, records):
        yield from self._match_terms(records)
