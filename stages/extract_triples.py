
"""

"""

import spacy

from spacy_pipeline import claucy, information_extractor




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
        for clause in doc._.clauses:
            data["clause"] = clause
            yield data
