        
import spacy
from spacy.tokens import Doc

class SentenceElement:
    def __init__(self, tok):
        self.tok = tok


class Verb:
    def __init__(self, tok):
        self.tok = tok
        self.predicate = self._to_predicate(tok)
        
    def _to_predicate(self, verb):
        yield verb
        for right in verb.rights:
            yield from right.lefts
            yield right

@spacy.Language.factory("InformationExtractor7")
class InformationExtractor:
    
    def __init__(self, nlp, name = "InformationExtractor"):
        Doc.set_extension("triples", default=[], force=True)
        #.set_extension("triples")
    
    def extract_triples(self, clauses):
        for clause in clauses:
            obj = SentenceElement(clause.direct_object or clause.indirect_object)
            subj = SentenceElement(clause.subject)
            verb = Verb(clause.verb)
            yield {"s": subj, "v": verb, "o": obj}
    
    def __call__(self, doc):
        if not hasattr(doc._, 'clauses'):
            raise AttributeError('This pipeline step must be run after claucy.')
        doc._.triples = [triple for triple in self.extract_triples(doc._.clauses)]
        return doc