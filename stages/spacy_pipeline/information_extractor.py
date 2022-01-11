import warnings
import itertools

import spacy
from spacy.tokens import Doc


class Triple:
    def __init__(self, subject, verb, object_):
        self.subject = subject
        self.object_ = object_
        self.verb = verb
        self.subject_nes = None
        self.object_nes = None
        self.predicate = None
        self._is_matched = False

    def __repr__(self):
        ret_val = f"{self.subject}\n  -> {self.verb}\n     -> {self.object_}"
        return ret_val

    def __bool__(self):
        return not self._is_matched or (self.subject_nes and self.object_nes)

    def _to_predicate(self, verb):
        yield from verb
        for right in verb.rights:
            yield from right.lefts
            yield right

    def prepare_predicate(self, verb, cutoff=None):
        for tok in self._to_predicate(verb):
            if tok.idx >= cutoff or tok.pos_ in ["dobj", "dative"]:
                break
            yield tok

    def _find_ne(self, span, ne):
        matched = ne.matched_term
        try:
            start = span.doc.text.casefold().index(matched.casefold())
        except ValueError:
            return False
        stop = start + len(matched)
        for tok in span:
            tok_idx = tok.idx
            if tok_idx > stop:
                break
            if start <= tok.idx and tok.idx + len(tok) <= stop:
                ne.idx = start
                yield ne


    def match(self, nes):
        self._is_matched = True
        self.subject_nes = list(filter(None, [list(self._find_ne(self.subject, ne)) for ne in nes]))
        self.object_nes = list(filter(None, [list(self._find_ne(self.object_, ne)) for ne in nes]))
        if not self.object_nes:
            return
        first_obj = min(tok.idx for tok in itertools.chain(*self.object_nes))
        self.predicate = self.prepare_predicate(self.verb, cutoff = first_obj)

    def nodes(self):
        pass

    def edges(self):
        pass
        

    @staticmethod
    def from_svo(clause):
        obj = clause.direct_object or clause.indirect_object
        subj = clause.subject
        verb = clause.verb
        return(Triple(subject=subj, verb=verb, object_=obj))

    @staticmethod
    def from_svc(clause):
        subj = clause.subject
        verb = clause.verb
        obj = clause.complement
        return(Triple(subject=subj, verb=verb, object_=obj))

        
@spacy.Language.factory("InformationExtractor")
class InformationExtractor:
    
    def __init__(self, nlp, name = "InformationExtractor"):
        Doc.set_extension("triples", default=[], force=True)
        #.set_extension("triples")
    
    def extract_triples(self, clauses):
        dispatch = {"SVO": Triple.from_svo,
        "SVC": Triple.from_svc}
        for clause in clauses:
            if clause.type in dispatch.keys():
                func = dispatch[clause.type]
                yield func(clause)
            else:
                yield None
    
    def __call__(self, doc):
        if not hasattr(doc._, 'clauses'):
            raise AttributeError('This pipeline step must be run after claucy.')
        if doc._.clauses is None:
            doc._.triples = None
            return doc
        doc._.triples = [triple for triple in self.extract_triples(doc._.clauses)]
        return doc