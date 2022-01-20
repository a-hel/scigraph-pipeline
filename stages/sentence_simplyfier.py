from datetime import datetime
import itertools
import sys
sys.path.append("stages/spacy_pipeline/muss")

import muss
from muss.simplify import Simplifier

from .base import PipelineStep
from .utils import batched

class SentenceSimplyfier(PipelineStep):
    muss_version = "1.0"#muss.__version__
    upstream = "summaries"
    downstream = "simple_conclusions"
    model_name = 'muss_en_mined'
    model = Simplifier(model_name)

    def _simplify(self, sents):
        yield from self.model.run(sents)

    def _run_once(self, data):
        return self._simplify(sents=[data])
        
    def _run(self, data):
        for batch in batched(data, 500):
            b1, b2 = itertools.tee(batch, 2)
            conclusions = (b.conclusion for b in b1)
            for sent, elem in zip(self._simplify(sents=conclusions), b2):
                print(sent)
                if sent.casefold().startswith("conclusions".casefold()):
                    sent = sent[11:]
                elif sent.casefold().startswith("conclusion".casefold()):
                    sent = sent[10:]
                data = {"summary_id": elem.id,
                "muss_version": self.muss_version,
                "conclusion": sent,
                "date_added": datetime.now()}
                yield data


def main(db):
    upstream = db.summaries
    downstream = db.simple_conclusions
    data = db.get_records(table=upstream, downstream=downstream)
    step = SentenceSimplyfier()
    for e, elem in enumerate(step.run(data)):
        if not e % 100:
            print("Processing entry no %s." % e)
        db.add_record([elem], table=downstream)
