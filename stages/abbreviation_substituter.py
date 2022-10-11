import re
from itertools import groupby
from datetime import datetime


def _substitute_word(sentence, abbreviation):
    try:
        substituted = re.sub(
            r"\b%ss?\b" % re.escape(abbreviation.abbreviation),
            abbreviation.meaning,
            sentence,
        )
        substituted = re.sub(
            r"\b%ss?\b" % re.escape(abbreviation.abbreviation.rstrip("s")),
            abbreviation.meaning,
            substituted,
        )
    except re.error as e:
        print(e)
        return sentence
    except TypeError as e:
        print(abbreviation.abbreviation)
        print(abbreviation.meaning)
        print(sentence)
        raise TypeError(e)
    return substituted


def substitute(sentence, abbrevs):
    for abbreviation in abbrevs:  # sentence.article_id.abbreviations:
        sentence = _substitute_word(sentence, abbreviation)
    return sentence


def substitute_abbreviations(simple_conclusions, exclude=[1398855]):
    for simple_conclusion in simple_conclusions:
        abbrevs = simple_conclusion.summary_id.abbreviations
        abbrevs = list(abbrevs)
        # if len(abbrevs) == 0:
        #     substituted_sentence
        # article_id = abbrevs[0].article_id.id
        # if article_id in exclude:
        #     continue
        sentence = simple_conclusion.conclusion
        substituted_sentence = substitute(sentence, abbrevs)
        yield {
            # "article_id": article_id,
            "simple_conclusion_id": simple_conclusion.id,
            "summary_id": simple_conclusion.summary_id.id,
            "conclusion": substituted_sentence,
            "date_added": datetime.now(),
        }
