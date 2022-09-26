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
        print(sentence)
        print(abbreviation)
        sentence = _substitute_word(sentence, abbreviation)
    return sentence


def substitute_abbreviations(abbreviations, exclude=[1398855]):
    for summary_id, abbrevs in groupby(abbreviations, key=lambda x: x.summary_id):
        abbrevs = list(abbrevs)
        article_id = abbrevs[0].article_id.id
        if article_id in exclude:
            continue
        sentences = summary_id.simple_conclusions.conclusion
        for simple_conclusion_id in summary_id.simple_conclusions.id:
            break
        for sentence in sentences:
            substituted_sentence = substitute(sentence, abbrevs)
            yield {
                "id": article_id,
                "simple_conclusion_id": simple_conclusion_id,
                "summary_id": summary_id,
                "conclusion": substituted_sentence,
                "date_added": datetime.now(),
            }
