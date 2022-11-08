import re
from datetime import datetime
from typing import List, Generator, Dict

from utils.logging import PipelineLogger
from custom_types import Records, RawRecords

logger = PipelineLogger("SubstituteTask")


def _substitute_word(sentence: str, abbreviation: str) -> str:
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
        logger.debug(
            f"Unable to substitute '{abbreviation.abbreviation}' in '{sentence}'."
        )
        return sentence
    except TypeError as e:
        logger.error(
            f"""Error during substitution:
        Abbreviation: \t {abbreviation.abbreviation}
        Meaning: \t\t {abbreviation.meaning}
        Sentence: \t {sentence}"""
        )
        raise
    return substituted


def substitute(sentence: str, abbrevs: List[str]) -> str:
    for abbreviation in abbrevs:
        sentence = _substitute_word(sentence, abbreviation)
    return sentence


def substitute_abbreviations(
    simple_conclusions: Records,
) -> RawRecords:
    for simple_conclusion in simple_conclusions:
        abbrevs = simple_conclusion.summary_id.abbreviations
        abbrevs = list(abbrevs)
        sentence = simple_conclusion.conclusion
        substituted_sentence = substitute(sentence, abbrevs)
        yield {
            "simple_conclusion_id": simple_conclusion.id,
            "summary_id": simple_conclusion.summary_id.id,
            "conclusion": substituted_sentence,
            "date_added": datetime.now(),
        }
