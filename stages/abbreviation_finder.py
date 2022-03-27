from abbreviations import schwartz_hearst

from .base import PipelineStep
from .utils import load_article

class AbbreviationFinder(PipelineStep):
    upstream = "article"
    downstream = "abbreviations"

    def _run_once(self, article):
        text = load_article(article.uri)
        introduction = text["Introduction"]
        pairs = schwartz_hearst.extract_abbreviation_definition_pairs(doc_text=introduction)
        for abbr, meaning in pairs.items():
            yield {"article_id": article.id, "doi": article.doi, "abbreviation": abbr, "meaning": meaning}

    def _run(self, data):
        for article in data:
            yield from article