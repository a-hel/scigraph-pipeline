from abbreviations import schwartz_hearst

from .pipeline_step import PipelineStep
#from .utils import load_article


class AbbreviationFinder(PipelineStep):
    upstream = "article"
    downstream = "abbreviations"


    def _run_once(self, article):
        #text = load_article(article.uri)
        introduction = article["Introduction"]
        introduction = introduction.replace("\n", " ")
        pairs = schwartz_hearst.extract_abbreviation_definition_pairs(
            doc_text=introduction, first_definition=True
        )
        for abbr, meaning in pairs.items():
            yield {
                "article_id": article["id"],
                "doi": article["doi"],
                "abbreviation": abbr,
                "meaning": meaning,
            }

    def _run(self, data):
        for article in data:
            yield from self._run_once(article)

def find_abbreviations(articles):
    for article in articles:
        introduction = article["Introduction"]
        introduction = introduction.replace("\n", " ")
        pairs = schwartz_hearst.extract_abbreviation_definition_pairs(
            doc_text=introduction, first_definition=True
        )
        for abbr, meaning in pairs.items():
            yield {
                "article_id": int(article["id"]),
                "abbreviation": abbr,
                "meaning": meaning,
            }