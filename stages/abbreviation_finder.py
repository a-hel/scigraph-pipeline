from abbreviations import schwartz_hearst

from typing import List, Dict, Generator, Union


def find_abbreviations(
    articles: List[Dict[str, str]]
) -> Generator[None, Dict[str, Union[str, int]], None]:
    for article in articles:
        introduction = article["Introduction"]
        introduction = introduction.replace("\n", " ")
        pairs = schwartz_hearst.extract_abbreviation_definition_pairs(
            doc_text=introduction, first_definition=True
        )
        for abbr, meaning in pairs.items():

            yield {
                "article_id": int(article.get("id", 0)),
                "doi": article["doi"],
                "abbreviation": abbr,
                "meaning": meaning,
            }
