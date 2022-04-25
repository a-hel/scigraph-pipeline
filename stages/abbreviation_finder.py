from abbreviations import schwartz_hearst


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