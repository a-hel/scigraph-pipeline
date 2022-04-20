from .stages.article_parser import parse_file


def load_article(article_id, doi, uri):
    """
    Read full text of article that is referenced in the database.
    Currently only works with files.
    """
    parsed_text = parse_file(uri)

    return {"article_id": article_id, "doi": doi, "text": parsed_text}
