import pony

from dotenv import load_dotenv
from stages import extract_ner
import database


load_dotenv()


@pony.orm.db_session
def sentence_generator(db):
    elems = pony.orm.select(c for c in db.summaries)
    for elem in elems:
        yield {"id": elem.id, "conclusion": elem.conclusion}


if __name__ == "__main__":
    db = database.get_database()
    extractor = extract_ner.recognize_named_entities(
        sentence_generator(db), parser="local"
    )
    db.add_named_entity(extractor)
