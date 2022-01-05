
import csv
import json
from datetime import date, datetime
from pony.orm import Database, Required, Optional, commit, PrimaryKey, Set, db_session, select

db = Database()

class Article(db.Entity):
    _table_ = ('SciGraphPipeline', 'articles')
    id = PrimaryKey(int, auto=True)
    doi = Required(str, index=True)
    uri = Required(str)
    summaries = Set("Summary", reverse="article_id")

class Summary(db.Entity):
    _table_ = ('SciGraphPipeline', 'summaries')
    id = PrimaryKey(int, auto=True)
    article_id = Required(Article, reverse="summaries")
    summary = Required(str)
    conclusion = Required(str)
    date_added = Required(datetime)
    scitldr_version = Required(str)
    error = Optional(str)
    named_entities = Set("NamedEntity", reverse="summary_id")
    triples = Set("Triple", reverse="summary_id")

class NamedEntity(db.Entity):
    _table_ = ('SciGraphPipeline', 'named_entities')
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse='named_entities')
    matched_term = Required(str)
    preferred_term = Required(str)
    cui = Required(str)
    metamap_version = Required(str)
    objects = Set('Triple', reverse='object_id')
    subjects = Set('Triple', reverse='subject_id')

class Triple(db.Entity):
    _table_ = ('SciGraphPipeline', 'triples')
    id = PrimaryKey(int, auto=True)
    summary_id = Required(Summary, reverse='triples')
    subject_id = Required(NamedEntity, reverse='subjects')
    predicate = Required(str)
    object_id = Required(NamedEntity, reverse='objects')
    svo_version = Required(str)

class Log(db.Entity):
    _table_ = ('SciGraphPipeline', 'log')
    id = PrimaryKey(int, auto=True)
    table = Required(str)
    last_processed = Required(int)
    timestamp = Required(datetime)



class Pony:
    def __init__(self, db, host="localhost", user="postgres", port=5432, password=None, database="SciGraph_staging"):
        self.db = db
        self.db.bind(provider='postgres', user=user, password=password, host=host, database=database)
        self.db.generate_mapping(create_tables=True)
        self.articles = Article
        self.summaries = Summary
        self.triples = Triple
        self.named_entities = NamedEntity
        self.logs = Log

    def _commit(self, table, last_record):
        commit()
        checkpoint = {"table": ".".join(table._table_),
        "last_processed": last_record.id,
        "timestamp": datetime.now()}
        self.logs(**checkpoint)
        commit()

    @db_session()
    def _add_record(self, data, table, periodic_commit=50):
        for e, elem in enumerate(data):
            last_record = table(**elem)
            if not e % periodic_commit:
                print("Processing %s: %s" % (e, last_record))
                self._commit(table, last_record)
        try:
            self._commit(table, last_record)
        except UnboundLocalError:
            return None
        return last_record.id

    #@db_session()
    def _get_record(self, table):
        elems = select(c for c in table if not c.named_entities.id)
        yield from elems

    def get_conclusion(self):
        elems = select(c for c in self.summaries if c.named_entities.id)
        
        #elems = select((c.id, c.conclusion, c.named_entities.matched_term, c.named_entities.preferred_term) for c in self.summaries if c.named_entities.id)
        yield from elems

    
    def add_record(self, data, table, periodic_commit=50):
        return self._add_record(data, table, periodic_commit)

    def add_articles(self, data):
        last_id = self._add_record(data, self.articles, periodic_commit=1000)
        return {"article_id": last_id}

    def add_summary(self, data):
        last_id = self._add_record(data, self.summaries, periodic_commit=100)
        return {"summary_id": last_id}

    def add_named_entity(self, data):
        last_id = self._add_record(data, self.named_entities, periodic_commit=100)
        return {"named_entity_id": last_id}
        


def get_database(config="config.json"):
    with open(config, "r") as f:
        cfg = json.load(f)
    postgres_cfg = cfg.get('postgres')
    postgres_cfg = {key: postgres_cfg.get(key) for key in ['host', 'user', 'port', 'password']}
    pony = Pony(db, **postgres_cfg)
    return pony

def _read_file(fname):
    with open(fname) as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader, None)
        for doi, summary, conclusion in reader:
            data = {"doi": doi, "summary": summary, "conclusion": conclusion, "date_added": datetime.now(), "scitldr_version": "0.0.0"}
            yield data

def files_to_db(fnames):
    db = get_database()
    for fname in fnames:
        reader = _read_file(fname)
    db.add_summary(reader)



