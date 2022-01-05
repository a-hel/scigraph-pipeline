import os
import csv
import itertools
import time
import json
import subprocess

import requests
import spacy
from dotenv import load_dotenv

from pipeline.graph import Node, Edge
from pipeline.graph import DbDriver
from pipeline import claucy

import logging
load_dotenv()


def _parse_online(text):
    time.sleep(.2)
    url = 'https://ii-public1.nlm.nih.gov/metamaplite/rest/annotate'
    headers = {'Accept': 'text/plain'}
    payload = {
        'inputtext': str(text),
        'docformat': 'freetext',
        'resultformat': 'json',
    }
    response = requests.post(url, payload, headers=headers)
    data = response.json()
    named_entities = {
        item['matchedtext']: [
            {
                'name': elem['conceptinfo']['preferredname'],
                'cui': elem['conceptinfo']['cui']
            } for elem in item['evlist']
        ]  #+ [{'conceptinfo': {'preferredname': item['matchedtext'].title(), 'cui': 0}}]
        for item in data
    }
    return named_entities


def _parse_locally(text):
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("'", "")# "\\'")
    metamap_path = os.getenv('METAMAP_PATH')
    cmd = 'echo "%s" | %s/bin/metamap --lexicon db -Z 2018AB -I --JSONn'#f 4'
    processed = subprocess.run(cmd % (text, metamap_path),
                               shell=True,
                               capture_output=True)
    if processed.returncode:
        raise ValueError("Metamap error: %s" % processed.stderr.decode('utf-8'))
    txt = processed.stdout.decode('utf-8')
    json_txt = txt[txt.index('{'):]
    if json_txt == '{"AllDocuments":[':
        print(text)
        raise IOError("Metamap error or server not running. Start with \n `./bin/skrmedpostctl start`\n`./bin/wsdserverctl start`")
    try:
        data = json.loads(json_txt)
    except json.JSONDecodeError:
        print(text)
        raise ValueError("Invalid metamap output: %s" % json_txt)
    except ValueError:
        raise ValueError("Could not parse metamap output: %s" % processed)
    phrases = data['AllDocuments'][0]['Document']['Utterances'][0]['Phrases']
    named_entities = {
        #item['Mappings'][0]['MappingCandidates'][0]['CandidateMatched'].removeprefix('*^'): [{
        item['Mappings'][0]['MappingCandidates'][0]['CandidateMatched'].lstrip('*^'): [{
            'name':
            elem['MappingCandidates'][0]['CandidatePreferred'],
            'cui':
            elem['MappingCandidates'][0]['CandidateCUI']
        } for elem in item['Mappings']]
        for item in phrases if item['Mappings']
    }
    return named_entities


def recognize_named_entities(text, parser='local'):
    """Extract MeSH terms from text."""
    parsers = {'local': _parse_locally, 'web': _parse_online}
    named_entities = parsers[parser](text)

    return named_entities


def sentence_generator(fname):
    with open(fname, 'r') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        for doi, summary, conclusion in reader:
            conclusion = conclusion.lstrip().strip()
            yield doi, summary, conclusion


def _find_obj_from_clause(span):
    obj = [o for o in span if o.dep_ == "pobj"]
    return obj


def _find_object(verb):
    _object = claucy._find_matching_child(verb.root, ["dative", "dobj"])
    if _object is not None:
        return _object, False
    pobj_clause = [
        claucy.extract_span_from_entity(c) for c in verb.root.children
        if c.dep_ in ("prep", "advmod", "agent")
    ]
    _object = itertools.chain(*[_find_obj_from_clause(o) for o in pobj_clause])
    return _object, True


def find_mesh(candidates, ner):
    if not isinstance(candidates, list):
        candidates = [candidates]
    for tok in candidates:
        if not tok:
            continue
        concept = get_concept(tok)
        entity = get_named_entity(concept, ner)
        if not entity:
            continue
        yield entity


def sentence_to_svos(sent, ner):
    verbs = claucy._get_verb_chunks(sent)
    for verb in verbs:
        yield extract_svo(verb, ner)


def extract_svo(verb, ner):
    svo = [None, None, None]
    _subject = claucy._get_subject(verb)
    _subject = list(find_mesh(_subject, ner))
    if not any(_subject):
        return None
    svo[0] = list(itertools.chain(*_subject))
    _objects, is_passive = _find_object(verb)
    _object = list(
        itertools.chain(*[(find_mesh(_object, ner)) for _object in _objects]))
    if not any(_object):
        return None
    svo[2] = list(itertools.chain(*_object))
    _relation = get_relation(verb)
    if _relation is None:
        return None
    svo[1] = _relation
    if is_passive:
        svo = list(reversed(svo))
    return svo


def get_relation(doc):
    # TODO: Only get verbs that relate to concepts ("we" not in lefts)
    relation = [[
        t for t in get_concept(tok) if t.pos_ in ['ADV', 'PUNCT', 'VERB']
    ] for tok in doc if tok.pos_ == 'VERB']
    if not relation:
        return None
    return relation[0]


def get_concept(tok):
    for left in tok.lefts:
        yield from get_concept(left)
    yield tok


def get_named_entity(tokens, rne):
    found_entities = []
    lookup_keys = {key.replace(' ', ''): key for key in rne}
    lookup_text = ''.join([tok.orth_ for tok in tokens]).replace(' ', '')
    for lookup_key, key in lookup_keys.items():
        if lookup_key in lookup_text:
            found_entities.append(rne[key])
    return found_entities


def synonym_structure(concept):
    nodes = [Node('synonym', synonym) for synonym in concept]
    nodes[0].set_type('concept')
    edges = [Edge(syn_node, nodes[0], '_SYN') for syn_node in nodes]
    edges.pop(0)
    return list(itertools.chain(nodes)), list(itertools.chain(edges))


def relation_structure(concepts, reverse=False):
    #if reverse:
    #    concepts = reversed(concepts)
    nodes, edges = zip(*[synonym_structure(concept) for concept in concepts])
    rel_edges = [
        Edge(nodes[i - 1][0], nodes[i][0], '_REL') for i in range(len(nodes))
    ]
    rel_edges.pop(0)
    return list(itertools.chain(nodes)), list(
        itertools.chain(edges + (rel_edges, )))


def knowledge_structure(subjects, relation, objects, doi, summary, conclusion):
    s_nodes, s_edges = relation_structure(subjects, reverse=True)
    o_nodes, o_edges = relation_structure(objects, reverse=False)
    r_edge = Edge(s_nodes[-1][0], o_nodes[0][0], '_VERB', {
        "name": relation,
        "doi": doi,
        'summary': summary,
        'conclusion': conclusion
    })
    return (list(itertools.chain(s_nodes + o_nodes)),
            list(itertools.chain(s_edges + o_edges + [[r_edge]])))


def svo_to_graph(db, svo, doi, summary, conclusion):
    subjects, relation, objects = svo
    relation = " ".join([rel.lemma_ for rel in relation])
    nodes, edges = knowledge_structure(subjects,
                                       relation,
                                       objects,
                                       doi=doi,
                                       summary=summary,
                                       conclusion=conclusion)
    for node in nodes:
        for n in node:
            db.add_node(n)
    for edge in edges:
        if not edge:
            continue
        for e in edge:
            if not e.is_synonym():
                e.update({'doi': doi})
            db.add_edge(e)


def main(input_, run_id):
    os.environ['TOKENIZERS_PARALLELISM'] = "false"
    error_dois = []
    db = DbDriver(username=os.getenv('DB_USER'),
                  password=os.getenv('DB_PASSWORD'),
                  dbname=os.getenv('DB_NAME'),
                  host=os.getenv('DB_HOST'),
                  port=os.getenv('DB_PORT'))
    nlp = spacy.load('en_core_web_trf')
    fname = os.path.join(input_, 'summaries_%s.csv' % run_id)
    sentences = sentence_generator(fname)
    for e, (doi, summary, conclusion) in enumerate(sentences):
        if e <= 38500:
            continue
        if not e % 50:
            print("%s - %s" % (e, conclusion))
        try:
            doc = nlp(conclusion)
            ner = recognize_named_entities(conclusion)
            for svo in sentence_to_svos(doc, ner):
                if svo is None:
                    continue
                svo_to_graph(db,
                             svo,
                             doi=doi,
                             summary=summary,
                             conclusion=conclusion)
        except Exception as err:
            error_dois.append(doi + ":" + str(err))
            print("Error processing line %s (doi: %s): %s" % (e, doi, err))
    with open("error_dois.txt", "w") as f:
        f.write("\n".join(error_dois))

def conclusion_to_triple(dois, conclusions):
    nlp = spacy.load('en_core_web_trf')
    for e, (doi, conclusion) in enumerate(zip(dois, conclusions)):
        if not e % 100:
            print("%s - %s" % (e, conclusion))
        doc = nlp(conclusion)
        ner = recognize_named_entities(conclusion)
        for subject, predicate, object in sentence_to_svos(doc, ner):
            if subject is None:
                continue
            yield {"doi": doi, "subject": subject, "predicate": predicate, "object": object}
