import json


import os
import json
import time
import more_itertools
import asyncio

import requests

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

async def _metamap_call(record):
    metamap_path = os.getenv('METAMAP_PATH')
    version_cmd = ""
    metamap_version = "2018AB"
    cmd = 'echo "%s" | %s/bin/metamap --lexicon db -Z 2018AB -I --JSONn'#f 4'
    text = record.get("conclusion")
    summary_id = record.get("id")
    for exclude in ["conclusions", "conclusion"]:
        if text.lower().startswith(exclude):
            text = text[len(exclude):]

    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("'", "")# "\\'")

    proc = await asyncio.create_subprocess_shell(
        cmd % (text, metamap_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)


    stdout, stderr = await proc.communicate()
    if proc.returncode:
        print("Metamap error: %s" % stderr.decode('utf-8'))
        raise ValueError("Metamap error: %s" % stderr.decode('utf-8'))
    
    
    txt = stdout.decode('utf-8')
    json_txt = txt[txt.index('{'):]
    if json_txt == '{"AllDocuments":[':
        print(text)
        raise IOError(f"Metamap error or server not running. Start with \n `{metamap_path}/bin/skrmedpostctl start`\n`{metamap_path}/bin/wsdserverctl start`")
    try:
        data = json.loads(json_txt)
    except json.JSONDecodeError:
        print(text)
        raise ValueError("Invalid metamap output: %s" % json_txt)
    except ValueError:
        raise ValueError("Could not parse metamap output: %s" % stdout.decode('utf8'))
    phrases = data['AllDocuments'][0]['Document']['Utterances'][0]['Phrases']
    named_entities = [ne for ne in to_ner(phrases, summary_id, metamap_version)]
    return named_entities

def to_ner(phrases, summary_id, metamap_version):
    for item in phrases:
        if not item['Mappings']:
            continue
        for elem in item['Mappings']:
            named_entity = {
                "summary_id": summary_id,
                "matched_term": item['Mappings'][0]['MappingCandidates'][0]['CandidateMatched'].lstrip('*^'),
                "preferred_term": 
        elem['MappingCandidates'][0]['CandidatePreferred'],
        'cui':
        elem['MappingCandidates'][0]['CandidateCUI'],
        'metamap_version': metamap_version}
            yield named_entity

async def _collect_output(conclusions):
    data = await asyncio.gather(
        *[_metamap_call(record) for record in conclusions],
        return_exceptions=True
    )
    return data

def _parse_locally(conclusions, batch_size=20):
    loop = asyncio.get_event_loop()
    for batch in more_itertools.ichunked(conclusions, batch_size):

        results = loop.run_until_complete(
            _collect_output(
                batch
            )
        )
        for result in results:
            if not result:
                continue
            if isinstance(result, Exception):
                continue
            yield from result
    loop.close()

    
        


def recognize_named_entities(sentences, parser='local'):
    """Extract MeSH terms from text."""
    parsers = {'local': _parse_locally, 'web': _parse_online}
    parser = parsers[parser]

    yield from parser(sentences)


