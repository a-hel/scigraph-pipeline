import os
import csv
from lxml import etree
import logging


def _normalize_title(context, title):
    try:
        title = title[0].lower()
    except IndexError:
        title = ""
    return [title]


ns = etree.FunctionNamespace(None)
ns["_normalize"] = _normalize_title


def id_convert(filename):
    print(os.getcwd())
    with open(filename, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(reader)
        lookup = {row[8]: row[7] for row in reader}
    print("Generated lookup table with %s article names." % len(lookup))
    return lookup


def _expand_section(section):
    section = "\n".join([sec for sec in section.itertext()])
    return section


def _text_from_xpath(root, xpath):
    elems = root.xpath(xpath)
    elem_text = "\n".join([_expand_section(elem) for elem in elems if elem is not None])
    return elem_text


def extract_abstract(root):
    abstract_xpath = "//*/abstract"
    abstract_text = _text_from_xpath(root, abstract_xpath)
    return abstract_text


def extract_section(root, synonyms):
    xpaths = [
        "//*/sec[child::title[starts-with(_normalize(text()), '%s')]]",
        "//*/sec[starts-with(_normalize(@sec-type),'%s')]",
    ]
    for xpath in xpaths:
        for synonym in synonyms:
            full_xpath = xpath % synonym
            text = _text_from_xpath(root, full_xpath)
            if not text == "":
                for syn in reversed(synonyms):
                    if text.startswith(syn):
                        text = text[len(syn) :]
                return text
    return text


def parse_article(plaintext, elements, show_error=False):
    error_msgs = {None: "%s is None.", False: "%s is False.", "": "%s is empty."}
    root = etree.fromstring(plaintext)
    article_type = root.xpath("//article/@article-type")[0]
    if article_type not in [
        "research-article",
    ]:  #'review-article']:
        raise TypeError('Article is of type "%s"' % article_type)
    article = {k: v(root) for k, v in elements.items()}
    for k, v in article.items():
        if v in error_msgs:
            raise ValueError(error_msgs[v] % k)
    return article


def parse_file(path, filename, lookup):
    intro_synonyms = ["introduction", "background"]
    conc_synonyms = ["conclusion", "conclusions", "summary", "discussion"]
    article_elements = {
        "Abstract": extract_abstract,
        "Introduction": lambda root: extract_section(root, intro_synonyms),
        "Conclusion": lambda root: extract_section(root, conc_synonyms),
    }
    pmc = filename.rsplit(".", 1)[0]
    doi = lookup.get(pmc, pmc)
    full_path = os.path.join(path, filename)
    article_data = {"doi": doi or pmc, "origin": full_path}
    with open(full_path, "rb") as f:
        plaintext = f.read()
    try:
        article = parse_article(plaintext, article_elements)
        article.update(article_data)
        error = None
    except ValueError as e:
        article = article_data
        error = "%s - %s" % (full_path, str(e))
    except TypeError as e:
        article = None
        error = "%s - %s" % (full_path, str(e))
    return article, error


def parse_from_folder(folder, lookup, suffix="nxml"):
    for file in os.listdir(folder):
        if not file.endswith(suffix):
            continue
        article, error = parse_file(folder, file, lookup)
        if article is None:
            logging.info(error)
            continue
        yield article

def load_article(data):
    intro_synonyms = ["introduction", "background"]
    conc_synonyms = ["conclusion", "conclusions", "summary", "discussion"]
    article_elements = {
        "Abstract": extract_abstract,
        "Introduction": lambda root: extract_section(root, intro_synonyms),
        "Conclusion": lambda root: extract_section(root, conc_synonyms),
    }
    article_data = {"doi": data.uri, "origin": data.uri}
    with open(data.uri, "rb") as f:
        plaintext = f.read()
    try:
        article = parse_article(plaintext, article_elements)
        article.update(article_data)
        error = None
    except ValueError as e:
        article = article_data
        error = "%s - %s" % (data.uri, str(e))
    except TypeError as e:
        article = None
        error = "%s - %s" % (data.uri, str(e))
    return article, error


def parse_articles(folder):
    # lookup = id_convert(os.path.join(os.getenv('ASSET_DIR'), "PMC-ids.csv"))
    lookup = {}

    def step(data=None):
        results = parse_from_folder(folder, lookup)
        yield from results

    return step
