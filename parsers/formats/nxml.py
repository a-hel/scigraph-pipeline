from typing import List
from lxml import etree

def _expand_section(section):
    section = "\n".join([sec for sec in section.itertext()])
    return section

def _text_from_xpath(root, xpath):
    elems = root.xpath(xpath)
    elem_text = "\n".join([_expand_section(elem) for elem in elems if elem is not None])
    return elem_text


def _extract_abstract(root):
    abstract_xpath = "//*/abstract"
    abstract_text = _text_from_xpath(root, abstract_xpath)
    return abstract_text


def _extract_section(root, synonyms):
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

def parse(plaintext: str, include: List=[
        "research-article",
    ]):
    article_elements = {
        "Abstract": _extract_abstract,
        "Introduction": lambda root: _extract_section(root, intro_synonyms),
        "Conclusion": lambda root: _extract_section(root, conc_synonyms),
    }
    error_msgs = {None: "%s is None.", False: "%s is False.", "": "%s is empty."}
    root = etree.fromstring(plaintext)
    article_type = root.xpath("//article/@article-type")[0]
    if article_type not in include:
        raise TypeError('Article is of type "%s"' % article_type)
    article = {k: v(root) for k, v in article_elements.items()}
    for k, v in article.items():
        if v in error_msgs:
            raise ValueError(error_msgs[v] % k)
    return article