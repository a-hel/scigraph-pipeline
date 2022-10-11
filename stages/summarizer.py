import os
import datetime
import csv
from dotenv import load_dotenv
import logging

import torch
from fairseq.models.bart import BARTModel
import spacy


load_dotenv()


class Summarizer:
    def __init__(
        self,
        checkpoint_file=os.getenv("CHECKPOINT_FILE").replace(os.path.sep, "."),
        # test_fname="test.hypo",
        checkpoint_dir=os.path.join(
            os.getenv("ASSET_DIR"), os.getenv("SCITLDR_MODELDIR")
        ),
        datadir=os.path.join(os.getenv("ASSET_DIR"), os.getenv("SCITLDR_DATADIR")),
        lenpen=0.4,
        beam=2,
        max_len_b=30,
        min_len=5,
        no_repeat_ngram_size=3,
    ):
        self.checkpoint_file = checkpoint_file
        self.checkpoint_dir = checkpoint_dir
        self.datadir = datadir
        self.lenpen = lenpen
        self.beam = beam
        self.max_len_b = max_len_b
        self.min_len = min_len
        self.no_repeat_ngram_size = no_repeat_ngram_size
        self.bart = BARTModel.from_pretrained(
            self.checkpoint_dir,
            checkpoint_file=self.checkpoint_file,
            data_name_or_path=self.datadir + "-bin",
            task="translation",
        )

        if torch.cuda.is_available():
            logging.info("Cuda enabled")
            self.bart.cuda()
            self.bart.half()
        else:
            logging.info("Cuda not enabled.")
        self.bart.eval()

    def summarize(self, article, only_conclusion=False):

        if only_conclusion:
            article = [article["Conclusion"]]
        else:
            article = [
                " ".join(
                    [
                        article[section]
                        for section in ["Abstract", "Introduction", "Conclusion"]
                    ]
                ),
            ]
        article = [art.replace("\n", " ") for art in article]
        with torch.no_grad():
            hypotheses_batch = self.bart.sample(
                article,
                beam=self.beam,
                lenpen=self.lenpen,
                max_len_b=self.max_len_b,
                min_len=self.min_len,
                no_repeat_ngram_size=self.no_repeat_ngram_size,
            )
        return hypotheses_batch


def summarize_articles(articles, checkpoint_file=None):
    checkpoint_file = checkpoint_file or os.getenv("CHECKPOINT_FILE").replace(
        os.path.sep, "."
    )
    summarizer = Summarizer(checkpoint_file=checkpoint_file, max_len_b=50)
    for article in articles:
        try:
            summary = summarizer.summarize(article)
            conclusion = summarizer.summarize(article, only_conclusion=True)
            error = None
        except Exception as error:
            summary = None
            conclusion = None
            error = "Summarization error:\t%s\t%s\t%s\n" % (
                article["doi"],
                article["origin"],
                error,
            )

        data = {"doi": article["doi"], "summary": summary, "conclusion": conclusion}
        yield data


