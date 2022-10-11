import more_itertools
import tarfile
import os
import pandas as pd


class TarFileReader:
    def __init__(self, archive, lookup):
        lookup = pd.read_csv(lookup)
        self.lookup = lookup.set_index("AccessionID")
        print(archive)
        self.open_tarfile = tarfile.open(archive, "r")

    def __del__(self):
        try:
            self.open_tarfile.close()
        except AttributeError:
            pass

    def _member_to_text(self, member):
        file = self.open_tarfile.extractfile(member)
        content = file.read()
        return content

    def __iter__(self):
        for member in self.open_tarfile.getmembers():
            yield member.name, self._member_to_text(member)

    def __getitem__(self, key):
        art = self.lookup.loc[key]["Article File"]
        member = self.open_tarfile.getmember(art)
        content = self._member_to_text(member)
        return content


def batched(data, batch_size=100):
    yield from more_itertools.ichunked(data, batch_size)
