import csv
from datetime import datetime
from typing import Iterator, List
import _csv
import requests

class FileReader:
    """
    Read CSV lines from a local file or a URL. Provide a generator that returns dicts of the
    lines as key->value pairs, where the keys are the column names.
    """

    UTF_ENCODING = "utf-8"
    URL_CHUNK_SIZE = 1024 * 1024

    def __init__(self,
                 filename: str,
                 has_header: bool = False,
                 delimiter: str = ",",
                 limit: int = 0):

        self._name: str = filename
        self._limit = limit
        self._has_header = has_header
        self._header_line = None

        if delimiter == "tab":
            self._delimiter = "\t"
        else:
            self._delimiter = delimiter

    @property
    def name(self) -> str:
        return self._name

    @property
    def header_line(self) -> List[str]:
        return self._header_line

    @property
    def delimiter(self):
        return self._delimiter

    def iterate_rows(self,
                     iterator: Iterator[List[str]],
                     limit: int = 0) -> Iterator[List[str]]:
        """
        Iterate rows in a presumed CSV file.

        :param iterator: Read from this iterator
        :param limit: Only read up to limit lines (0 for all lines)
        :return: An iterator providing parsed lines.
        """

        # size = 0

        reader = csv.reader(iterator, delimiter=self._delimiter)

        if self._has_header and self._header_line is None:
            self._header_line = next(reader)

        try:
            for i, row in enumerate(reader, 1):
                if (limit > 0) and (i > limit):
                        break
                else:
                    yield row
        except _csv.Error as e:
            print(f"Exception: {e} at line {i}. {row}")
            raise

    def __iter__(self):
        return self

    def __next__(self):
        yield from self.readline(limit=0)

    def readline(self, limit:int = 0) -> Iterator[List[str]]:
        if self._name.startswith("http"):
            yield from self.read_url_file(limit=limit)
        else:
            yield from self.read_local_file(limit=limit)

    @staticmethod
    def read_remote_by_line(url: str) -> Iterator[List[str]]:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            residue=None
            for chunk in r.iter_content(FileReader.URL_CHUNK_SIZE, decode_unicode=True):
                if chunk:
                    for line in chunk.splitlines(keepends=True):
                        if residue:
                            line = residue + line
                            residue=None
                        if line[-1:] == "\n" or line[-1:] == "\r":
                            yield line
                        else:
                            residue = line
            assert residue is None

    def read_url_file(self, limit: int = 0) -> Iterator[List[str]]:
        yield from self.iterate_rows(FileReader.read_remote_by_line(self._name),
                                     limit=limit)

    def read_local_file(self, limit: int = 0) -> Iterator[List[str]]:

        with open(self._name, newline="") as csv_file:
            yield from self.iterate_rows(csv_file, limit=limit)
