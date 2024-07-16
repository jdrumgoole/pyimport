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
                 fields: List[str] = None,
                 has_header: bool = False,
                 delimiter: str = ",",
                 limit: int = 0):

        self._filename: str = filename
        self._limit = limit
        self._has_header = has_header
        self._header_line = None
        self._fields = fields
        self._dict_reader = None
        self._file = None
        if delimiter == "tab":
            self._delimiter = "\t"
        else:
            self._delimiter = delimiter

    def _open_reader(self):
        self._file = open(self._filename, 'r')
        self._dict_reader = csv.DictReader(self._file, fieldnames=self._fields, delimiter=self._delimiter)
        return self._dict_reader

    def _close_reader(self):
        self._file.close()
        self._dict_reader = None
        self._file = None
    def __enter__(self):
        self._open_reader()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_reader()

    @staticmethod
    def skip_lines(f, skip_count: int):
        """
        >>> f = open( "test_set_small.txt", "r" )
        >>> skipLines( f , 20 )
        20
        """

        line_count = 0
        if skip_count > 0:
            # print( "Skipping")
            dummy = f.readline()  # skipcount may be bigger than the number of lines i  the file
            while dummy:
                line_count = line_count + 1
                if line_count == skip_count:
                    break
                dummy = f.readline()
        return line_count

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def header_line(self) -> List[str]:
        return self._header_line

    def __iter__(self):
        self._open_reader()

    def __next__(self):
        return self.readline(limit=0)

    @staticmethod
    def read_first_lines(filename: str, limit: int= 10) -> str:
        lines = []
        with open(filename, mode='r') as file:
            for i in range(limit):
                line = file.readline()
                if not line:  # Break if there are fewer than 10 lines in the file
                    break
                lines.append(line)
        return ''.join(lines)

    @staticmethod
    def sniff_header(filename: str) -> bool:
        sample = FileReader.read_first_lines(filename)
        sniffer = csv.Sniffer()  # Create a Sniffer object
        has_header = sniffer.has_header(sample)  # Use Sniffer to detect header
        return has_header

    @property
    def delimiter(self):
        return self._delimiter

    def iterate_rows(self,
                     iterator: Iterator[List[str]],
                     limit: int = 0) -> Iterator[dict]:
        """
        Iterate rows in a presumed CSV file.

        :param iterator: Read from this iterator
        :param limit: Only read up to limit lines (0 for all lines)
        :return: An iterator providing parsed lines.
        """

        # size = 0

        reader = csv.DictReader(iterator, fieldnames=self._fields, delimiter=self._delimiter)
        # TODO: Handle the situation where the wrong delimiter is passed
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
        if self._filename.startswith("http"):
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
        yield from self.iterate_rows(FileReader.read_remote_by_line(self._filename),
                                     limit=limit)

    def read_local_file(self, limit: int = 0) -> Iterator[List[str]]:

        with open(self._filename, newline="") as csv_file:
            yield from self.iterate_rows(csv_file, limit=limit)
