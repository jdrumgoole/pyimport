import requests


class LineReader:
    def __init__(self, filename_or_url):
        self._filename_or_url = filename_or_url
        self._file = None
        self._is_remote = self._filename_or_url.startswith('http://') or self._filename_or_url.startswith('https://')

    def __enter__(self):
        if self._is_remote:
            self._file = self._read_remote_file()
        else:
            self._file = open(self._filename_or_url, 'r')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file and not self._is_remote:
            self._file.close()

    def __iter__(self):
        return self._line_generator()

    def _read_remote_file(self):
        response = requests.get(self._filename_or_url, stream=True)
        response.raise_for_status()
        return response.iter_lines(decode_unicode=True)

    def _line_generator(self):
        if self._file is None:
            raise StopIteration

        if self._is_remote:
            for line in self._file:
                yield line.strip()
        else:
            for line in self._file:
                yield line.strip()

    # Usage example for local file:


# with LineReader('example.txt') as file_iter:
#     for line in file_iter:
#         print(line)
#
#     # Usage example for remote file:
# with LineReader('http://example.com/remotefile.txt') as file_iter:
#     for line in file_iter:
#         print(line)
