import time

import requests
import aiohttp
from typing import TextIO

from requests.adapters import HTTPAdapter
from urllib3 import Retry


class BlockReader:

    def __init__(self, file: TextIO, block_size:int = 1024*1024):
        self._block_size = block_size
        self._file = file

    def __iter__(self):
        while True:
            block = self._file.read(self._block_size)
            if block:
                yield block
            else:
                break


def diff(lhs:str, rhs: str) -> bool:
    with open(lhs, 'r') as f1:
        with open(rhs, 'r') as f2:
            for line1, line2 in zip(f1, f2):
                if line1 != line2:
                    return False
    return True


def is_url(url: str) -> bool:
    return url.startswith("http") or url.startswith("https")


class LocalLineReader:

    def __init__(self, file: TextIO, skip_lines=0):
        self._file = file
        self._skip_lines = skip_lines

    def __iter__(self):
        for i, line in enumerate(self._file, 1):
            if i <= self._skip_lines:
                continue
            yield line.strip()

    @property
    def filename(self):
        return self._file.name

    @staticmethod
    def read_first_lines(filename: str, limit: int = 10) -> str:
        lines = []
        with open(filename, mode='r') as file:
            for i in range(limit):
                line = file.readline()
                if not line:  # Break if there are fewer than 10 lines in the file
                    break
                lines.append(line)
        return ''.join(lines)


class RemoteLineReader:

    def __init__(self, url: str, skip_lines=0, block_size=1024*1024):
        self.url = url
        self._skip_lines = skip_lines
        self._block_size = block_size

    @property
    def filename(self):
        return self.url

    @staticmethod
    def robust_download(url:str, max_retries:int=3):
        # Configure retries for connection errors
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        for attempt in range(max_retries + 1):
            try:
                response = session.get(url, stream=True, timeout=(10, 300))
                response.raise_for_status()
                return response

            except requests.exceptions.ConnectionError as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    time.sleep(wait_time)
                else:
                    raise

            except requests.exceptions.RequestException as e:
                print(f"Non-connection error: {e}")
                raise

        return None

    def __iter__(self):
        with self.robust_download(self.url) as r:
            residue = None
            for chunk in r.iter_content(self._block_size, decode_unicode=True):
                if chunk:
                    for i, line in enumerate(chunk.splitlines(keepends=True), 1):
                        if i <= self._skip_lines:
                            continue
                        yield line.strip()
            assert residue is None


class AsyncRemoteLineReaderException(Exception):
    pass


class AsyncRemoteLineReader:
    def __init__(self, url,skip_lines=0, block_size=1024*1024):
        self.url = url
        self._skip_lines = skip_lines
        self._block_size = block_size

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        self.response = await self.session.get(self.url)
        self.response.raise_for_status()  # Ensure we get a successful response
        self.content = self.response.content
        for _ in range(self._skip_lines):
            line = await self.content.readline()
            if line is None:
                break

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.response.release()
        await self.session.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        line = await self.content.readline()
        if line:
            return line.decode('utf-8').strip()
        else:
            raise StopAsyncIteration
