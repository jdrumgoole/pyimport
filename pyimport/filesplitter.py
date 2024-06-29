"""
Created on 13 Aug 2017

@author: jdrumgoole

=====================================
File_Splitter
=====================================

File Splitter is a class that takes a file and splits it into separate pieces. Its purpose built for
use with pyimport and is expected to be used to split CSV files (which may or may not have
a header, hence the **has_header** argument). When splitting a file the output files are produced without
a header file.

The file can be split by number of lines using the **splitfile** function. Alternatively
the file may be split automatically into a number of pieces specified by as a parameter to
**autosplit**. Autosplitting is achieved by by guessing the average line os_size by looking at
the first ten lines and taking an average of those lines.

The output files have the same filename as the input file with a number appended ( .1, .2, .3 etc.).

There is also a **count_lines** function to thread_id the lines in a file.

"""
import os
import shutil
from enum import Enum
from typing import Generator, Tuple


class BlockReader(object):
    BLOCK_SIZE = 64 * 1024

    def __init__(self, filename, block_size=None):

        self._filename = filename

        if block_size:
            self._block_size = block_size
        else:
            self._block_size = BlockReader.BLOCK_SIZE

    def __enter__(self):
        self._file = open(self.filename, "rb")
        return self._file

    def __exit__(self, *args):
        self._file.close()

    @staticmethod
    def read_blocks(file, block_size=None):

        if not block_size:
            block_size = BlockReader.BLOCK_SIZE

        while True:
            # disable universal newlines so that sizes are correct when
            # reading DOS and Linux files.
            b = file.read(block_size)
            if not b:
                break
            yield b

    @staticmethod
    def readline(file):
        return file.readline()

    def read_fd(self, fd):
        for block in self.read_blocks(fd, self._block_size):
            yield block

    def read_file(self, filename):
        with open(filename, "rb") as f:
            yield from self.read_fd(f)


class FileType(Enum):
    DOS = 1
    UNIX = 2


class CounterException(Exception):
    pass


class LineCounter:
    """
    Count the lines in a file efficiently by reading in a block
    at a time and counting '\n' chars. Blocks are large by
    default (64k).
    """

    def __init__(self, filename=None, count_now=True):

        self._first_line = None
        self._line_count = None
        self._file_size = 0
        self._filename = filename

        if count_now and filename:
            LineCounter.count_now(self._filename)

    @property
    def line_count(self):
        if self._line_count is None:
            raise CounterException
        else:
            return self._line_count

    def first_line(self):
        return self._first_line

    def file_size(self):
        return self._file_size

    @staticmethod
    def count_lines_in_file(file_path:str) -> int:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for _ in f)

    def count_now(*args):
        total_lines = 0
        for file_path in args:
            if os.path.isfile(file_path):
                total_lines += LineCounter.count_lines_in_file(file_path)
            else:
                raise FileNotFoundError(file_path)
        return total_lines


    @staticmethod
    def skip_lines(f, skip_count):
        """
        >>> f = open( "test_set_small.txt", "r" )
        >>> skipLines( f , 20 )
        20
        """

        line_count = 0
        if skip_count > 0:
            # print( "Skipping")
            dummy = f.readline()  # skipCount may be bigger than the number of lines i  the file
            while dummy:
                line_count = line_count + 1
                if line_count == skip_count:
                    break
                dummy = f.readline()

        return line_count


class FileSplitter:
    """
    Split a file into a number of segments. You can autosplit a file into a specific
    number of pieces (autosplit) or divide in segments of a specific os_size (splitfile)
    """

    def __init__(self, input_filename, has_header=False):
        """

        Need to work out how to get line_count etc. consist for unit testing. Needs to be
        canonical for DOS and UNIX files.

        WIP

        :param input_filename : The file to be split
        has_header : Does this file have a header line
        """
        self._input_filename = input_filename
        self._has_header = has_header
        self._line_count = None
        self._header_line = ""  # Not none so len does something sensible when has_header is false

        self._header_line, self._file_type = self.get_file_type_and_header(filename=self._input_filename,
                                                                             has_header=has_header)
        # cls._data_lines_count = 0
        self._size_threshold = 1024 * 10
        self._split_size = None
        self._auto_splits = None
        self._splits = None

    @property
    def line_count(self):
        if self._line_count is None:
            self._line_count = LineCounter.count_now(self._input_filename)
            return self._line_count
        else:
            return self._line_count

    @staticmethod
    def compare_files(lhs:str, rhs:str) -> bool:
        lhs_file = rhs_file = None
        try:
            lhs_file = open(lhs, 'r', encoding='utf-8')
            rhs_file = open(rhs, 'r', encoding='utf-8')

            for line1, line2 in zip(lhs_file, rhs_file):
                if line1 != line2:
                    return False

            # Ensure both files have reached EOF
            if lhs_file.read(1) or rhs_file.read(1):
                return False
        finally:
            if lhs_file:
                lhs_file.close()
            if rhs_file:
                rhs_file.close()

        return True

    @staticmethod
    def compare_concatenated_files(original_filename:str, list_of_filenames:list[str]) -> bool:
        # Create a temporary file to store the concatenated content
        temp_filename = 'temp_concatenated_file.txt'

        try:
            with open(temp_filename, 'w', encoding='utf-8') as temp_file:
                # Concatenate the contents of the files in the list
                for filename in list_of_filenames:
                    with open(filename, 'r', encoding='utf-8') as f:
                        shutil.copyfileobj(f, temp_file)

            return FileSplitter.compare_files(original_filename, temp_filename)
        finally:
            os.unlink(temp_filename)

    @staticmethod
    def get_header(filename):
        with open(filename, "r") as f:
            header = f.readline()
        return header                  #.rstrip()

    @staticmethod
    def get_file_type_and_header(filename:str, has_header:bool) -> [str|None, FileType]:
        line = ""
        header_line = None
        with open(filename, "r") as f:
            if has_header:
                header_line = f.readline()
            if f.newlines and f.newlines == '\r\n':
                file_type = FileType.DOS
            else:
                file_type = FileType.UNIX
        return header_line, file_type

    @staticmethod
    def new_file(filename, ext):
        basename = os.path.basename(filename)
        filename = f"{basename}.{ext}"

        newfile = open(filename, "w")
        return newfile, filename

    @staticmethod
    def copy_file(lhs, rhs, ignore_header=True) -> Tuple[str, int]:
        """
        Copy the input file to the file ;param rhs. If :param
        ignore_header is true the strip the header during copying.
        :param lhs:
        :param rhs:
        :param ignore_header:
        :return:
        """

        line_count = 0
        with open(lhs, "r") as input_file:
            if ignore_header:
                _ = input_file.readline()
            with open(rhs, "w") as output_file:
                for i in input_file:
                    line_count = line_count + 1
                    output_file.write(i)

        return rhs, line_count

    @property
    def has_header(self):
        return self._has_header

    def header_line(self):
        return self._header_line

    def no_header_size(self):
        return self._size - len(self._header_line)

    def output_files(self):
        return list(self._files.keys())

    # def data_lines_count(cls):
    #     return cls._data_lines_count

    @staticmethod
    def split_file(filename: str, split_size, has_header=False) -> Generator:

        file_is_open = False
        if split_size < 1:
            yield FileSplitter.copy_file(filename, filename + ".1", ignore_header=has_header)
        else:
            with (open(filename, 'r', encoding='utf-8') as file):
                try:
                    part_num = 1
                    lines_in_part = 0

                    for line_num, line in enumerate(file, start=1):
                        if not file_is_open:
                            part_file = open(f"{filename}.{part_num}", 'w', encoding='utf-8')
                            file_is_open = True
                        if line_num == 1 and has_header:
                            continue
                        else:
                            part_file.write(line)
                            lines_in_part += 1
                            if lines_in_part == split_size:
                                part_file.close()
                                file_is_open = False
                                yield part_file.name, lines_in_part
                                part_num += 1
                                lines_in_part = 0

                    if lines_in_part > 0:
                        part_file.close()
                        file_is_open = False
                        yield part_file.name, lines_in_part

                finally:
                    if file_is_open:
                        part_file.close()

    def file_type(self):
        return self._file_type

    @staticmethod
    def get_average_line_size(filename, has_header:bool=None, sample_size=10):
        """
        Read the first sample_size lines of a file (ignoring the header). Use these lines to estimate the
        average line os_size.
        :return: average_line_size
        """

        line_sample = 10
        count = 0
        line = None
        sample_lines =[]
        with open(filename, "r") as f:
            for i, line in enumerate(f, start=1):
                if i == 1 and has_header:
                    continue
                if i <= sample_size:
                    sample_lines.append(line)
                else:
                    break
        if has_header:
            i = i-1

        if i >= 1:
            if i < sample_size:
                sample_size = i
            avg_line_size = int(round((sum(len(line) for line in sample_lines) / sample_size)))
        else:
            avg_line_size = 0

        return avg_line_size

    @staticmethod
    def shim_names(g):
        for i in g:
            yield i[0]

    @staticmethod
    def autosplit(filename, has_header, split_count):

        average_line_size = FileSplitter.get_average_line_size(filename, has_header)

        if average_line_size > 0:
            if split_count > 0:
                file_size = os.path.getsize(filename)

                total_lines = int(round(file_size / average_line_size))
                # print( "total lines : %i"  % total_lines )

                split_size = int(round(total_lines / split_count))
            else:
                split_size = 0

            # print("Splitting '%s' into at least %i pieces of os_size %i" % (
            # cls._input_filename, split_count + 1, cls._split_size))
            yield from FileSplitter.split_file(filename, split_size, has_header)


def split_files(args) -> [(str, int)]:

    files = []
    for filename in args.filenames:
        if not os.path.isfile(filename):
            print(f"No such input file:'{filename}'")
            continue

        splitter = FileSplitter(filename, args.hasheader)

        if args.autosplit or args.splitsize == 0:
            if args.verbose and not args.input:
                print(f"Autosplitting: '{filename}' into approximately {args.autosplit} parts")
            for name, size in FileSplitter.autosplit(filename, args.hasheader, args.autosplit):
                files.append((name, size))
        else:
            if args.verbose and not args.input:
                print(f"Splitting '{filename}' using {args.splitsize}")
            for name, size in splitter.split_file(args.splitsize):
                files.append((name, size))

        count = 1
        original_lines = splitter.line_count
        total_new_lines = 0

        for name, lines in files:
            total_new_lines = total_new_lines + lines

            if args.input:
                print(f"{name} ", end="")
            else:
                if args.verbose:
                    print(f"{count:4}. '{name}' Lines: {lines:6}")
                    count = count + 1
        if args.input:
            print("")

        if len(files) > 1:
            if args.verbose and not args.input:
                print(f"Original file: '{filename}' Lines: {original_lines}")

        if splitter.has_header:
            original_lines = original_lines - 1
        if files and (total_new_lines != original_lines):
            raise ValueError(f"Lines of '{filename}' and total lines of pieces"\
                             f"{files}"
                             f"\ndo not match:"
                             f"\noriginal_lines : {original_lines}"
                             f"\npieces lines   : {total_new_lines}")

    return files
