import os

from pyimport.fieldfile import FieldFile
from pyimport.filesplitter import LineCounter
from pyimport.mdbimportcmd import MDBImportCommand
from test.mdbtest import MDBTestDB
import pytest

csv_files = [x for x in os.listdir() if x.endswith(".csv")]

csv_files = {'user_logins_comma.csv':";",
             'invoices_8.csv': '|',
             'user_logins_space.csv': " ",  # TODO: make sure we can parse space as a"space" delimiter like tab.
             'payments_1.csv': ":",
             'user_logins_asterisk.csv': "*",   # TODO: make sure we can parse * as a"asterisk" delimiter like tab.
             'reviews_3.csv': "tab",
             'user_logins_doublequote.csv': "\"",
             'products_9.csv': ":",
             'orders_6.csv': "|",
             'user_logins_colon.csv': ":",
             'products_7.csv': ",",
             'reviews_10.csv': ";",
             'user_logins_tab.csv': "tab",
             'user_logins_semicolon.csv': ";",
             'user_logins_caret.csv': "^",
             'products_5.csv': ";",
             'invoices_2.csv': "|",
             'user_logins_pipe.csv': "|",
             'user_logins_tilde.csv': "~",
             'subscriptions_4.csv': "|",
             'user_logins.csv': "tab",
             'logging_events.csv': "tab"}

csv_files_test = {
    'payments_1.csv': ":",
}


def test_import():
    with MDBTestDB() as tr:
        for filename, delimiter in csv_files.items():
            initial_size = tr.count()
            file_size = LineCounter.count_now(filename) - 1 # subtract header
            tr.args.add_arguments(filenames=[filename], delimiter=delimiter, hasheader=True)
            results = MDBImportCommand(tr.args.ns).run()
            this_result = results.filename_results(filename)
            assert this_result
            assert results.total_errors == 0
            final_size = tr.count()
            new_docs = final_size - initial_size
            assert new_docs == file_size, f"{filename} {delimiter} {new_docs} {file_size}"
            assert new_docs == results.total_written
            os.unlink(FieldFile.make_default_tff_name(filename))

