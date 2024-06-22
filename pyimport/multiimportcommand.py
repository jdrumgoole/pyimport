import asyncio
import logging
import multiprocessing
import os
import time
from multiprocessing import Process

from pyimport import commandutils, asynccommandutils
from pyimport.command import seconds_to_duration
from pyimport.commandutils import ImportResults
from pyimport.fieldfile import FieldFile


class MultiImportCommand:

    def __init__(self, audit=None, args=None):

        self._audit = audit
        self._args = args
        self._log = logging.getLogger(__name__)

        commandutils.print_args(self._log, args)
        self._log.info(f"Pool size        : {args.poolsize}")
        self._log.info(f"Fork using       : {args.forkmethod}")

    def async_processor(self, q: multiprocessing.Queue, filename: str):
        total_written, elapsed = asyncio.run(
            asynccommandutils.process_one_file(self._log, self._args, self._audit, filename))
        results = ImportResults(total_written, elapsed, filename)
        q.put(results)

    def sync_processor(self, q: multiprocessing.Queue, filename: str):
        total_written, elapsed = commandutils.process_one_file(self._log, self._args, filename)
        results = ImportResults(total_written, elapsed, filename)
        q.put(results)

    def run(self) -> [int, float]:
        proc_list = []
        total_written = 0
        try:
            time_start = time.time()
            output_q = multiprocessing.Queue()
            for arg_list in commandutils.chunker(self._args.filenames, self._args.poolsize):  # blocks of poolsize
                proc_list = []
                for filename in arg_list:
                    if not os.path.isfile(filename):
                        self._log.warning(f"No such file: '{filename}' ignoring")
                        continue

                    self._log.info(f"Processing:'{filename}'")
                    if self._args.asyncpro:
                        proc = Process(target=self.async_processor, args=(output_q, filename,))
                    else:
                        proc = Process(target=self.sync_processor, args=(output_q, filename,))
                    proc.start()
                    proc_list.append(proc)

                for p in proc_list:
                    p.join()

            while not output_q.empty():
                r = output_q.get()
                self._log.info(f"imported file: '{r.filename}' ({r.total_written} rows)")
                self._log.info(f"Total elapsed time to upload '{r.filename}' : {seconds_to_duration(r.elapsed_time)}")
                total_written += r.total_written

            time_finish = time.time()
            elapsed_time = time_finish - time_start
            self._log.info(f"Total elapsed time to upload all files : {seconds_to_duration(elapsed_time)}")
            self._log.info(f"Average upload rate per second: {round(total_written / elapsed_time)}")

        except KeyboardInterrupt:
            self._log.error(f"Keyboard interrupt... exiting")
            for p in proc_list:
                p.kill()

        return total_written, elapsed_time

    def total_written(self):
        return self._total_written

    @property
    def field_info(self):
        return self._field_file
