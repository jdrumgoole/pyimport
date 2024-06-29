from datetime import datetime, timezone

from pyimport.command import seconds_to_duration


class ImportResult:
    def __init__(self, total_written, elapsed_time, filename, error=None):
        self._filename = filename
        self._total_written = total_written
        self._elapsed_time = elapsed_time
        self._error = error
        self._timestamp = datetime.now(timezone.utc)
        if total_written is not None and elapsed_time is not None:
            self._average_rate = total_written / elapsed_time
        else:
            self._average_rate = None

        if elapsed_time is not None:
            self._elapsed_duration = seconds_to_duration(elapsed_time)
        else:
            self._elapsed_duration = None

    @classmethod
    def import_error(cls, filename, error):
        return cls(None, None, filename, error)

    def __bool__(self):
        return not self._error

    def __str__(self):
        return f"Total written:{self.total_written}, Elapsed time:{seconds_to_duration(self.elapsed_time)}"

    @property
    def total_written(self):
        if self._error:
            return None
        else:
            return self._total_written

    @property
    def elapsed_time(self):
        if self._error:
            return None
        else:
            return self._elapsed_time

    @property
    def elapsed_duration(self):
        return self._elapsed_duration

    @property
    def filename(self):
        if self._error:
            return None
        else:
            return self._filename

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def avg_records_per_sec(self):
        return self._average_rate

    @property
    def error(self):
        return self._error

    def __repr__(self):
        if self._error:
            return f"ImportResults( None, None, {self.filename}, {self.import_error})"
        else:
            return f"ImportResults({self.total_written}, {self.elapsed_time}, {self.filename})"


class ImportResults:
    def __init__(self, results: list[ImportResult]|None = None):
        if results is None:
            self._results = []
        else:
            self._results = results

        self._total_results = sum(1 for r in self.results if not r.error)
        self._total_errors = len(results) - self._total_results

        if self._total_results == 0:
            self._total_written = None
            self._elapsed_time = None
        else:
            self._total_written = sum([r.total_written for r in self.results])
            self._elapsed_time = sum([r.elapsed_time for r in self.results])

    @property
    def results(self):
        return self._results

    @property
    def total_results(self):
        return self._total_results

    @property
    def total_errors(self):
        return self._total_errors

    @property
    def filenames(self):
        return [r.filename for r in self.results if not r.error]

    @property
    def errors(self):
        return [r for r in self.results if r.error]

    def filename_results(self, filename):
        candidates = [r for r in self.results if r.filename == filename]
        if len(candidates) == 0:
            return None
        else:
            return candidates[0]

    @property
    def total_written(self):
        return self._total_written

    @property
    def elapsed_time(self):
        return self._elapsed_time

    @property
    def avg_records_per_sec(self):
        if self._elapsed_time is None or self._total_written is None:
            return None
        else:
            return self._total_written / self._elapsed_time

    @property
    def duration(self):
        if self._elapsed_time is None:
            return None
        else:
            return seconds_to_duration(self._elapsed_time)

    def __str__(self):
        return f"Total written:{self._total_written}, Elapsed time:{self.duration}"

    def __repr__(self):
        return f"ImportResults({self.results})"
