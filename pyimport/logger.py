"""
Created on 28 Jun 2017

@author: jdrumgoole
"""

import logging
from enum import Enum


class ErrorResponse(Enum):
    Ignore = "ignore"
    Warn = "warn"
    Fail = "fail"

    def __str__(self):
        return self.value


class Log:
    #format_string = "%(asctime)s: %(filename)s:%(funcName)s:%(lineno)s: %(levelname)s: %(message)s"

    LOGGER_NAME = "pyimport"
    FORMAT_STRING = "%(message)s"
    log = logging.getLogger(LOGGER_NAME)

    def __init__(self, log_level=None):
        self._log = logging.getLogger(Log.LOGGER_NAME)
        self.set_level(log_level)

    @staticmethod
    def set_level(self, log_level=None):
        log = logging.getLogger(Log.LOGGER_NAME)
        if log_level:
            log.setLevel(log_level)
        else:
            log.setLevel(logging.INFO)
        return log

    @staticmethod
    def formatter() -> logging.Formatter:
        return logging.Formatter(Log.FORMAT_STRING)

    @staticmethod
    def add_null_hander():
        log = logging.getLogger(Log.LOGGER_NAME)
        log.addHandler(logging.NullHandler())
        return log

    @staticmethod
    def add_stream_handler(log_level=None):
        sh = logging.StreamHandler()
        sh.setFormatter(Log.formatter())
        if log_level:
            sh.setLevel(log_level)
        else:
            sh.setLevel(logging.INFO)
        log = logging.getLogger(Log.LOGGER_NAME)
        log.addHandler(sh)
        return log

    @staticmethod
    def add_file_handler(log_filename=None, log_level=None):

        if log_filename is None:
            log_filename = Log.LOGGER_NAME + ".log"
        else:
            log_filename = log_filename

        fh = logging.FileHandler(log_filename)
        fh.setFormatter(Log.formatter())
        if log_level:
            fh.setLevel(log_level)
        else:
            fh.setLevel(logging.INFO)

        log = logging.getLogger(Log.LOGGER_NAME)
        log.addHandler(fh)
        return log

    @property
    def log(self):
        return self._log

    @staticmethod
    def logging_level(level="WARN"):

        loglevel = None
        if level == "DEBUG":
            loglevel = logging.DEBUG
        elif level == "INFO":
            loglevel = logging.INFO
        elif level == "WARNING":
            loglevel = logging.WARNING
        elif level == "ERROR":
            loglevel = logging.ERROR
        elif level == "CRITICAL":
            loglevel = logging.CRITICAL

        return loglevel


class ExitException(Exception):
    pass


def raise_exit_exception(msg):
    raise ExitException(msg)


class ErrorHandler:

    def __init__(self, error_handling=ErrorResponse.Warn):
        self._log = logging.getLogger(Log.LOGGER_NAME)
        self._handling = error_handling

        self._warn_handler = {
            ErrorResponse.Warn: self._log.warning,
            ErrorResponse.Fail: lambda msg: raise_exit_exception,
            ErrorResponse.Ignore: lambda: None,
        }

        self._error_handler = {
            ErrorResponse.Warn: self._log.error,
            ErrorResponse.Fail: lambda msg: raise_exit_exception,
            ErrorResponse.Ignore: lambda: None,
        }

        self._fatal_handler = {
            ErrorResponse.Warn: lambda msg: raise_exit_exception,
            ErrorResponse.Fail: lambda msg: raise_exit_exception,
            ErrorResponse.Ignore: lambda msg: raise_exit_exception
        }

    def info(self, msg):
        self._log.info(msg)

    def warning(self, msg):
        self._warn_handler[self._handling](msg)

    def error(self, msg):
        self._error_handler[self._handling](msg)

    def fatal(self, msg):
        self._fatal_handler[self._handling](msg)


eh = ErrorHandler(ErrorResponse.Warn)
ehf = ErrorHandler(ErrorResponse.Fail)

