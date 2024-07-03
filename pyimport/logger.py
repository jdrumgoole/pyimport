"""
Created on 28 Jun 2017

@author: jdrumgoole
"""

import logging


class Log:
    #format_string = "%(asctime)s: %(filename)s:%(funcName)s:%(lineno)s: %(levelname)s: %(message)s"

    LOGGER_NAME = "pyimport"
    FORMAT_STRING = "%(message)s"

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
    def add_file_handler( log_filename=None, log_level=None):

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

    # def setup_custom_logger(self, name, log_level=None):
    #     formatter = self.formatter()
    #
    #     handler = logging.StreamHandler()
    #     handler.setFormatter(formatter)
    #
    #     logger = logging.getLogger(name)
    #     if log_level:
    #         logger.setLevel(log_level)
    #     else:
    #         logger.setLevel(logging.INFO)
    #     logger.setLevel(logging.DEBUG)
    #
    #     logger.addHandler(handler)
    #     return logger
