"""
Created on 28 Jun 2017

@author: jdrumgoole
"""

import logging


class Logger:
    LOGGER_NAME = "pyimport"
    '''
    Logging class that encapsulates logging interface
    '''

    #format_string = "%(asctime)s: %(filename)s:%(funcName)s:%(lineno)s: %(levelname)s: %(message)s"
    format_string = "%(message)s"

    def __init__(self, logger_name, log_level=None):

        self._logger_name = logger_name
        self._log_filename = None
        self._logger = logging.getLogger(logger_name)
        if log_level:
            self._logger.setLevel(log_level)
        else:
            self._logger.setLevel(logging.INFO)

        self.add_null_hander()
        self._null_handler = True

    @staticmethod
    def formatter():
        return logging.Formatter(Logger.format_string)

    @staticmethod
    def add_null_hander(name=None):
        if name is None:
            name = Logger.LOGGER_NAME
        logger = logging.getLogger(name)
        logger.addHandler(logging.NullHandler())
        return logger

    @staticmethod
    def add_stream_handler(name=None, log_level=None):
        sh = logging.StreamHandler()
        sh.setFormatter(Logger.formatter())
        if log_level:
            sh.setLevel(log_level)
        else:
            sh.setLevel(logging.INFO)

        if name is None:
            name = Logger.LOGGER_NAME
        logger = logging.getLogger(name)
        logger.addHandler(sh)
        return logger

    @staticmethod
    def add_file_handler(name, log_filename=None, log_level=None):

        if log_filename is None:
            log_filename = name + ".log"
        else:
            log_filename = log_filename

        fh = logging.FileHandler(log_filename)
        fh.setFormatter(Logger.formatter())
        if log_level:
            fh.setLevel(log_level)
        else:
            fh.setLevel(logging.INFO)

        logger = logging.getLogger(name)
        logger.addHandler(fh)
        return logger

    def log(self):
        return self._logger

    def __call__(self):
        return self._logger

    @staticmethod
    def LoggingLevel(level="WARN"):

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

    def setup_custom_logger(self, name, log_level=None):
        formatter = self.formatter()

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        logger = logging.getLogger(name)
        if log_level:
            logger.setLevel(log_level)
        else:
            logger.setLevel(logging.INFO)
        logger.setLevel(logging.DEBUG)

        logger.addHandler(handler)
        return logger
