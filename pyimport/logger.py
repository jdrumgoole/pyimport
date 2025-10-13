"""
Created on 28 Jun 2017

@author: jdrumgoole
"""

import logging
import sys
from enum import Enum
from colorama import Fore, Style, init as colorama_init


class ErrorResponse(Enum):
    Ignore = "ignore"
    Warn = "warn"
    Fail = "fail"

    def __str__(self):
        return self.value


class ColoredFormatter(logging.Formatter):
    """
    Colored formatter for log messages.

    Uses colorama for cross-platform color support.
    """

    # Color scheme
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT,
    }

    # Prefixes for different log levels
    PREFIXES = {
        'DEBUG': '🔍 ',
        'INFO': '✓ ',
        'WARNING': '⚠ ',
        'ERROR': '✗ ',
        'CRITICAL': '🔥 ',
    }

    def __init__(self, fmt=None, use_color=True):
        super().__init__(fmt)
        self.use_color = use_color and self._supports_color()

    @staticmethod
    def _supports_color():
        """Check if the terminal supports color."""
        # Check if running in a terminal
        if not hasattr(sys.stdout, 'isatty'):
            return False
        if not sys.stdout.isatty():
            return False
        # Windows color support via colorama
        return True

    def format(self, record):
        if self.use_color:
            # Save the original levelname
            levelname = record.levelname

            # Add color and prefix
            color = self.COLORS.get(levelname, '')
            prefix = self.PREFIXES.get(levelname, '')

            # Format the message with color
            record.levelname = f"{color}{prefix}{levelname}{Style.RESET_ALL}"
            record.msg = f"{color}{record.msg}{Style.RESET_ALL}"

        # Call parent formatter
        result = super().format(record)

        return result


class Log:
    #format_string = "%(asctime)s: %(filename)s:%(funcName)s:%(lineno)s: %(levelname)s: %(message)s"

    LOGGER_NAME = "pyimport"
    FORMAT_STRING = "%(message)s"
    log = logging.getLogger(LOGGER_NAME)

    # Initialize colorama for Windows support
    colorama_init(autoreset=True)

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
    def formatter(use_color=True) -> logging.Formatter:
        """
        Create a formatter with optional color support.

        Args:
            use_color: Enable colored output (default: True)
        """
        if use_color:
            return ColoredFormatter(Log.FORMAT_STRING)
        else:
            return logging.Formatter(Log.FORMAT_STRING)

    @staticmethod
    def add_null_hander():
        log = logging.getLogger(Log.LOGGER_NAME)
        log.addHandler(logging.NullHandler())
        return log

    @staticmethod
    def add_stream_handler(log_level=None, use_color=True):
        """
        Add a stream handler with optional color support.

        Args:
            log_level: Logging level (default: INFO)
            use_color: Enable colored output (default: True)
        """
        sh = logging.StreamHandler()
        sh.setFormatter(Log.formatter(use_color=use_color))
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

