'''Parsl
========

Parallel Scripting Library, designed to enable efficient workflow execution.

Importing
---------

To get all the required functionality, we suggest importing the library as follows:

>>> import parsl
>>> from parsl import *

Logging
-------

Following the general logging philosophy of python libraries, by default `Parsl <https://github.com/swift-lang/swift-e-lab/>`_
doesn't log anything. However the following helper functions are provided for logging:

1. set_stream_logger
    This sets the logger to the StreamHandler. This is quite useful when working from
    a Jupyter notebook.

2. set_file_logger
    This sets the logging to a file. This is ideal for reporting issues to the dev team.

'''

print("Version 0.1.1")
from parsl.app.app import APP, App
from parsl.app.executors import ThreadPoolExecutor, ProcessPoolExecutor
import logging
import parsl.app.errors
from parsl.dataflow.dflow import DataFlowKernel

__author__  = 'Yadu Nand Babuji'
__version__ = '0.1.0'

__all__ = ['App', 'DataFlowKernel', 'ThreadPoolExecutor', 'ProcessPoolExecutor']

def set_stream_logger(name='parsl', level=logging.DEBUG, format_string=None):
    ''' Add a stream log handler

    Args:
        name (string) : Set the logger name
        level (logging.LEVEL) : Set to logging.DEBUG by default
        format_string (sting) : Set to None by default

    Returns:
        None
    '''

    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_file_logger(filename, name='parsl', level=logging.DEBUG, format_string=None):
    ''' Add a stream log handler

    Args:
        filename (string) : Name of the file to write logs to
        name (string) : Logger name
        level (loggig.LEVEL) : Set the logging level.
        format_string (string) : Set the format string

    Returns:
        None
    '''

    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class NullHandler (logging.Handler):
    ''' Setup default logging to /dev/null since this is library.
    '''
    def emit(self, record):
        pass

logging.getLogger('parsl').addHandler(NullHandler())
