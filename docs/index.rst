.. Pyimport documentation master file

PyImport Documentation
======================

PyImport is a powerful Python command-line tool for importing CSV data into MongoDB with automatic type detection, parallel processing, and graceful handling of "dirty" data.

Features
--------

* **Automatic type detection** - Infers field types from CSV data
* **Multiple execution strategies** - Sync, async, multi-process, and threaded imports
* **Parallel processing** - Split large files and import in parallel for maximum throughput
* **Graceful error handling** - Falls back to strings on type conversion errors
* **Flexible date parsing** - Supports multiple date formats with fast ISO date parsing
* **Restart capability** - Resume failed imports from where they left off
* **Performance optimized** - Recent improvements provide 20-35% faster imports

Quick Start
-----------

.. code-block:: bash

   # Generate field file
   pyimport --genfieldfile data.csv

   # Import to MongoDB
   pyimport --database mydb --collection mycol data.csv

   # Fast parallel import
   pyimport --multi --splitfile --autosplit 8 --poolsize 4 data.csv

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   markdown/introduction
   markdown/installation
   markdown/quickstart
   API
   markdown/cli_reference
   markdown/fieldfiles
   markdown/advanced

Typical Performance
-------------------

* **Sync**: ~24,000-32,000 docs/sec
* **Async**: ~30,000-40,000 docs/sec
* **Multi-process**: ~50,000+ docs/sec

Installation
------------

.. code-block:: bash

   pip install pyimport

Requirements
------------

* Python 3.11+
* MongoDB 4.0+

Source Code
-----------

* GitHub: https://github.com/jdrumgoole/pyimport
* PyPI: https://pypi.org/project/pyimport/

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
