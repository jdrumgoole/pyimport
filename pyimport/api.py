"""
PyImport Python API

A clean, programmatic interface for importing CSV files to MongoDB using PyImport.
This API provides a simple way for third-party developers to use PyImport functionality
in their Python applications without dealing with command-line arguments.

Basic Usage:
    from pyimport.api import PyImportAPI

    # Simple import
    api = PyImportAPI()
    result = api.import_csv("data.csv", database="mydb", collection="mycol")
    print(f"Imported {result.total_written} records in {result.elapsed_duration}")

    # With field file generation
    api = PyImportAPI()
    field_file = api.generate_field_file("data.csv")
    result = api.import_csv("data.csv", field_file=field_file)

    # Advanced usage with all options
    api = PyImportAPI(
        mongodb_uri="mongodb://localhost:27017",
        database="mydb",
        collection="mycol"
    )
    result = api.import_csv(
        "data.csv",
        delimiter=",",
        has_header=True,
        batch_size=1000,
        parallel_mode="multi",
        pool_size=4
    )

@author: Claude Code
"""

import logging
from typing import Optional, List, Literal, Dict, Any
from pathlib import Path
import argparse

from pyimport.fieldfile import FieldFile, FieldFileException
from pyimport.mdbimportcmd import MDBImportCommand
from pyimport.asyncimport import AsyncMDBImportCommand
from pyimport.multiimportcommand import MultiImportCommand
from pyimport.threadimportcommand import ThreadImportCommand
from pyimport.importresult import ImportResult, ImportResults
from pyimport.logger import Log
from pyimport.audit import Audit


class PyImportAPI:
    """
    Main API class for programmatic access to PyImport functionality.

    This class provides a clean, Pythonic interface for importing CSV files
    to MongoDB without requiring command-line argument parsing.

    Attributes:
        mongodb_uri (str): MongoDB connection URI
        database (str): Default database name
        collection (str): Default collection name
        write_concern (int): MongoDB write concern (0-majority)
        journal (bool): Enable journaling
        fsync (bool): Force fsync on writes
    """

    def __init__(
        self,
        mongodb_uri: str = "mongodb://localhost:27017",
        database: str = "PYIM",
        collection: str = "imported",
        write_concern: int = 0,
        journal: bool = False,
        fsync: bool = False,
        log_level: str = "INFO",
        use_color: bool = True
    ):
        """
        Initialize PyImport API with connection settings.

        Args:
            mongodb_uri: MongoDB connection string
            database: Default database name
            collection: Default collection name
            write_concern: Write concern level (0 for fast, 1+ for acknowledged)
            journal: Enable write operation journaling
            fsync: Force disk sync on writes
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            use_color: Enable colorized log output
        """
        self.mongodb_uri = mongodb_uri
        self.database = database
        self.collection = collection
        self.write_concern = write_concern
        self.journal = journal
        self.fsync = fsync
        self.log_level = log_level
        self.use_color = use_color

        # Configure logging
        Log.set_level(log_level)
        if not Log().log.handlers:
            Log.add_stream_handler(log_level=log_level, use_color=use_color)
        self._log = Log().log

    def _create_args_namespace(
        self,
        filenames: List[str],
        database: Optional[str] = None,
        collection: Optional[str] = None,
        delimiter: str = ",",
        has_header: bool = False,
        field_file: Optional[str] = None,
        batch_size: int = 500,
        add_filename: bool = False,
        add_timestamp: bool = False,
        add_field: Optional[List[str]] = None,
        id_field: Optional[str] = None,
        noenrich: bool = False,
        cut: Optional[List[int]] = None,
        parallel_mode: Optional[Literal["multi", "threads", "async"]] = None,
        pool_size: int = 8,
        audit_host: Optional[str] = None,
        restart: bool = False,
        batch_id: Optional[str] = None,
        **kwargs
    ) -> argparse.Namespace:
        """
        Create an argparse.Namespace object with the specified parameters.
        This mimics command-line argument parsing for internal use.
        """
        args = argparse.Namespace()

        # Files
        args.filenames = filenames if isinstance(filenames, list) else [filenames]
        args.filelist = None

        # Database settings
        args.mdburi = self.mongodb_uri
        args.database = database or self.database
        args.collection = collection or self.collection
        args.writeconcern = self.write_concern
        args.journal = self.journal
        args.fsync = self.fsync

        # CSV parsing
        args.delimiter = delimiter
        args.hasheader = has_header
        args.fieldfile = field_file  # Note: importcmd.py checks for 'fieldfile' attribute
        args.fieldinfo = field_file  # Also set fieldinfo for compatibility
        args.batchsize = batch_size

        # Enrichment
        args.addfilename = add_filename
        # Convert boolean to DocTimeStamp enum
        if add_timestamp:
            from pyimport.doctimestamp import DocTimeStamp
            args.addtimestamp = DocTimeStamp.DOC_TIMESTAMP
        else:
            from pyimport.doctimestamp import DocTimeStamp
            args.addtimestamp = DocTimeStamp.NO_TIMESTAMP
        # addfield expects a single string or None (not a list)
        # If multiple fields provided, we need to process them one by one in the command
        # For now, take first one if list is provided
        if add_field and len(add_field) > 0:
            args.addfield = add_field[0] if isinstance(add_field, list) else add_field
        else:
            args.addfield = None
        args.idfield = id_field
        args.noenrich = noenrich
        args.cut = cut

        # Parallel processing
        args.multi = parallel_mode == "multi"
        args.threads = parallel_mode == "threads"
        args.asyncpro = parallel_mode == "async"
        args.poolsize = pool_size
        args.threadcount = pool_size

        # Audit and restart
        args.audit = audit_host is not None
        args.audithost = audit_host
        args.restart = restart
        args.batch_id = batch_id

        # Other options
        args.drop = False
        args.genfieldfile = False
        args.splitfile = False
        args.keepsplits = False
        args.loglevel = self.log_level
        args.silent = False
        args.no_color = not self.use_color
        args.argsource = False
        args.onerror = "warn"
        args.locator = False
        args.checkpoint_interval = 10000
        args.version = False
        args.limit = 0  # 0 means read all records
        args.info = ""
        args.verbose = False
        args.input = False
        args.autosplit = 2
        args.splitsize = 1024 * 10
        args.forkmethod = "fork"
        # Postgres args (not used but needed for compatibility)
        args.pgtable = "imported"
        args.pguser = "postgres"
        args.pguri = "postgresql://localhost:5432/postgres"
        args.pgport = 5432
        args.pgdatabase = "postgres"
        args.pgpassword = None

        # Add any additional kwargs
        for key, value in kwargs.items():
            setattr(args, key, value)

        return args

    def import_csv(
        self,
        filename: str | List[str],
        database: Optional[str] = None,
        collection: Optional[str] = None,
        delimiter: str = ",",
        has_header: bool = False,
        field_file: Optional[str | FieldFile] = None,
        batch_size: int = 500,
        add_filename: bool = False,
        add_timestamp: bool = False,
        add_field: Optional[List[str]] = None,
        id_field: Optional[str] = None,
        noenrich: bool = False,
        cut: Optional[List[int]] = None,
        parallel_mode: Optional[Literal["multi", "threads", "async"]] = None,
        pool_size: int = 8,
        audit_host: Optional[str] = None,
        drop_collection: bool = False
    ) -> ImportResults:
        """
        Import CSV file(s) to MongoDB.

        Args:
            filename: Path to CSV file or list of paths
            database: Target database (overrides instance default)
            collection: Target collection (overrides instance default)
            delimiter: CSV field delimiter
            has_header: Whether CSV has header row
            field_file: Path to field file (.tff) or FieldFile object
            batch_size: Number of documents per batch insert
            add_filename: Add source filename to each document
            add_timestamp: Add import timestamp to each document
            add_field: List of "key:value" pairs to add to each document
            id_field: Field to use as MongoDB _id
            noenrich: Skip all document enrichment
            cut: List of column indices to exclude (0-based)
            parallel_mode: Enable parallel processing ("multi", "threads", or "async")
            pool_size: Number of parallel workers
            audit_host: MongoDB URI for audit tracking
            drop_collection: Drop collection before import

        Returns:
            ImportResults: Object containing import statistics and results

        Raises:
            FieldFileException: If field file is invalid
            OSError: If file cannot be read
            ValueError: If parameters are invalid

        Example:
            >>> api = PyImportAPI()
            >>> result = api.import_csv("data.csv", database="test", has_header=True)
            >>> print(f"Imported {result.total_written} records")
        """
        # Convert single filename to list
        filenames = filename if isinstance(filename, list) else [filename]

        # Validate files exist
        for fn in filenames:
            if not Path(fn).exists() and not fn.startswith("http"):
                raise OSError(f"File not found: {fn}")

        # Handle FieldFile object
        field_file_path = None
        if isinstance(field_file, FieldFile):
            # Save FieldFile to temporary location
            import tempfile
            fd, field_file_path = tempfile.mkstemp(suffix=".tff")
            import os
            os.close(fd)
            # TODO: FieldFile needs a save() method - for now use path
            self._log.warning("FieldFile object passed but saving not yet implemented")
        elif field_file:
            field_file_path = field_file

        # Create args namespace
        args = self._create_args_namespace(
            filenames=filenames,
            database=database,
            collection=collection,
            delimiter=delimiter,
            has_header=has_header,
            field_file=field_file_path,
            batch_size=batch_size,
            add_filename=add_filename,
            add_timestamp=add_timestamp,
            add_field=add_field,
            id_field=id_field,
            noenrich=noenrich,
            cut=cut,
            parallel_mode=parallel_mode,
            pool_size=pool_size,
            audit_host=audit_host
        )

        # Drop collection if requested
        if drop_collection:
            from pyimport.dropcommand import DropCollectionCommand
            DropCollectionCommand(args=args).drop()

        # Select appropriate import command based on parallel mode
        if parallel_mode == "multi":
            cmd = MultiImportCommand(args)
        elif parallel_mode == "threads":
            cmd = ThreadImportCommand(args)
        elif parallel_mode == "async":
            cmd = AsyncMDBImportCommand(args)
        else:
            cmd = MDBImportCommand(args)

        # Run import
        results = cmd.run()

        return results

    def generate_field_file(
        self,
        csv_filename: str,
        output_filename: Optional[str] = None,
        delimiter: str = ",",
        has_header: bool = True,
        extension: str = ".tff"
    ) -> FieldFile:
        """
        Generate a field file (.tff) from a CSV file by analyzing the header and first row.

        Args:
            csv_filename: Path to CSV file
            output_filename: Output field file path (auto-generated if None)
            delimiter: CSV delimiter
            has_header: Whether CSV has header row
            extension: Field file extension (default: .tff)

        Returns:
            FieldFile: Generated field file object

        Raises:
            OSError: If CSV file cannot be read
            ValueError: If CSV structure is invalid

        Example:
            >>> api = PyImportAPI()
            >>> ff = api.generate_field_file("data.csv")
            >>> print(f"Generated field file with {len(ff.fields())} fields")
        """
        if not Path(csv_filename).exists() and not csv_filename.startswith("http"):
            raise OSError(f"File not found: {csv_filename}")

        field_file = FieldFile.generate_field_file(
            csv_filename=csv_filename,
            ff_filename=output_filename,
            ext=extension,
            delimiter=delimiter,
            has_header=has_header
        )

        self._log.info(f"Generated field file with {len(field_file.fields())} fields")

        return field_file

    def load_field_file(self, filename: str) -> FieldFile:
        """
        Load an existing field file (.tff).

        Args:
            filename: Path to field file

        Returns:
            FieldFile: Loaded field file object

        Raises:
            OSError: If file doesn't exist
            FieldFileException: If file is invalid

        Example:
            >>> api = PyImportAPI()
            >>> ff = api.load_field_file("data.tff")
            >>> fields = ff.fields()
        """
        return FieldFile.load(filename)

    def drop_collection(
        self,
        database: Optional[str] = None,
        collection: Optional[str] = None
    ) -> None:
        """
        Drop a MongoDB collection.

        Args:
            database: Database name (uses instance default if None)
            collection: Collection name (uses instance default if None)

        Example:
            >>> api = PyImportAPI()
            >>> api.drop_collection(database="test", collection="mycol")
        """
        from pyimport.dropcommand import DropCollectionCommand

        args = self._create_args_namespace(
            filenames=[],
            database=database,
            collection=collection
        )
        args.drop = True

        DropCollectionCommand(args=args).drop()
        self._log.info(f"Dropped collection {args.database}.{args.collection}")

    def restart_import(
        self,
        batch_id: Optional[str] = None,
        audit_host: Optional[str] = None,
        **import_kwargs
    ) -> ImportResults:
        """
        Restart a previously incomplete import using audit tracking.

        Args:
            batch_id: Batch ID to restart (auto-detects last incomplete if None)
            audit_host: MongoDB URI for audit database
            **import_kwargs: Additional arguments to pass to import_csv()

        Returns:
            ImportResults: Results of resumed import

        Raises:
            ValueError: If audit is not configured or no incomplete batch found

        Example:
            >>> api = PyImportAPI()
            >>> result = api.restart_import(batch_id="20231101_123456")
        """
        if not audit_host:
            raise ValueError("audit_host is required for restart functionality")

        import_kwargs['audit_host'] = audit_host
        import_kwargs['restart'] = True
        import_kwargs['batch_id'] = batch_id

        return self.import_csv(**import_kwargs)

    def get_audit_status(
        self,
        audit_host: str,
        batch_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get audit status for a batch or list incomplete batches.

        Args:
            audit_host: MongoDB URI for audit database
            batch_id: Specific batch ID to check (lists all incomplete if None)

        Returns:
            dict: Audit status information

        Example:
            >>> api = PyImportAPI()
            >>> status = api.get_audit_status("mongodb://localhost:27017")
            >>> print(status['incomplete_batches'])
        """
        audit = Audit(audit_host)

        if batch_id:
            completed_files = audit.get_completed_files(batch_id)
            return {
                'batch_id': batch_id,
                'completed_files': completed_files,
                'completed_count': len(completed_files)
            }
        else:
            incomplete = audit.get_last_incomplete_batch()
            return {
                'last_incomplete_batch': incomplete,
                'has_incomplete': incomplete is not None
            }


class PyImportBuilder:
    """
    Fluent builder interface for PyImport operations.

    Provides a more expressive, chainable API for configuring imports.

    Example:
        >>> from pyimport.api import PyImportBuilder
        >>>
        >>> result = (PyImportBuilder()
        ...     .connect("mongodb://localhost:27017")
        ...     .database("mydb")
        ...     .collection("mycol")
        ...     .csv_file("data.csv")
        ...     .delimiter(",")
        ...     .has_header(True)
        ...     .batch_size(1000)
        ...     .add_timestamp()
        ...     .parallel("multi", workers=4)
        ...     .import_data())
        >>>
        >>> print(f"Imported {result.total_written} records")
    """

    def __init__(self):
        self._mongodb_uri = "mongodb://localhost:27017"
        self._database = "PYIM"
        self._collection = "imported"
        self._filenames = []
        self._delimiter = ","
        self._has_header = False
        self._field_file = None
        self._batch_size = 500
        self._add_filename = False
        self._add_timestamp = False  # Boolean flag for API simplicity
        self._add_field = None
        self._id_field = None
        self._noenrich = False
        self._cut = None
        self._parallel_mode = None
        self._pool_size = 8
        self._audit_host = None
        self._drop_collection = False
        self._write_concern = 0
        self._journal = False
        self._fsync = False
        self._log_level = "INFO"
        self._use_color = True

    def connect(self, mongodb_uri: str) -> "PyImportBuilder":
        """Set MongoDB connection URI."""
        self._mongodb_uri = mongodb_uri
        return self

    def database(self, name: str) -> "PyImportBuilder":
        """Set target database name."""
        self._database = name
        return self

    def collection(self, name: str) -> "PyImportBuilder":
        """Set target collection name."""
        self._collection = name
        return self

    def csv_file(self, filename: str | List[str]) -> "PyImportBuilder":
        """Add CSV file(s) to import."""
        if isinstance(filename, list):
            self._filenames.extend(filename)
        else:
            self._filenames.append(filename)
        return self

    def delimiter(self, delim: str) -> "PyImportBuilder":
        """Set CSV delimiter."""
        self._delimiter = delim
        return self

    def has_header(self, value: bool = True) -> "PyImportBuilder":
        """Set whether CSV has header row."""
        self._has_header = value
        return self

    def field_file(self, path: str | FieldFile) -> "PyImportBuilder":
        """Set field file for type conversion."""
        self._field_file = path
        return self

    def batch_size(self, size: int) -> "PyImportBuilder":
        """Set batch size for bulk inserts."""
        self._batch_size = size
        return self

    def add_filename(self, value: bool = True) -> "PyImportBuilder":
        """Add source filename to each document."""
        self._add_filename = value
        return self

    def add_timestamp(self, value: bool = True) -> "PyImportBuilder":
        """Add import timestamp to each document."""
        self._add_timestamp = value
        return self

    def add_field(self, key: str, value: str) -> "PyImportBuilder":
        """Add custom field to each document."""
        if self._add_field is None:
            self._add_field = []
        self._add_field.append(f"{key}={value}")  # Use = separator, not :
        return self

    def id_field(self, field: str) -> "PyImportBuilder":
        """Set field to use as MongoDB _id."""
        self._id_field = field
        return self

    def no_enrich(self, value: bool = True) -> "PyImportBuilder":
        """Disable document enrichment."""
        self._noenrich = value
        return self

    def exclude_columns(self, *indices: int) -> "PyImportBuilder":
        """Exclude columns by index (0-based)."""
        self._cut = list(indices)
        return self

    def parallel(
        self,
        mode: Literal["multi", "threads", "async"],
        workers: int = 8
    ) -> "PyImportBuilder":
        """Enable parallel processing."""
        self._parallel_mode = mode
        self._pool_size = workers
        return self

    def audit(self, audit_host: str) -> "PyImportBuilder":
        """Enable audit tracking."""
        self._audit_host = audit_host
        return self

    def drop_first(self, value: bool = True) -> "PyImportBuilder":
        """Drop collection before import."""
        self._drop_collection = value
        return self

    def write_concern(self, level: int) -> "PyImportBuilder":
        """Set MongoDB write concern."""
        self._write_concern = level
        return self

    def journal(self, value: bool = True) -> "PyImportBuilder":
        """Enable journaling."""
        self._journal = value
        return self

    def fsync(self, value: bool = True) -> "PyImportBuilder":
        """Force fsync on writes."""
        self._fsync = value
        return self

    def log_level(self, level: str) -> "PyImportBuilder":
        """Set logging level."""
        self._log_level = level
        return self

    def color(self, value: bool = True) -> "PyImportBuilder":
        """Enable colorized output."""
        self._use_color = value
        return self

    def import_data(self) -> ImportResults:
        """
        Execute the import with configured settings.

        Returns:
            ImportResults: Import results and statistics
        """
        if not self._filenames:
            raise ValueError("No CSV files specified. Use csv_file() to add files.")

        api = PyImportAPI(
            mongodb_uri=self._mongodb_uri,
            database=self._database,
            collection=self._collection,
            write_concern=self._write_concern,
            journal=self._journal,
            fsync=self._fsync,
            log_level=self._log_level,
            use_color=self._use_color
        )

        return api.import_csv(
            filename=self._filenames,
            delimiter=self._delimiter,
            has_header=self._has_header,
            field_file=self._field_file,
            batch_size=self._batch_size,
            add_filename=self._add_filename,
            add_timestamp=self._add_timestamp,
            add_field=self._add_field,
            id_field=self._id_field,
            noenrich=self._noenrich,
            cut=self._cut,
            parallel_mode=self._parallel_mode,
            pool_size=self._pool_size,
            audit_host=self._audit_host,
            drop_collection=self._drop_collection
        )
