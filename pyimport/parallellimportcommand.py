import asyncio
import os

from pyimport.asyncimport import AsyncMDBImportCommand
from pyimport.mdbimportcmd import MDBImportCommand
from pyimport.importresult import ImportResult, ImportResults


class ParallelMDBImportCommand(MDBImportCommand):

    def __init__(self, args):
        super().__init__(args)

    @staticmethod
    def async_processor(args, log, filename: str, audit_params=None, batch_id=None):
        """
        Process a file asynchronously.

        Args:
            audit_params: Dict with 'host', 'database', 'collection' keys (or None)
        """
        # Recreate audit object in worker process to avoid pickling issues
        from pyimport.asyncaudit import AsyncAudit
        audit = None
        if audit_params:
            # AsyncAudit needs a database object, would need refactoring
            # For now, skip audit in async processor
            pass

        if not os.path.isfile(filename):
            log.warning(f"No such file: '{filename}' ignoring")
            return ImportResult.import_error(filename, "No such file")
        else:
            return asyncio.run(AsyncMDBImportCommand.process_one_file(args, log, filename,
                                                                       audit=audit, batch_id=batch_id))

    @staticmethod
    def sync_processor(args, log, filename: str, audit_params=None, batch_id=None):
        """
        Process a file synchronously.

        Args:
            audit_params: Dict with 'host', 'database', 'collection' keys (or None)
        """
        # Recreate audit object in worker process/thread to avoid pickling issues
        from pyimport.audit import Audit
        audit = None
        if audit_params:
            audit = Audit(
                host=audit_params['host'],
                database_name=audit_params['database'],
                collection_name=audit_params['collection']
            )

        if not os.path.isfile(filename):
            log.warning(f"No such file: '{filename}' ignoring")
            return ImportResult.error(filename, "No such file")
        else:
            return MDBImportCommand.process_one_file(args, log, filename,
                                                    audit=audit, batch_id=batch_id)

