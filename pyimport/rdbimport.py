from sqlalchemy.exc import SQLAlchemyError

from pyimport.db.rdbmanager import RDBManager
from pyimport.importcmd import ImportCommand


class RDBImportCommand(ImportCommand):
    def __init__(self, args):
        super().__init__(args)

    @staticmethod
    def prep_sql_database(args) -> RDBManager:
        # Check if pguri is provided
        if args.pguri:
            db_url = args.pguri
        else:
            # Check individual components and raise specific ValueErrors
            if not args.pguser:
                raise ValueError("pguser is required")
            if not args.pgpassword:
                raise ValueError("pgpassword is required")
            if not args.pgdatabase:
                raise ValueError("pgdatabase is required")

            # Build URL from individual components
            db_url = f"postgresql://{args.pguser}:{args.pgpassword}@localhost/{args.pgdatabase}"

        # Create a SQLAlchemy engine and metadata object
        try:
            rdb_mgr = RDBManager(db_url)
            if not rdb_mgr.is_database(db_url):
                rdb_mgr.create_database(db_url)

            return rdb_mgr
        except SQLAlchemyError as e:
            raise ConnectionError(f"Failed to connect to the database: {e}")
