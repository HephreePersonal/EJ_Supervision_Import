"""Simplified secure Operations database import.

Uses ``SecureBaseDBImporter`` for hardened execution. Refer to
``README_SECURE_IMPLEMENTATION.md`` for usage guidance and security
considerations.
"""

import logging
from typing import Any

from utils.logging_helper import setup_logging, operation_counts
from utils.etl_helpers import transaction_scope
from etl.secure_base_importer import SecureBaseDBImporter

logger = logging.getLogger(__name__)


class SecureOperationsDBImporter(SecureBaseDBImporter):
    DB_TYPE = "Operations"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Operations.txt"
    DEFAULT_CSV_FILE = "EJ_Operations_Selects_ALL.csv"

    def execute_preprocessing(self, conn: Any) -> None:
        """Execute scripts to collect Operations DB scope information."""
        logger.info("Defining document conversion scope for Operations DB")
        steps = [("GatherDocumentIDs", "operations/gather_documentids.sql")]
        with transaction_scope(conn):
            for name, script in steps:
                self.run_sql_file(conn, name, script)
        logger.info("Operations preprocessing complete")

    def prepare_drop_and_select(self, conn: Any) -> None:
        """Generate DROP and SELECT statements for Operations tables."""
        logger.info("Gathering list of Operations tables")
        self.run_sql_file(
            conn,
            "gather_drops_and_selects_operations",
            "operations/gather_drops_and_selects_operations.sql",
        )

    def update_joins_in_tables(self, conn: Any) -> None:
        """Insert JOIN statements into the tracking table."""
        logger.info("Updating JOINS in TablesToConvert")
        self.run_sql_file(conn, "update_joins", "operations/update_joins_operations.sql")
        logger.info("Updating JOINS for Operations tables is complete")

    def get_next_step_name(self) -> str:
        """Return the next importer to run after this one."""
        return "Financial migration"


def main() -> None:
    """Entry point for running the secure Operations importer from the CLI."""
    setup_logging()
    importer = SecureOperationsDBImporter()
    importer.run()
    logger.info(
        "Run completed - successes: %s failures: %s",
        operation_counts["success"],
        operation_counts["failure"],
    )


if __name__ == "__main__":
    main()
