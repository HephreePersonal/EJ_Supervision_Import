"""Simplified secure Justice database import.

This version uses the hardened ``SecureBaseDBImporter`` and is intended for
production use. See ``README_SECURE_IMPLEMENTATION.md`` for migration steps and
additional security notes.
"""

import logging
from typing import Any

from utils.logging_helper import setup_logging, operation_counts
from utils.etl_helpers import transaction_scope
from etl.secure_base_importer import SecureBaseDBImporter
from utils.etl_helpers import load_sql

logger = logging.getLogger(__name__)


class SecureJusticeDBImporter(SecureBaseDBImporter):
    DB_TYPE = "Justice"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Justice.txt"
    DEFAULT_CSV_FILE = "EJ_Justice_Selects_ALL.csv"

    def execute_preprocessing(self, conn: Any) -> None:
        """Run SQL scripts that define the Justice supervision scope."""
        logger.info("Defining supervision scope for Justice DB")
        steps = [
            ("GatherCaseIDs", "justice/gather_caseids.sql"),
            ("GatherChargeIDs", "justice/gather_chargeids.sql"),
            ("GatherPartyIDs", "justice/gather_partyids.sql"),
            ("GatherWarrantIDs", "justice/gather_warrantids.sql"),
            ("GatherHearingIDs", "justice/gather_hearingids.sql"),
            ("GatherEventIDs", "justice/gather_eventids.sql"),
        ]
        with transaction_scope(conn):
            for name, script in steps:
                self.run_sql_file(conn, name, script)
        logger.info("Justice preprocessing complete")

    def prepare_drop_and_select(self, conn: Any) -> None:
        """Generate DROP and SELECT statements for the Justice tables."""
        logger.info("Gathering list of Justice tables")
        self.run_sql_file(conn, "gather_drops_and_selects", "justice/gather_drops_and_selects.sql")

    def update_joins_in_tables(self, conn: Any) -> None:
        """Insert JOIN statements into the tracking table."""
        logger.info("Updating JOINS in TablesToConvert")
        self.run_sql_file(conn, "update_joins", "justice/update_joins.sql")
        logger.info("Updating JOINS for Justice tables is complete")

    def get_next_step_name(self) -> str:
        """Return the next importer to run after this one."""
        return "Operations migration"


def main() -> None:
    """Entry point for running the secure Justice importer from the CLI."""
    setup_logging()
    importer = SecureJusticeDBImporter()
    importer.run()
    logger.info(
        "Run completed - successes: %s failures: %s",
        operation_counts["success"],
        operation_counts["failure"],
    )


if __name__ == "__main__":
    main()
