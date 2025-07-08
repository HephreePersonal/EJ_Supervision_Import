"""Simplified secure Financial database import."""

import logging
from typing import Any

from utils.logging_helper import setup_logging, operation_counts
from utils.etl_helpers import transaction_scope
from etl.secure_base_importer import SecureBaseDBImporter

logger = logging.getLogger(__name__)


class SecureFinancialDBImporter(SecureBaseDBImporter):
    DB_TYPE = "Financial"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Financial.txt"
    DEFAULT_CSV_FILE = "EJ_Financial_Selects_ALL.csv"

    def execute_preprocessing(self, conn: Any) -> None:
        logger.info("Defining supervision scope for Financial DB")
        steps = [("GatherFeeInstanceIDs", "financial/gather_feeinstanceids.sql")]
        with transaction_scope(conn):
            for name, script in steps:
                self.run_sql_file(conn, name, script)
        logger.info("Financial preprocessing complete")

    def prepare_drop_and_select(self, conn: Any) -> None:
        logger.info("Gathering list of Financial tables")
        self.run_sql_file(
            conn,
            "gather_drops_and_selects_financial",
            "financial/gather_drops_and_selects_financial.sql",
        )

    def update_joins_in_tables(self, conn: Any) -> None:
        logger.info("Updating JOINS in TablesToConvert")
        self.run_sql_file(conn, "update_joins_financial", "financial/update_joins_financial.sql")
        logger.info("Updating JOINS for Financial tables is complete")

    def get_next_step_name(self) -> str:
        return "LOB Columns"


def main() -> None:
    setup_logging()
    importer = SecureFinancialDBImporter()
    importer.run()
    logger.info(
        "Run completed - successes: %s failures: %s",
        operation_counts["success"],
        operation_counts["failure"],
    )


if __name__ == "__main__":
    main()
