"""ETL script to migrate Operations database tables.

The importer executes SQL from ``sql_scripts/operations`` against the database
configured via ``MSSQL_TARGET_CONN_STR``. Command line options mirror those of
the Justice importer. Consult ``README.md`` for ``Quick Start`` instructions
and the overall ``ETL Process Flow``.
"""

import logging
from utils.logging_helper import setup_logging, operation_counts
import time
import json
import sys
import os
import argparse
from typing import Any
from dotenv import load_dotenv
import pandas as pd
import urllib
import sqlalchemy
from db.connections import get_target_connection
from etl import core
from etl.base_importer import BaseDBImporter
from tqdm import tqdm
from sqlalchemy.types import Text
import tkinter as tk
from tkinter import N, messagebox
from config.settings import settings, parse_database_name

from utils.etl_helpers import (
    log_exception_to_file,
    load_sql,
    run_sql_step,
    run_sql_script,
    transaction_scope,
)

logger = logging.getLogger(__name__)

DEFAULT_LOG_FILE = "PreDMSErrorLog_Operations.txt"

# Determine the target database name from environment variables/connection
# string. This value replaces the ``{{DB_NAME}}`` placeholder in the SQL
# scripts so the ETL can run against any target database.
conn_val = settings.mssql_target_conn_str if settings.mssql_target_conn_str else None
DB_NAME = settings.mssql_target_db_name or parse_database_name(conn_val)

class OperationsDBImporter(BaseDBImporter):
    """Operations database import implementation."""
    
    DB_TYPE = "Operations"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Operations.txt"
    DEFAULT_CSV_FILE = "EJ_Operations_Selects_ALL.csv"
    
    def parse_args(self) -> argparse.Namespace:
        """Parse command line arguments for the Operations DB import script.

        Example usage can be found in ``README.md`` under ``Quick Start``.
        """
        parser = argparse.ArgumentParser(description="Operations DB Import ETL Process")
        parser.add_argument(
            "--log-file",
            help="Path to the error log file. Overrides the EJ_LOG_DIR environment variable."
        )
        parser.add_argument(
            "--csv-file",
            help="Path to the Operations Selects CSV file. Overrides the EJ_CSV_DIR environment variable."
        )
        parser.add_argument(
            "--include-empty", 
            action="store_true",
            help="Include empty tables in the migration process."
        )
        parser.add_argument(
            "--skip-pk-creation",
            action="store_true",
            help="Skip primary key and constraint creation step."
        )
        parser.add_argument(
            "--csv-chunk-size",
            type=int,
            help="Number of rows per chunk when reading the CSV file."
        )
        parser.add_argument(
            "--config-file",
            default="config/secure_config.json",
            help="Path to JSON configuration file with all settings."
        )
        parser.add_argument(
            "--verbose", "-v",
            action="store_true",
            help="Enable verbose logging."
        )
        parser.add_argument(
            "--extra-validation",
            action="store_true",
            help="Enable extra SQL validation checks"
        )
        return parser.parse_args()
        
    def execute_preprocessing(self, conn: Any) -> None:
        """Define supervision scope for Operations DB."""
        logger.info("Defining supervision scope...")
        steps = [("GatherDocumentIDs", "operations/gather_documentids.sql")]

        with transaction_scope(conn):
            for name, script in core.safe_tqdm(steps, desc="SQL Script Progress", unit="step"):
                self.run_sql_file(conn, name, script)
        
        logger.info("All Staging steps completed successfully. Document Conversion Scope Defined.")
    
    def prepare_drop_and_select(self, conn: Any) -> None:
        """Prepare SQL statements for dropping and selecting data."""
        logger.info("Gathering list of Operations tables with SQL Commands to be migrated.")
        self.run_sql_file(
            conn,
            "gather_drops_and_selects_operations",
            "operations/gather_drops_and_selects_operations.sql",
        )
    
    def update_joins_in_tables(self, conn: Any) -> None:
        """Update the TablesToConvert table with JOINs."""
        logger.info("Updating JOINS in TablesToConvert List")
        self.run_sql_file(conn, "update_joins", "operations/update_joins_operations.sql")
        logger.info("Updating JOINS for Operations tables is complete.")
    
    def get_next_step_name(self) -> str:
        """Return the name of the next step in the ETL process."""
        return "Financial migration"

def main():
    """Main entry point for Operations DB Import."""
    setup_logging()
    load_dotenv()
    importer = OperationsDBImporter()
    importer.run()
    logger.info(
        "Run completed - successes: %s failures: %s",
        operation_counts["success"],
        operation_counts["failure"],
    )

if __name__ == "__main__":
    main()
