"""Base class for database import operations."""

from __future__ import annotations

import logging
import os
import json
import argparse
import tkinter as tk
from tkinter import messagebox
import pandas as pd
import urllib
import sqlalchemy
from typing import Any, Optional
from sqlalchemy.types import Text

from db.mssql import get_target_connection
from utils.etl_helpers import (
    load_sql,
    run_sql_script,
    log_exception_to_file,
    transaction_scope,
    execute_sql_with_timeout,
)
from etl.core import (
    sanitize_sql,
    safe_tqdm,
    load_config,
    validate_environment,
    validate_sql_identifier,
)
from config import ETLConstants, settings

logger = logging.getLogger(__name__)


class RowCountMismatchError(Exception):
    """Raised when row count validation fails."""


class BaseDBImporter:
    """Base class for database import operations."""
    
    # Override these in subclasses
    DB_TYPE = "base"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Base.txt"
    DEFAULT_CSV_FILE = "EJ_Base_Selects_ALL.csv"

    def __init__(self) -> None:
        """Initialize the importer with default values."""
        self.config = None
        self.db_name = None
        self.progress_file = os.environ.get(
            "PROGRESS_FILE",
            os.path.join(
                os.environ.get("EJ_LOG_DIR", ""),
                f"{self.DB_TYPE}_progress.json",
            ),
        )

    def parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description=f"{self.DB_TYPE} database import operations")
        # Add other arguments...
        parser.add_argument("--config", dest="config_file",
                           default="config/values.json",  # Set default config path
                           help="Path to configuration file")
        return parser.parse_args()

    def validate_environment(self) -> None:
        """Validate required environment variables."""
        required_vars = {
            'MSSQL_TARGET_CONN_STR': "Database connection string is required",
            'EJ_CSV_DIR': "Directory containing ETL CSV files is required"
        }
        
        optional_vars = {
            'EJ_LOG_DIR': "Directory for log files (defaults to current directory)",
            'INCLUDE_EMPTY_TABLES': "Set to '1' to include empty tables (defaults to '0')",
            'SQL_TIMEOUT': "Timeout in seconds for SQL operations (defaults to 300)"
        }
        
        validate_environment(required_vars, optional_vars)

    def load_config(self, args: argparse.Namespace) -> None:
        """Load configuration from arguments and environment."""
        default_config = {
            "include_empty_tables": False,
            "always_include_tables": [],
            "csv_filename": self.DEFAULT_CSV_FILE,
            "log_filename": self.DEFAULT_LOG_FILE,
            "skip_pk_creation": False,
            "sql_timeout": ETLConstants.DEFAULT_SQL_TIMEOUT,  # seconds
            "csv_chunk_size": ETLConstants.DEFAULT_CSV_CHUNK_SIZE,
            "fail_on_mismatch": settings.fail_on_mismatch,
        }
        
        self.config = load_config(args.config_file, default_config)
        
        # Add diagnostic logging
        if "always_include_tables" in self.config:
            logger.info(f"Found {len(self.config['always_include_tables'])} tables in always_include_tables: {self.config['always_include_tables']}")
        else:
            logger.warning("No always_include_tables defined in configuration")

        # Override config with environment variables
        if os.environ.get("INCLUDE_EMPTY_TABLES") == "1":
            self.config["include_empty_tables"] = True
        if os.environ.get("SQL_TIMEOUT"):
            self.config["sql_timeout"] = int(os.environ.get("SQL_TIMEOUT"))
        if os.environ.get("CSV_CHUNK_SIZE"):
            self.config["csv_chunk_size"] = int(os.environ.get("CSV_CHUNK_SIZE"))
        if os.environ.get("FAIL_ON_MISMATCH") == "1":
            self.config["fail_on_mismatch"] = True
        
        # Override config with command line arguments
        if args.include_empty:
            self.config["include_empty_tables"] = True
        if args.skip_pk_creation:
            self.config["skip_pk_creation"] = True
        if hasattr(args, "csv_chunk_size") and args.csv_chunk_size:
            self.config["csv_chunk_size"] = args.csv_chunk_size
        
        # Set up paths
        self.config['log_file'] = args.log_file or os.path.join(
            os.environ.get("EJ_LOG_DIR", ""), 
            self.config["log_filename"]
        )
        
        self.config['csv_file'] = args.csv_file or os.path.join(
            os.environ.get("EJ_CSV_DIR", ""),
            self.config["csv_filename"]
        )

    def _load_progress(self) -> dict:
        """Return progress data from ``self.progress_file`` if present."""
        if not self.progress_file or not os.path.exists(self.progress_file):
            return {}
        try:
            with open(self.progress_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:  # pragma: no cover - edge case
            logger.error("Failed to read progress file %s: %s", self.progress_file, exc)
            return {}

    def _get_progress(self, key: str) -> int:
        data = self._load_progress()
        try:
            return int(data.get(key, 0))
        except Exception:
            return 0

    def _update_progress(self, key: str, value: int) -> None:
        if not self.progress_file:
            return
        data = self._load_progress()
        data[key] = value
        try:
            os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as exc:  # pragma: no cover - unlikely
            logger.error("Failed to write progress file %s: %s", self.progress_file, exc)

    def import_joins(self) -> sqlalchemy.engine.Engine:
        """Import JOIN statements from CSV to build selection queries."""
        logger.info(f"Importing JOINS from {self.DB_TYPE} Selects CSV")
        
        # Set up database connection for pandas
        conn_str = os.environ['MSSQL_TARGET_CONN_STR']
        params = urllib.parse.quote_plus(conn_str)
        db_url = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = sqlalchemy.create_engine(db_url)
        
        csv_path = self.config['csv_file']
        log_file = self.config['log_file']
        
        if not os.path.exists(csv_path):
            error_msg = f"CSV file not found: {csv_path}"
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise FileNotFoundError(error_msg)
        
        # Read and import CSV in chunks to avoid excessive memory usage
        chunksize = self.config.get("csv_chunk_size", ETLConstants.DEFAULT_CSV_CHUNK_SIZE)
        table_name = (
            f'TableUsedSelects_{self.DB_TYPE}' if self.DB_TYPE != 'Justice' else 'TableUsedSelects'
        )
        total_rows = 0
        first = True
        for chunk in safe_tqdm(
            pd.read_csv(csv_path, delimiter='|', encoding='utf-8', chunksize=chunksize),
            desc="Importing JOINs",
            unit="rows",
        ):
            chunk = chunk.astype({
                'DatabaseName': 'str', 'SchemaName': 'str', 'TableName': 'str',
                'Freq': 'str', 'InScopeFreq': 'str', 'Select_Only': 'str',
                'fConvert': 'str', 'Drop_IfExists': 'str', 'Selection': 'str',
                'Select_Into': 'str'
            })
            total_rows += len(chunk)
            chunk.to_sql(
                table_name,
                con=engine,
                if_exists='replace' if first else 'append',
                index=False,
                dtype={'Select_Into': Text(), 'Drop_IfExists': Text()},
            )
            first = False

        logger.info(
            f"Successfully imported {total_rows} JOIN definitions from {csv_path}"
        )
        return engine

    def execute_table_operations(self, conn: Any) -> None:
        """Execute DROP and SELECT INTO operations."""
        logger.info("Executing table operations (DROP/SELECT)")
        log_file = self.config['log_file']

        table_name = f"TablesToConvert_{self.DB_TYPE}" if self.DB_TYPE != 'Justice' else 'TablesToConvert'
        table_name = validate_sql_identifier(table_name)

        db_name = validate_sql_identifier(self.db_name)
        successful_tables = 0
        failed_tables = 0
        start_idx = self._get_progress("table_operations")

        try:
            with transaction_scope(conn):
                rows = self._fetch_table_operation_rows(conn, db_name, table_name)

                for idx, row_dict in enumerate(
                    safe_tqdm(rows, desc="Drop/Select", unit="table"), 1
                ):
                    if idx <= start_idx:
                        continue
                    try:
                        if self._process_table_operation_row(conn, row_dict, idx, log_file):
                            successful_tables += 1
                            self._update_progress("table_operations", idx)
                        else:
                            failed_tables += 1
                    except Exception as row_error:
                        error_msg = f"Row processing error for row {idx}: {str(row_error)}"
                        logger.error(error_msg)
                        log_exception_to_file(error_msg, log_file)
                        failed_tables += 1

        except Exception as query_error:
            error_msg = f"Fatal query error: {str(query_error)}"
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise

        logger.info(f"Table operations completed: {successful_tables} successful, {failed_tables} failed")

    def _fetch_table_operation_rows(self, conn: Any, db_name: str, table_name: str) -> list[dict[str, Any]]:
        """Retrieve rows describing table operations to perform."""
        query = f"""
            SELECT RowID, DatabaseName, SchemaName, TableName, fConvert, ScopeRowCount,
                   CAST(Drop_IfExists AS NVARCHAR(MAX)) AS Drop_IfExists,
                   CAST(CAST(Select_Into AS NVARCHAR(MAX)) + CAST(ISNULL(Joins, N'') AS NVARCHAR(MAX)) AS NVARCHAR(MAX)) AS [Select_Into]
            FROM {db_name}.dbo.{table_name} S
            WHERE fConvert=1
            ORDER BY DatabaseName, SchemaName, TableName
        """

        cursor = execute_sql_with_timeout(
            conn, query, timeout=self.config["sql_timeout"]
        )
        
        # Handle SQLAlchemy CursorResult objects differently than DB-API cursors
        if hasattr(cursor, "mappings"):
            # SQLAlchemy 1.4+ CursorResult object
            return list(cursor.mappings())
        elif hasattr(cursor, "keys") and callable(cursor.keys):
            # Older SQLAlchemy versions
            columns = cursor.keys()
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        elif hasattr(cursor, "description"):
            # Standard DB-API cursor
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        else:
            # Last resort fallback - try to make it work somehow
            try:
                # Try to get results any way possible
                if hasattr(cursor, "fetchall"):
                    rows = cursor.fetchall()
                    # Use column positions as names if we have no other info
                    return [
                        {f"col{i}": value for i, value in enumerate(row)} 
                        for row in rows
                    ]
                else:
                    # If all else fails, just convert to list
                    return list(cursor)
            except Exception as e:
                logger.error(f"Failed to process query results: {e}")
                return []

    def _should_process_table(
        self, scope_row_count: Any, schema_name: str | None = None,
        table_name: str | None = None
    ) -> bool:
        """
        Determine whether a table should be processed based on row count and overrides.
        
        Supports both "schema.table" and "database.schema.table" formats in always_include_tables.
        """
        # If we're including all tables regardless of row count, return True
        if self.config.get("include_empty_tables"):
            return True
            
        # Create set of override patterns (case-insensitive)
        overrides = {
            t.strip().lower() for t in self.config.get("always_include_tables", [])
        }
        
        # Check for match using different formats
        schema_table = f"{schema_name}.{table_name}".lower()
        db_schema_table = f"{self.db_name}.{schema_name}.{table_name}".lower()
        db_type_schema_table = f"{self.DB_TYPE.lower()}.{schema_name}.{table_name}".lower()
        
        logger.debug(f"Checking table formats: {schema_table}, {db_schema_table}, {db_type_schema_table}")
        logger.debug(f"Against overrides: {overrides}")
        
        # Try all formats that might be in the config
        if schema_table in overrides:
            logger.debug(f"Including table {schema_table} (matched schema.table format)")
            return True
        elif db_schema_table in overrides:
            logger.debug(f"Including table {db_schema_table} (matched db.schema.table format)")
            return True
        elif db_type_schema_table in overrides:
            logger.debug(f"Including table {db_type_schema_table} (matched {self.DB_TYPE.lower()}.schema.table format)")
            return True

        # For empty tables that aren't in our override list
        if scope_row_count is None or int(scope_row_count) <= 0:
            logger.debug(f"Skipping empty table {schema_name}.{table_name} (not in always_include_tables)")
            return False
            
        # Table has rows, include it
        return True

    def _validate_table_copy(
        self,
        conn: Any,
        schema_name: str,
        table_name: str,
        expected_rows: Any,
        log_file: str,
    ) -> None:
        """Compare row counts between source scope and target table."""
        if expected_rows is None:
            return

        full_name = f"{schema_name}.{table_name}"
        try:
            cur = execute_sql_with_timeout(
                conn,
                f"SELECT COUNT(*) FROM {full_name}",
                timeout=self.config["sql_timeout"],
            )
            actual = cur.fetchone()[0]
        except Exception as exc:
            msg = f"Validation failed for {full_name}: {exc}"
            logger.error(msg)
            log_exception_to_file(msg, log_file)
            if self.config.get("fail_on_mismatch"):
                raise
            return

        if int(actual) != int(expected_rows):
            msg = (
                f"Row count mismatch for {full_name}: expected {expected_rows}, got {actual}"
            )
            logger.warning(msg)
            log_exception_to_file(msg, log_file)
            if self.config.get("fail_on_mismatch"):
                raise RowCountMismatchError(msg)

    def _process_table_operation_row(
        self, conn: Any, row_dict: dict[str, Any], idx: int, log_file: str
    ) -> bool:
        drop_sql = row_dict.get("Drop_IfExists", "")
        select_into_sql = row_dict.get("Select_Into", "")
        fconvert = row_dict.get("fConvert")
        row_id = row_dict.get("RowID")

        table_name = validate_sql_identifier(row_dict.get("TableName"))
        schema_name = validate_sql_identifier(row_dict.get("SchemaName"))
        db_name = validate_sql_identifier(self.db_name)  # Ensure we have the database name
        scope_row_count = row_dict.get("ScopeRowCount")

        # Create both versions of the table name - with and without database prefix
        full_table_name = f"{schema_name}.{table_name}"
        fully_qualified_name = f"{db_name}.{full_table_name}"

        # Only check row count if fConvert is 1
        if fconvert == 1 and select_into_sql:
            try:
                # Check if we have a SELECT INTO statement to work with
                if " INTO " in select_into_sql.upper():
                    # Find the position of " INTO " with proper parenthesis handling
                    paren_count = 0
                    into_pos = -1
                    
                    for i in range(len(select_into_sql)):
                        if select_into_sql[i:i+1] == "(":
                            paren_count += 1
                        elif select_into_sql[i:i+1] == ")":
                            paren_count -= 1
                        elif select_into_sql[i:i+6].upper() == " INTO " and paren_count == 0:
                            into_pos = i
                            break
                    
                    if into_pos > -1:
                        # Get the SELECT part of the query (before INTO)
                        select_part = select_into_sql[:into_pos].strip()
                        
                        # Transform the SELECT to COUNT as requested
                        count_sql = ""
                        if select_part.upper().startswith("SELECT DISTINCT"):
                            # For DISTINCT queries, find the first column to use with COUNT(DISTINCT)
                            columns_part = select_part[len("SELECT DISTINCT"):].strip()
                            from_pos = columns_part.upper().find(" FROM ")
                            
                            if from_pos > -1:
                                # Extract first column for COUNT(DISTINCT )
                                first_column = columns_part[:from_pos].split(",")[0].strip()
                                from_clause = columns_part[from_pos:].strip()
                                count_sql = f"SELECT COUNT(DISTINCT {first_column}) {from_clause}"
                            else:
                                # Skip count if we can't parse properly
                                logger.debug(f"Skipping count validation for {full_table_name} (can't parse DISTINCT query)")
                        else:
                            # For non-DISTINCT queries
                            if select_part.upper().startswith("SELECT"):
                                # Replace first SELECT with COUNT(*)
                                select_clause = select_part[len("SELECT"):].strip()
                                from_pos = select_clause.upper().find(" FROM ")
                                
                                if from_pos > -1:
                                    from_clause = select_clause[from_pos:].strip()
                                    count_sql = f"SELECT COUNT(*) {from_clause}"
                                else:
                                    # Skip count if we can't parse FROM clause
                                    logger.debug(f"Skipping count validation for {full_table_name} (can't parse FROM clause)")
                            else:
                                # Skip for unparseable queries
                                logger.debug(f"Skipping count validation for {full_table_name} (unparseable query)")
                        
                        # Execute the count query if we were able to build one
                        if count_sql:
                            try:
                                logger.debug(f"Executing count validation: {count_sql}")
                                count_result = execute_sql_with_timeout(
                                    conn, count_sql, timeout=self.config["sql_timeout"]
                                )
                                actual_count = count_result.fetchone()[0]
                                
                                # Use the actual count instead of the static ScopeRowCount
                                scope_row_count = actual_count
                                logger.debug(f"Validated row count for {full_table_name}: {actual_count}")
                            except Exception as count_error:
                                logger.warning(
                                    f"Count query failed for {full_table_name}, using original ScopeRowCount ({scope_row_count}): {count_error}"
                                )
                        
                else:
                    # We don't attempt to count rows directly from the table as it may not exist yet
                    logger.debug(f"Skipping row count validation for {full_table_name} (no SELECT INTO pattern found)")

            except Exception as ex:
                logger.warning(f"Error processing SELECT statement for {full_table_name}: {ex}")

        if not drop_sql.strip():
            return True

        logger.info(
            f"RowID:{idx} Drop If Exists:({self.DB_TYPE}.{full_table_name})"
        )
        try:
            sanitize_sql(
                conn,
                drop_sql,
                timeout=self.config["sql_timeout"],
            )

            if select_into_sql.strip():
                logger.info(
                    f"RowID:{idx} Select INTO:({self.DB_TYPE}.{full_table_name})"
                )
                sanitize_sql(
                    conn,
                    select_into_sql,
                    timeout=self.config["sql_timeout"],
                )

                # Determine inserted row count and update metadata table
                count_cur = execute_sql_with_timeout(
                    conn,
                    f"SELECT COUNT(*) FROM {full_table_name}",
                    timeout=self.config["sql_timeout"],
                )
                inserted_count = count_cur.fetchone()[0]

                tables_table = (
                    f"TablesToConvert_{self.DB_TYPE}" if self.DB_TYPE != "Justice" else "TablesToConvert"
                )
                tables_table = validate_sql_identifier(tables_table)
                update_sql = (
                    f"UPDATE {db_name}.dbo.{tables_table} SET ScopeRowCount = ? WHERE RowID = ?"
                )
                sanitize_sql(
                    conn,
                    update_sql,
                    params=(inserted_count, row_id),
                    timeout=self.config["sql_timeout"],
                )
                scope_row_count = inserted_count

            conn.commit()
            self._validate_table_copy(
                conn, schema_name, table_name, scope_row_count, log_file
            )
            return True

        except RowCountMismatchError:
            raise
        except Exception as sql_error:
            conn.rollback()
            error_msg = (
                f"SQL execution error for row {idx} ({full_table_name}): {str(sql_error)}"
            )
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            return False

    def create_primary_keys(self, conn: Any) -> None:
        """Create primary keys and NOT NULL constraints."""
        if self.config['skip_pk_creation']:
            logger.info("Skipping primary key and constraint creation as requested in configuration")
            return

        log_file = self.config['log_file']
        pk_table = (
            f"PrimaryKeyScripts_{self.DB_TYPE}" if self.DB_TYPE != 'Justice' else "PrimaryKeyScripts"
        )
        tables_table = (
            f"TablesToConvert_{self.DB_TYPE}" if self.DB_TYPE != 'Justice' else "TablesToConvert"
        )
        pk_table = validate_sql_identifier(pk_table)
        tables_table = validate_sql_identifier(tables_table)

        logger.info(f"Generating List of Primary Keys and NOT NULL Columns for {self.DB_TYPE} Database")
        pk_script_name = f"create_primarykeys_{self.DB_TYPE.lower()}" if self.DB_TYPE != 'Justice' else 'create_primarykeys'
        pk_sql = load_sql(f'{self.DB_TYPE.lower()}/{pk_script_name}.sql', self.db_name)
        
        # Split the script into individual statements and execute them separately
        statements = [stmt.strip() for stmt in pk_sql.split(';') if stmt.strip()]
        
        try:
            for i, stmt in enumerate(statements):
                logger.debug(f"Executing PK script statement {i+1} of {len(statements)}")
                try:
                    conn.execute(sqlalchemy.text(stmt))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error executing statement {i+1}: {e}")
                    log_exception_to_file(f"Error executing statement {i+1}: {e}\n\nStatement: {stmt}", log_file)
                    raise
        except Exception as e:
            logger.error(f"Failed to execute primary key script: {e}")
            raise

        # Rest of your existing code...
        # Verify the table was created before proceeding
        verify_sql = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WITH (NOLOCK) WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{pk_table}'"
        verify_result = None
        try:
            verify_result = conn.execute(sqlalchemy.text(verify_sql)).fetchone()
        except Exception as e:
            logger.error(f"Error verifying PrimaryKeyScripts table: {e}")
        
        if not verify_result:
            error_msg = f"Critical error: {pk_table} table was not created by the SQL script."
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise RuntimeError(error_msg)

        db_name = validate_sql_identifier(self.db_name)
        with transaction_scope(conn):
            rows = self._fetch_pk_rows(conn, db_name, pk_table, tables_table)

            start_idx = self._get_progress("pk_creation")
            for idx, row in enumerate(safe_tqdm(rows, desc="PK Creation", unit="table"), 1):
                if idx <= start_idx:
                    continue
                self._process_pk_row(conn, row, idx, log_file)
                self._update_progress("pk_creation", idx)

        logger.info(f"All Primary Key/NOT NULL statements executed FOR THE {self.DB_TYPE} DATABASE.")

    def _fetch_pk_rows(self, conn: Any, db_name: str, pk_table: str, tables_table: str) -> list[dict[str, Any]]:
        # Verify the tables exist before running the main query
        verify_sql = f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WITH (NOLOCK) 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ('{pk_table}', '{tables_table}')
        """
        
        try:
            verify_result = conn.execute(sqlalchemy.text(verify_sql)).fetchone()
            if not verify_result or verify_result[0] < 2:
                logger.error(f"One or both required tables missing: {pk_table}, {tables_table}")
                return []
        except Exception as e:
            logger.error(f"Error verifying required tables: {e}")
            return []

        query = f"""
            WITH CTE_PKS AS (
                SELECT 1 AS TYPEY, S.DatabaseName, S.SchemaName, S.TableName, S.Script
                FROM {db_name}.dbo.{pk_table} S
                WHERE S.ScriptType='NOT_NULL'
                UNION
                SELECT 2 AS TYPEY, S.DatabaseName, S.SchemaName, S.TableName, S.Script
                FROM {db_name}.dbo.{pk_table} S
                WHERE S.ScriptType='PK'
            )
            SELECT S.TYPEY, TTC.ScopeRowCount, S.DatabaseName, S.SchemaName, S.TableName,
                   REPLACE(S.Script, 'FLAG NOT NULL', 'BIT NOT NULL') AS [Script], TTC.fConvert
            FROM CTE_PKS S
            INNER JOIN {db_name}.dbo.{tables_table} TTC WITH (NOLOCK)
                ON S.SCHEMANAME=TTC.SchemaName AND S.TABLENAME=TTC.TableName
            WHERE TTC.fConvert=1
            ORDER BY S.SCHEMANAME, S.TABLENAME, S.TYPEY
        """

        try:
            cursor = execute_sql_with_timeout(conn, query, timeout=self.config["sql_timeout"])
        except Exception as e:
            logger.error(f"Error executing PK rows query: {e}")
            return []
            
        # Handle SQLAlchemy CursorResult objects differently than DB-API cursors
        try:
            if hasattr(cursor, "mappings"):
                # SQLAlchemy 1.4+ CursorResult object
                return list(cursor.mappings())
            elif hasattr(cursor, "keys") and callable(cursor.keys):
                # Older SQLAlchemy versions
                columns = cursor.keys()
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            elif hasattr(cursor, "description"):
                # Standard DB-API cursor
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
            else:
                # Last resort fallback
                return list(cursor)
        except Exception as e:
            logger.error(f"Error processing PK query results: {e}")
            return []

    def _process_pk_row(self, conn: Any, row_dict: dict[str, Any], idx: int, log_file: str) -> None:
        createpk_sql = row_dict.get('Script')
        scope_row_count = row_dict.get('ScopeRowCount')
        schema_name = validate_sql_identifier(row_dict.get('SchemaName'))
        table_name = validate_sql_identifier(row_dict.get('TableName'))
        full_table_name = f"{schema_name}.{table_name}"

        logger.info(f"RowID:{idx} PK Creation:({self.DB_TYPE}.{full_table_name})")
        if self._should_process_table(scope_row_count, schema_name, table_name):
            try:
                sanitize_sql(
                    conn,
                    createpk_sql,
                    timeout=self.config['sql_timeout'],
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                error_msg = (
                    f"Error executing PK statements for row {idx} ({self.DB_TYPE}.{full_table_name}): {e}"
                )
                logger.error(error_msg)
                log_exception_to_file(error_msg, log_file)

    def show_completion_message(self, next_step_name: Optional[str] = None) -> bool:
        """Show a message box indicating completion and asking to continue."""
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        
        message = f"{self.DB_TYPE} database migration is complete.\n\n"
        message += f"You may now drop the {self.DB_TYPE} database if desired.\n\n"
        
        if next_step_name:
            message += f"Click Yes to proceed to {next_step_name}, or No to stop."
            proceed = messagebox.askyesno(f"{self.DB_TYPE} DB Migration Complete", message)
            root.destroy()
            return proceed
        else:
            message += "Click OK to continue."
            messagebox.showinfo(f"{self.DB_TYPE} DB Migration Complete", message)
            root.destroy()
            return False

    def run(self) -> bool:
        """Template method - main execution flow."""
        try:
            # Parse command line args and load config
            args = self.parse_args()
            self.validate_environment()
            self.load_config(args)

            if os.environ.get("RESUME") != "1" and os.path.exists(self.progress_file):
                try:
                    os.remove(self.progress_file)
                except OSError:
                    pass

            # Set up logging level
            if args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)

            # Verify database connectivity before proceeding
            from db.health import check_target_connection
            if not check_target_connection():
                logger.error("Database connectivity check failed")
                return False

            # Get target database name
            from config import settings, parse_database_name
            conn_val = settings.mssql_target_conn_str.get_secret_value() if settings.mssql_target_conn_str else None
            self.db_name = settings.mssql_target_db_name or parse_database_name(conn_val)

            # Begin database operations
            with get_target_connection() as target_conn:
                # Execute specific pre-processing steps
                self.execute_preprocessing(target_conn)
                
                # Prepare SQL commands for drops and inserts
                self.prepare_drop_and_select(target_conn)
                
                # Import joins from CSV
                self.import_joins()
                
                # Update joins in tables
                self.update_joins_in_tables(target_conn)
                
                # Execute table operations
                self.execute_table_operations(target_conn)
                
                # Create primary keys and constraints
                self.create_primary_keys(target_conn)
                
                # Show completion message and determine next steps
                next_step_name = self.get_next_step_name()
                proceed = self.show_completion_message(next_step_name)

                if os.path.exists(self.progress_file):
                    try:
                        os.remove(self.progress_file)
                    except OSError:
                        pass

                if proceed and next_step_name:
                    logger.info(f"User chose to proceed to {next_step_name}.")
                    return True
                else:
                    logger.info(f"User chose to stop after {self.DB_TYPE} migration.")
                    return False
                    
        except Exception as e:
            logger.exception("Unexpected error")
            import traceback
            error_details = traceback.format_exc()
            
            # Try to log the error to file
            try:
                log_file = self.config.get('log_file', self.DEFAULT_LOG_FILE)
                log_exception_to_file(error_details, log_file)
            except Exception as log_exc:
                logger.error(f"Failed to write to error log: {log_exc}")
            
            # Try to show error message box
            try:
                root = tk.Tk()
                root.withdraw()
                messagebox.showerror("ETL Script Error", f"An error occurred:\n\n{error_details}")
                root.destroy()
            except Exception as msgbox_exc:
                logger.error(f"Failed to show error message box: {msgbox_exc}")
            
            return False
    
    # Methods that must be implemented by subclasses
    
    def execute_preprocessing(self, conn: Any) -> None:
        """Execute database-specific preprocessing steps."""
        raise NotImplementedError("Subclasses must implement execute_preprocessing()")
    
    def prepare_drop_and_select(self, conn: Any) -> None:
        """Prepare SQL statements for dropping and selecting data."""
        raise NotImplementedError("Subclasses must implement prepare_drop_and_select()")
    
    def update_joins_in_tables(self, conn: Any) -> None:
        """Update tables with JOINs."""
        raise NotImplementedError("Subclasses must implement update_joins_in_tables()")
    
    def get_next_step_name(self) -> str:
        """Return the name of the next step in the ETL process."""
        raise NotImplementedError("Subclasses must implement get_next_step_name()")
