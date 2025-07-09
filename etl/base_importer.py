"""Base class for database import operations."""
from __future__ import annotations
import logging
import os
import argparse
import tkinter as tk
from tkinter import messagebox
import pandas as pd
import urllib
import sqlalchemy
from typing import Any, Optional
from sqlalchemy.types import Text
from sqlalchemy.exc import SQLAlchemyError
import pyodbc
from utils.etl_helpers import SQLExecutionError
from db.connections import get_target_connection
from utils.etl_helpers import (
    load_sql,
    run_sql_script,
    log_exception_to_file,
    transaction_scope,
    execute_sql_with_timeout,
)
from utils.progress_tracker import ProgressTracker
from utils.sql_security import validate_sql_statement
from etl.core import (
    sanitize_sql,
    safe_tqdm,
    load_config,
    validate_environment,
    validate_sql_identifier,
)
from config import ETLConstants
from config.settings import settings

logger = logging.getLogger(__name__)


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
        self.progress = ProgressTracker(self.progress_file)
        self.extra_validation = False

    def parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description=f"{self.DB_TYPE} database import operations")
        # Add other arguments...
        parser.add_argument("--config", dest="config_file",
                           default="config/secure_config.json",  # Set default config path
                           help="Path to configuration file")
        parser.add_argument(
            "--extra-validation",
            action="store_true",
            help="Enable extra SQL validation checks",
        )
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

    def run_sql_file(self, conn: Any, name: str, filename: str) -> None:
        """Load a SQL file and execute it with optional validation."""
        sql = load_sql(filename, self.db_name)
        if self.extra_validation:
            sql = validate_sql_statement(sql, allow_ddl=True)
        
        # Use the new pyodbc raw execution for scripts with GO statements
        from utils.etl_helpers import run_sql_script_pyodbc_raw
        run_sql_script_pyodbc_raw(conn, name, sql, timeout=self.config["sql_timeout"])

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
        start_idx = self.progress.get("table_operations")

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
                            self.progress.update("table_operations", idx)
                        else:
                            failed_tables += 1
                    except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as row_error:
                        table = f"{row_dict.get('SchemaName')}.{row_dict.get('TableName')}"
                        error_msg = f"Row processing error during DROP/SELECT for {table}: {row_error}"
                        logger.error(error_msg)
                        log_exception_to_file(error_msg, log_file)
                        raise

        except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as query_error:
            error_msg = f"Fatal query error during table operations: {query_error}"
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise

        logger.info(f"Table operations completed: {successful_tables} successful, {failed_tables} failed")

    def drop_empty_tables(self, conn: Any) -> None:
        """Drop any tables that ended up with zero rows."""
        log_file = self.config['log_file']
        tables_table = (
            f"TablesToConvert_{self.DB_TYPE}" if self.DB_TYPE != 'Justice' else 'TablesToConvert'
        )
        tables_table = validate_sql_identifier(tables_table)

        if not self.db_name:
            logger.warning("Database name not available; skipping drop_empty_tables")
            return

        db_name = validate_sql_identifier(self.db_name)

        query = (
            f"SELECT SchemaName, TableName FROM {db_name}.dbo.{tables_table} "
            "WHERE fConvert=1 AND ISNULL(ScopeRowCount,0)=0"
        )

        try:
            cursor = execute_sql_with_timeout(
                conn, query, timeout=self.config["sql_timeout"]
            )
        except (SQLAlchemyError, pyodbc.Error) as e:  # pragma: no cover - depends on DB
            logger.warning(f"Could not fetch empty tables: {e}")
            return

        try:
            if hasattr(cursor, "mappings"):
                rows = list(cursor.mappings())
            elif hasattr(cursor, "keys") and callable(cursor.keys):
                columns = cursor.keys()
                rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
            elif hasattr(cursor, "description"):
                columns = [d[0] for d in cursor.description]
                rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
            else:
                rows = [
                    {f"col{i}": val for i, val in enumerate(r)}
                    for r in cursor.fetchall()
                ]
        except (SQLAlchemyError, pyodbc.Error) as e:  # pragma: no cover - edge case
            logger.error(f"Failed processing empty table query: {e}")
            return

        overrides = {
            t.strip().lower() for t in self.config.get("always_include_tables", [])
        }

        with transaction_scope(conn):
            for row in rows:
                schema_name = validate_sql_identifier(row.get("SchemaName") or row.get("schemaname"))
                table_name = validate_sql_identifier(row.get("TableName") or row.get("tablename"))
                patterns = [
                    f"{schema_name}.{table_name}".lower(),
                    f"{self.db_name}.{schema_name}.{table_name}".lower(),
                    f"{self.DB_TYPE.lower()}.{schema_name}.{table_name}".lower(),
                ]

                if any(p in overrides for p in patterns):
                    continue

                drop_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
                try:
                    sanitize_sql(
                        conn,
                        drop_sql,
                        timeout=self.config["sql_timeout"],
                    )
                except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as e:
                    logger.error(
                        f"Error dropping table {schema_name}.{table_name}: {e}"
                    )
                    log_exception_to_file(str(e), log_file)

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
        row_id: int,
        actual_rows: int,
        log_file: str,
    ) -> None:
        """Update metadata table with the actual row count."""
        if row_id is None or actual_rows is None:
            return

        tables_table = (
            f"TablesToConvert_{self.DB_TYPE}" if self.DB_TYPE != "Justice" else "TablesToConvert"
        )
        tables_table = validate_sql_identifier(tables_table)
        db_name = validate_sql_identifier(self.db_name)

        # Use named parameters with SQLAlchemy text() syntax
        update_sql = (
            f"UPDATE {db_name}.dbo.{tables_table} SET ScopeRowCount = :actual_rows WHERE RowID = :row_id"
        )

        try:
            # Pass parameters as a dictionary for named parameters
            sanitize_sql(
                conn,
                update_sql,
                params={'actual_rows': actual_rows, 'row_id': row_id},
                timeout=self.config["sql_timeout"],
            )
        except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as exc:
            msg = f"Failed to update row count for RowID {row_id}: {exc}"
            logger.error(msg)
            log_exception_to_file(msg, log_file)

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
                            except (SQLAlchemyError, pyodbc.Error) as count_error:
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
                # FIXED: Use the fully qualified name instead of just schema.table
                fully_qualified_name = f"{db_name}.{full_table_name}"

                # Add this block to handle prefixed table names
                if self.DB_TYPE == "Operations":
                    actual_table_name = f"Operations_{table_name}"
                    fully_qualified_table_name = f"{db_name}.{schema_name}.{actual_table_name}"
                elif self.DB_TYPE == "Financial":
                    actual_table_name = f"Financial_{table_name}"
                    fully_qualified_table_name = f"{db_name}.{schema_name}.{actual_table_name}"
                else:
                    # For Justice DB (and base tests), use schema.table only
                    if self.DB_TYPE == "base":
                        fully_qualified_table_name = full_table_name
                    else:
                        fully_qualified_table_name = fully_qualified_name

                count_cur = execute_sql_with_timeout(
                    conn,
                    f"SELECT COUNT(*) FROM {fully_qualified_table_name}",
                    timeout=self.config["sql_timeout"],
                )
                inserted_count = count_cur.fetchone()[0]
                scope_row_count = inserted_count

            conn.commit()
            self._validate_table_copy(
                conn,
                row_id,
                scope_row_count,
                log_file,
            )
            return True

        except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as sql_error:
            conn.rollback()
            error_msg = (
                f"SQL execution error for row {idx} ({full_table_name}): {str(sql_error)}"
            )
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise

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
        pk_script_filename = f'{self.DB_TYPE.lower()}/{pk_script_name}.sql'
        
        # Use run_sql_file to properly handle GO statements
        try:
            self.run_sql_file(conn, f"Create Primary Keys - {self.DB_TYPE}", pk_script_filename)
            logger.info(f"Successfully executed primary key creation script for {self.DB_TYPE}")
        except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as e:
            logger.error(f"Failed to execute primary key creation script: {e}")
            log_exception_to_file(
                f"Error executing primary key creation script: {e}",
                log_file,
            )
            raise

        # Verify the table was created before proceeding
        verify_sql = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WITH (NOLOCK) WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{pk_table}'"
        verify_result = None
        try:
            verify_result = conn.execute(sqlalchemy.text(verify_sql)).fetchone()
        except (SQLAlchemyError, pyodbc.Error) as e:
            logger.error(f"Error verifying PrimaryKeyScripts table: {e}")
        
        if not verify_result:
            error_msg = f"Critical error: {pk_table} table was not created by the SQL script."
            logger.error(error_msg)
            log_exception_to_file(error_msg, log_file)
            raise RuntimeError(error_msg)

        db_name = validate_sql_identifier(self.db_name)
        with transaction_scope(conn):
            rows = self._fetch_pk_rows(conn, db_name, pk_table, tables_table)

            start_idx = self.progress.get("pk_creation")
            for idx, row in enumerate(safe_tqdm(rows, desc="PK Creation", unit="table"), 1):
                if idx <= start_idx:
                    continue
                self._process_pk_row(conn, row, idx, log_file)
                self.progress.update("pk_creation", idx)

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
        except (SQLAlchemyError, pyodbc.Error) as e:
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
        except (SQLAlchemyError, pyodbc.Error) as e:
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
        except (SQLAlchemyError, pyodbc.Error) as e:
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
            except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as e:
                conn.rollback()
                error_msg = (
                    f"Error executing PK statements for row {idx} ({self.DB_TYPE}.{full_table_name}): {e}"
                )
                logger.error(error_msg)
                log_exception_to_file(error_msg, log_file)
                raise

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
            self.extra_validation = bool(os.environ.get("EJ_EXTRA_VALIDATION"))
            if getattr(args, "extra_validation", False):
                self.extra_validation = True
            self.validate_environment()
            self.load_config(args)

            if os.environ.get("RESUME") != "1":
                self.progress.delete()

            # Set up logging level
            if args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)

            # Verify database connectivity before proceeding
            from db.health import check_target_connection
            if not check_target_connection():
                logger.error("Database connectivity check failed")
                return False

            # Get target database name
            from config.settings import settings, parse_database_name
            conn_val = settings.mssql_target_conn_str if settings.mssql_target_conn_str else None
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

                # Drop any empty tables that were created
                self.drop_empty_tables(target_conn)

                # Create primary keys and constraints
                self.create_primary_keys(target_conn)
                
                # Show completion message and determine next steps
                next_step_name = self.get_next_step_name()
                proceed = self.show_completion_message(next_step_name)

                self.progress.delete()

                if proceed and next_step_name:
                    logger.info(f"User chose to proceed to {next_step_name}.")
                    return True
                else:
                    logger.info(f"User chose to stop after {self.DB_TYPE} migration.")
                    return False
                    
        except (SQLExecutionError, SQLAlchemyError, pyodbc.Error) as e:
            logger.exception("Database error")
            import traceback
            error_details = traceback.format_exc()
            try:
                log_file = self.config.get('log_file', self.DEFAULT_LOG_FILE)
                log_exception_to_file(error_details, log_file)
            except Exception as log_exc:
                logger.error(f"Failed to write to error log: {log_exc}")
            try:
                root = tk.Tk()
                root.withdraw()
                messagebox.showerror("ETL Script Error", f"An error occurred:\n\n{error_details}")
                root.destroy()
            except Exception as msgbox_exc:
                logger.error(f"Failed to show error message box: {msgbox_exc}")
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
