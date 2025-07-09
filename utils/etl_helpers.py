"""Helper functions for executing SQL statements with logging and retries."""
import logging
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, List, Optional
import sqlalchemy
from config import ETLConstants, parse_database_name, settings
from db.migrations import ensure_version_table, has_migration, record_migration
from utils.logging_helper import record_failure, record_success


class ETLError(Exception):
    """Base exception for ETL operations."""
class SQLExecutionError(ETLError):
    """Exception raised when SQL execution fails."""

    def __init__(
        self, sql: str, original_error: Exception, table_name: Optional[str] = None
    ):
        self.sql = sql
        self.original_error = original_error
        self.table_name = table_name
        msg = f"SQL execution failed for {table_name or 'statement'}: {original_error}"
        super().__init__(msg)
logger = logging.getLogger(__name__)

def run_sql_script_pyodbc_raw(
    conn: Any, name: str, sql: str, timeout: int = ETLConstants.DEFAULT_SQL_TIMEOUT
) -> None:
    """Execute a SQL script by splitting on GO statements and executing each batch with pyodbc."""
    logger.info(f"Starting raw pyodbc script execution: {name}")
    ensure_version_table(conn)
    if has_migration(conn, name):
        logger.info(f"Skipping script {name}: already applied")
        return
    
    start_time = time.time()
    try:
        # Split by GO statements - same regex as before
        go_batches = re.split(r'(?:^|\n)\s*GO\s*(?:\r?\n|$)', sql, flags=re.IGNORECASE | re.MULTILINE)
        
        logger.info(f"SQL script split into {len(go_batches)} batches for pyodbc execution")
        
        total_batches = 0
        
        # Use raw pyodbc connection
        if hasattr(conn, "cursor"):
            # This is a pyodbc connection
            with conn.cursor() as cursor:
                cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000}")
                
                for batch_idx, batch in enumerate(go_batches):
                    batch_sql = batch.strip()
                    
                    # Skip completely empty batches
                    if not batch_sql:
                        logger.debug(f"Skipping empty batch {batch_idx + 1}")
                        continue
                    
                    # Remove leading/trailing comments and whitespace, but keep SQL statements
                    lines = batch_sql.split('\n')
                    sql_lines = []
                    
                    for line in lines:
                        line = line.strip()
                        # Skip empty lines and comment-only lines
                        if not line or line.startswith('--'):
                            continue
                        # Keep lines that have SQL content (even if they have comments)
                        sql_lines.append(line)
                    
                    # If no SQL content remains, skip this batch
                    if not sql_lines:
                        logger.debug(f"Skipping comment-only batch {batch_idx + 1}")
                        continue
                    
                    # Reconstruct the batch with only SQL content
                    final_batch_sql = '\n'.join(sql_lines)
                    
                    logger.info(f"Executing pyodbc batch {batch_idx + 1} of {len(go_batches)}")
                    logger.debug(f"Batch SQL (first 200 chars): {final_batch_sql[:200]}...")
                    
                    try:
                        cursor.execute(final_batch_sql)
                        conn.commit()
                        logger.info(f"Completed pyodbc batch {batch_idx + 1} successfully")
                        total_batches += 1
                    except Exception as e:
                        logger.error(f"Error executing pyodbc batch {batch_idx + 1}: {e}")
                        logger.error(f"Batch SQL: {final_batch_sql}")
                        raise SQLExecutionError(final_batch_sql, e, table_name=name)
        else:
            # For SQLAlchemy connections, get the raw pyodbc connection
            raw_conn = conn.connection.connection  # Get the underlying pyodbc connection
            with raw_conn.cursor() as cursor:
                cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000}")
                
                for batch_idx, batch in enumerate(go_batches):
                    batch_sql = batch.strip()
                    
                    # Skip completely empty batches
                    if not batch_sql:
                        logger.debug(f"Skipping empty batch {batch_idx + 1}")
                        continue
                    
                    # Remove leading/trailing comments and whitespace, but keep SQL statements
                    lines = batch_sql.split('\n')
                    sql_lines = []
                    
                    for line in lines:
                        line = line.strip()
                        # Skip empty lines and comment-only lines
                        if not line or line.startswith('--'):
                            continue
                        # Keep lines that have SQL content (even if they have comments)
                        sql_lines.append(line)
                    
                    # If no SQL content remains, skip this batch
                    if not sql_lines:
                        logger.debug(f"Skipping comment-only batch {batch_idx + 1}")
                        continue
                    
                    # Reconstruct the batch with only SQL content
                    final_batch_sql = '\n'.join(sql_lines)
                    
                    logger.info(f"Executing pyodbc batch {batch_idx + 1} of {len(go_batches)}")
                    logger.debug(f"Batch SQL (first 200 chars): {final_batch_sql[:200]}...")
                    
                    try:
                        cursor.execute(final_batch_sql)
                        raw_conn.commit()
                        logger.info(f"Completed pyodbc batch {batch_idx + 1} successfully")
                        total_batches += 1
                    except Exception as e:
                        logger.error(f"Error executing pyodbc batch {batch_idx + 1}: {e}")
                        logger.error(f"Batch SQL: {final_batch_sql}")
                        raise SQLExecutionError(final_batch_sql, e, table_name=name)
        
        elapsed = time.time() - start_time
        logger.info(f"Completed pyodbc script: {name} - executed {total_batches} batches in {elapsed:.2f} seconds")
        record_success()
        record_migration(conn, name)
        
    except SQLExecutionError:
        raise
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error in pyodbc script {name}: {e}")
        logger.info(f"Pyodbc script {name} failed after {elapsed:.2f} seconds")
        record_failure()
        raise SQLExecutionError(sql, e, table_name=name)
def execute_sql_with_timeout(
    conn: Any,
    sql: str,
    params: Optional[tuple[Any, ...]] = None,
    timeout: int = ETLConstants.DEFAULT_SQL_TIMEOUT,
) -> Any:
    """Execute SQL with parameters and timeout."""
    if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
        # SQLAlchemy connection
        if params:
            result = conn.execute(sqlalchemy.text(sql), params)
        else:
            result = conn.execute(sqlalchemy.text(sql))
        return result
    else:
        # pyodbc connection
        cursor = conn.cursor()
        cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000}")
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor
def load_sql(filename: str, db_name: Optional[str] = None) -> str:
    """Load a SQL file from the ``sql_scripts`` package.

    The function uses :mod:`importlib.resources` so it works when the project is
    bundled with tools like PyInstaller.  SQL files use the ``{{DB_NAME}}``
    placeholder which will be replaced with the provided ``db_name``.  If no
    value is supplied, the function attempts to use ``run_etl.app``'s configured
    database name or the one from :mod:`config.settings`.

    Args:
        filename: Path to SQL file relative to ``sql_scripts`` package
        db_name: Optional database name to substitute for ``{{DB_NAME}}``

    Returns:
        SQL content with database name substituted if provided
    """

    # Normalize the requested file path and ensure it does not escape the
    # ``sql_scripts`` package.  ``resolve`` with ``strict=False`` is used so the
    # path need not actually exist on disk when packaged.
    path = Path(filename.replace("\\", "/"))
    if path.is_absolute():
        logger.error(f"Attempted absolute SQL path: {filename}")
        raise ValueError(f"Invalid SQL file path: {filename}")

    base_path = (Path(__file__).resolve().parent.parent / "sql_scripts").resolve()
    target_path = (base_path / path).resolve()
    if not str(target_path).startswith(str(base_path)):
        logger.error(f"Attempted SQL path traversal: {filename}")
        raise ValueError(f"Invalid SQL file path: {filename}")

    parts = path.parts

    package = "sql_scripts"
    if len(parts) > 1:
        package += "." + ".".join(parts[:-1])
    resource = parts[-1]

    try:
        from importlib import resources

        sql = resources.files(package).joinpath(resource).read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError) as exc:
        logger.error(f"SQL file not found: {filename}")
        raise FileNotFoundError(f"SQL file not found: {filename}") from exc

    if db_name is None:
        # Try to get the database name from the running UI if available
        try:
            import run_etl

            app = getattr(run_etl, "app", None)
            if app and hasattr(app, "entries"):
                db_name = app.entries["database"].get()
        except Exception:
            pass

    if db_name is None:
        conn_val = (
            settings.mssql_target_conn_str.get_secret_value()
            if settings.mssql_target_conn_str
            else None
        )
        db_name = settings.mssql_target_db_name or parse_database_name(conn_val)

    if db_name:
        sql = sql.replace("{{DB_NAME}}", db_name)
        logger.debug(f"Replaced database placeholder in {filename} with {db_name}")

    return sql
def log_exception_to_file(error_details: str, log_path: str) -> None:
    """Append exception details to a log file."""
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {error_details}\n")
    except Exception as file_exc:
        logger.error(f"Failed to write to error log file: {file_exc}")

@contextmanager
def transaction_scope(conn: Any) -> Generator[Any, None, None]:
    """Context manager to run a series of statements in a transaction.

    It temporarily disables ``autocommit`` on the provided connection and
    ensures that the connection is committed if the block succeeds or
    rolled back if an exception is raised.  The original ``autocommit``
    setting is restored afterwards.
    """

    original_autocommit = getattr(conn, "autocommit", None)
    if original_autocommit is not None:
        conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if original_autocommit is not None:
            conn.autocommit = original_autocommit

def run_sql_step(
    conn: Any, name: str, sql: str, timeout: int = ETLConstants.DEFAULT_SQL_TIMEOUT
) -> Optional[List[Any]]:
    logger.info(f"Starting step: {name}")
    start_time = time.time()
    try:
        # SQLAlchemy connection
        if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
            result = conn.execute(sqlalchemy.text(sql))
            try:
                results = result.fetchall()
                logger.info(f"{name}: Retrieved {len(results)} rows")
            except Exception:
                results = None
                logger.info(f"{name}: Statement executed (no results to fetch)")
            elapsed = time.time() - start_time
            logger.info(f"Completed step: {name} in {elapsed:.2f} seconds")
            record_success()
            record_migration(conn, name)
            return results
        # DB-API connection
        else:
            with conn.cursor() as cursor:
                cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000}")
                cursor.execute(sql)
                try:
                    results = cursor.fetchall()
                    logger.info(f"{name}: Retrieved {len(results)} rows")
                except Exception:
                    results = None
                    logger.info(f"{name}: Statement executed (no results to fetch)")
            elapsed = time.time() - start_time
            logger.info(f"Completed step: {name} in {elapsed:.2f} seconds")
            record_success()
            record_migration(conn, name)
            return results
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error executing step {name}: {e}. SQL: {sql}")
        logger.info(f"Step {name} failed after {elapsed:.2f} seconds")
        record_failure()
        raise SQLExecutionError(sql, e, table_name=name)
def run_sql_step_with_retry(
    conn: Any,
    name: str,
    sql: str,
    timeout: int = ETLConstants.DEFAULT_SQL_TIMEOUT,
    max_retries: int = ETLConstants.MAX_RETRY_ATTEMPTS,
) -> Optional[List[Any]]:
    """Execute a SQL step with retry logic for transient ``pyodbc.Error`` failures.

    Timeout and deadlock errors trigger exponential backoff retries.
    """

    for attempt in range(max_retries):
        try:
            return run_sql_step(conn, name, sql, timeout)
        except SQLExecutionError as exc:
            import pyodbc  # Imported lazily for tests that stub this module

            if not isinstance(exc.original_error, pyodbc.Error):
                raise

            if attempt == max_retries - 1:
                raise

            err_str = str(exc.original_error).lower()
            if "timeout" in err_str:
                logger.warning(
                    f"Timeout on attempt {attempt + 1} for {name}, retrying..."
                )
            elif "deadlock" in err_str:
                logger.warning(
                    f"Deadlock on attempt {attempt + 1} for {name}, retrying..."
                )

            time.sleep(2**attempt)
def run_sql_script(
    conn: Any, name: str, sql: str, timeout: int = ETLConstants.DEFAULT_SQL_TIMEOUT, raw_execution: bool = False
) -> None:
    """Execute a multi-statement SQL script."""
    logger.info(f"Starting script: {name}")
    ensure_version_table(conn)
    if has_migration(conn, name):
        logger.info(f"Skipping script {name}: already applied")
        return
    
    start_time = time.time()
    try:
        # If raw_execution is True, execute the entire SQL as-is
        if raw_execution:
            logger.info(f"Executing entire SQL script as single statement (raw mode)")
            logger.debug(f"Script content (first 500 chars): {sql[:500]}...")
            
            # SQLAlchemy connection
            if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
                conn.execute(sqlalchemy.text(sql))
                conn.commit()
            # DB-API connection
            else:
                with conn.cursor() as cursor:
                    cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000}")
                    cursor.execute(sql)
                    conn.commit()
            
            elapsed = time.time() - start_time
            logger.info(f"Completed raw script: {name} in {elapsed:.2f} seconds")
            record_success()
            record_migration(conn, name)
            return
        
        # Original batch processing logic continues here...
        go_batches = re.split(r'(?:^|\n)\s*GO\s*(?:\r?\n|$)', sql, flags=re.IGNORECASE | re.MULTILINE)
        
        # Debug logging to see how the SQL is being split
        logger.info(f"SQL script split into {len(go_batches)} batches")
        
        total_statements = 0

        # SQLAlchemy connection
        if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
            for batch_idx, batch in enumerate(go_batches):
                if not batch.strip():
                    logger.debug(f"Skipping empty batch {batch_idx + 1}")
                    continue
                    
                # Log BEFORE execution with actual timing
                logger.info(f"Executing batch {batch_idx + 1} of {len(go_batches)} at {time.strftime('%H:%M:%S')}")
                
                # Execute the entire batch as a single statement
                batch_sql = batch.strip()
                if batch_sql and not batch_sql.startswith("--"):
                    try:
                        logger.debug(f"Batch {batch_idx + 1} SQL (first 200 chars): {batch_sql[:200]}...")
                        
                        # Execute and commit immediately
                        conn.execute(sqlalchemy.text(batch_sql))
                        conn.commit()
                        
                        logger.info(f"Completed batch {batch_idx + 1} successfully at {time.strftime('%H:%M:%S')}")
                        total_statements += 1
                        
                        # Add a small delay to ensure proper sequencing
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error executing batch {batch_idx + 1}: {e}")
                        logger.error(f"Batch SQL: {batch_sql}")
                        raise SQLExecutionError(batch_sql, e, table_name=name)
                
        # DB-API connection
        else:
            with conn.cursor() as cursor:
                # Set the query timeout
                cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000}")

                for batch_idx, batch in enumerate(go_batches):
                    if not batch.strip():
                        logger.debug(f"Skipping empty batch {batch_idx + 1}")
                        continue
                        
                    # Log BEFORE execution with actual timing
                    logger.info(f"Executing batch {batch_idx + 1} of {len(go_batches)} at {time.strftime('%H:%M:%S')}")
                    
                    # Execute the entire batch as a single statement
                    batch_sql = batch.strip()
                    if batch_sql and not batch_sql.startswith("--"):
                        try:
                            logger.debug(f"Batch {batch_idx + 1} SQL (first 200 chars): {batch_sql[:200]}...")
                            
                            cursor.execute(batch_sql)
                            conn.commit()
                            
                            logger.info(f"Completed batch {batch_idx + 1} successfully at {time.strftime('%H:%M:%S')}")
                            total_statements += 1
                            
                            # Add a small delay to ensure proper sequencing
                            time.sleep(0.1)
                            
                        except Exception as e:
                            logger.error(f"Error executing batch {batch_idx + 1}: {e}")
                            logger.error(f"Batch SQL: {batch_sql}")
                            raise SQLExecutionError(batch_sql, e, table_name=name)

        elapsed = time.time() - start_time
        logger.info(f"Completed script: {name} - executed {total_statements} batches in {elapsed:.2f} seconds")
        record_success()
        record_migration(conn, name)
    except SQLExecutionError:
        raise
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error in script {name}: {e}")
        logger.info(f"Script {name} failed after {elapsed:.2f} seconds")
        record_failure()
        raise SQLExecutionError(sql, e, table_name=name)