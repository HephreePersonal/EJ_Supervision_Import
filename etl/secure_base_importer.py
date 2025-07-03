"""Enhanced base importer with security, error handling, and performance improvements."""

from __future__ import annotations

import logging
import os
import json
import argparse
import asyncio
import time
from pathlib import Path
from typing import Any, Optional, Dict, List, AsyncGenerator
from dataclasses import dataclass
from contextlib import asynccontextmanager

import pandas as pd
import sqlalchemy
from sqlalchemy.types import Text

# Import our new security and configuration modules
from config.secure_settings import SecureETLSettings, ConfigurationManager, get_secure_settings
from utils.sql_security import (
    SQLSecurityValidator, 
    SafeSQLBuilder, 
    validate_sql_identifier, 
    validate_table_name, 
    validate_sql_statement,
    SQLValidationResult,
    SQLRiskLevel
)
from utils.etl_helpers import (
    load_sql,
    run_sql_script,
    log_exception_to_file,
    transaction_scope,
    execute_sql_with_timeout,
    SQLExecutionError
)
from db.mssql import get_target_connection

logger = logging.getLogger(__name__)


@dataclass
class ProcessingContext:
    """Context for processing operations with security tracking."""
    operation_name: str
    table_name: Optional[str] = None
    row_id: Optional[int] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    risk_level: SQLRiskLevel = SQLRiskLevel.LOW
    start_time: float = None
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = time.time()


class SecureETLException(Exception):
    """Enhanced ETL exception with security context."""
    
    def __init__(
        self,
        message: str,
        context: ProcessingContext,
        original_error: Optional[Exception] = None,
        security_violation: bool = False
    ):
        super().__init__(message)
        self.context = context
        self.original_error = original_error
        self.security_violation = security_violation
        self.timestamp = time.time()


class SecureBaseDBImporter:
    """Enhanced base importer with security and performance improvements."""
    
    # Override these in subclasses
    DB_TYPE = "base"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Base.txt"
    DEFAULT_CSV_FILE = "EJ_Base_Selects_ALL.csv"
    
    def __init__(self):
        """Initialize the secure importer."""
        self.settings: Optional[SecureETLSettings] = None
        self.sql_validator = SQLSecurityValidator()
        self.sql_builder = SafeSQLBuilder()
        self.config_manager = ConfigurationManager()
        self.processing_stats = {
            "tables_processed": 0,
            "rows_processed": 0,
            "security_violations": 0,
            "errors": 0
        }
    
    def initialize(self) -> None:
        """Initialize the importer with secure settings."""
        try:
            self.settings = get_secure_settings()
            logger.info(f"Initialized {self.DB_TYPE} importer with secure configuration")
        except Exception as e:
            logger.error(f"Failed to initialize secure settings: {e}")
            raise SecureETLException(
                f"Configuration initialization failed: {e}",
                ProcessingContext("initialization"),
                original_error=e,
                security_violation=True
            )
    
    def parse_args(self) -> argparse.Namespace:
        """Parse command line arguments with security validation."""
        parser = argparse.ArgumentParser(description=f"{self.DB_TYPE} DB Import ETL Process")
        
        parser.add_argument(
            "--log-file",
            help="Path to the error log file. Overrides the EJ_LOG_DIR environment variable."
        )
        parser.add_argument(
            "--csv-file",
            help="Path to the CSV file. Overrides the EJ_CSV_DIR environment variable."
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
            "--verbose", "-v", 
            action="store_true",
            help="Enable verbose logging."
        )
        parser.add_argument(
            "--allow-high-risk-sql",
            action="store_true",
            help="Allow high-risk SQL operations (use with caution)."
        )
        
        return parser.parse_args()
    
    async def run_async(self) -> bool:
        """Main async execution method with comprehensive error handling."""
        context = ProcessingContext("main_execution")
        
        try:
            # Initialize secure configuration
            self.initialize()
            
            # Parse and validate arguments
            args = self.parse_args()
            self._validate_arguments(args)
            
            # Set logging level
            if args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)
            
            logger.info(f"Starting secure {self.DB_TYPE} ETL process")
            
            # Execute ETL stages
            async with self._get_database_connection() as conn:
                await self._execute_preprocessing_secure(conn, context)
                await self._prepare_drop_and_select_secure(conn, context)
                await self._import_joins_secure(conn, context)
                await self._update_joins_secure(conn, context)
                await self._execute_table_operations_secure(conn, context)
                
                if not args.skip_pk_creation:
                    await self._create_primary_keys_secure(conn, context)
            
            logger.info(f"Secure {self.DB_TYPE} ETL completed successfully")
            self._log_processing_stats()
            return True
            
        except SecureETLException as e:
            if e.security_violation:
                logger.critical(f"SECURITY VIOLATION: {e}")
                self.processing_stats["security_violations"] += 1
            else:
                logger.error(f"ETL error: {e}")
            self.processing_stats["errors"] += 1
            return False
            
        except Exception as e:
            logger.exception("Unexpected error in secure ETL process")
            self.processing_stats["errors"] += 1
            return False
    
    @asynccontextmanager
    async def _get_database_connection(self):
        """Secure database connection context manager."""
        conn = None
        try:
            conn = get_target_connection()
            
            # Validate connection security
            await self._validate_connection_security(conn)
            
            yield conn
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise SecureETLException(
                f"Failed to establish secure database connection: {e}",
                ProcessingContext("database_connection"),
                original_error=e
            )
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    async def _validate_connection_security(self, conn: Any) -> None:
        """Validate database connection security."""
        try:
            # Test basic connection
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: conn.execute(sqlalchemy.text("SELECT 1")).fetchone()
            )
            
            if not result:
                raise ValueError("Connection test failed")
            
            logger.debug("Database connection security validated")
            
        except Exception as e:
            raise SecureETLException(
                f"Connection security validation failed: {e}",
                ProcessingContext("connection_validation"),
                original_error=e,
                security_violation=True
            )
    
    async def _execute_preprocessing_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Execute preprocessing with security validation."""
        context.operation_name = "preprocessing"
        logger.info(f"Starting secure preprocessing for {self.DB_TYPE}")
        
        try:
            # This method should be implemented by subclasses
            await self.execute_preprocessing_async(conn, context)
            logger.info(f"Preprocessing completed for {self.DB_TYPE}")
            
        except Exception as e:
            raise SecureETLException(
                f"Preprocessing failed: {e}",
                context,
                original_error=e
            )
    
    async def _prepare_drop_and_select_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Prepare DROP and SELECT statements with security validation."""
        context.operation_name = "prepare_drop_select"
        logger.info(f"Preparing DROP and SELECT statements for {self.DB_TYPE}")
        
        try:
            # Load and validate SQL script
            script_name = f"{self.DB_TYPE.lower()}/gather_drops_and_selects_{self.DB_TYPE.lower()}.sql"
            if self.DB_TYPE == "Justice":
                script_name = "justice/gather_drops_and_selects.sql"
            
            sql_content = load_sql(script_name, self.settings.mssql_target_db_name)
            
            # Validate the SQL for security issues
            validation_result = self.sql_validator.validate_sql_statement(sql_content, allow_ddl=True)
            
            if not validation_result.is_valid:
                raise SecureETLException(
                    f"SQL validation failed: {'; '.join(validation_result.issues)}",
                    context,
                    security_violation=True
                )
            
            if validation_result.risk_level in [SQLRiskLevel.HIGH, SQLRiskLevel.CRITICAL]:
                logger.warning(f"High-risk SQL detected in {script_name}: {validation_result.issues}")
            
            # Execute the validated SQL
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: run_sql_script(conn, f"prepare_drop_select_{self.DB_TYPE}", sql_content, self.settings.sql_timeout)
            )
            
            logger.info(f"DROP and SELECT preparation completed for {self.DB_TYPE}")
            
        except Exception as e:
            raise SecureETLException(
                f"DROP and SELECT preparation failed: {e}",
                context,
                original_error=e
            )
    
    async def _import_joins_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Import joins with memory-efficient processing and security validation."""
        context.operation_name = "import_joins"
        logger.info(f"Importing joins for {self.DB_TYPE}")
        
        try:
            csv_path = self._get_csv_file_path()
            
            if not csv_path.exists():
                raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
            # Validate CSV file
            await self._validate_csv_file_security(csv_path)
            
            # Process CSV in secure chunks
            total_rows = 0
            async for chunk_data in self._process_csv_securely(csv_path):
                await self._insert_chunk_secure(conn, chunk_data, context)
                total_rows += len(chunk_data)
            
            logger.info(f"Successfully imported {total_rows} join definitions for {self.DB_TYPE}")
            
        except Exception as e:
            raise SecureETLException(
                f"Join import failed: {e}",
                context,
                original_error=e
            )
    
    async def _process_csv_securely(self, csv_path: Path) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Process CSV file in secure chunks."""
        chunk_size = self.settings.csv_chunk_size
        
        def read_csv_chunk(skip_rows: int) -> Optional[pd.DataFrame]:
            try:
                return pd.read_csv(
                    csv_path,
                    delimiter='|',
                    encoding='utf-8',
                    skiprows=skip_rows,
                    nrows=chunk_size,
                    dtype=str  # Read everything as string for security
                )
            except Exception:
                return None
        
        skip_rows = 0
        header_processed = False
        
        while True:
            chunk = await asyncio.get_event_loop().run_in_executor(
                None,
                read_csv_chunk,
                skip_rows if header_processed else 0
            )
            
            if chunk is None or chunk.empty:
                break
            
            if not header_processed:
                header_processed = True
                skip_rows = 1
            
            # Validate and clean chunk data
            cleaned_chunk = await self._validate_chunk_data(chunk)
            
            yield cleaned_chunk.to_dict('records')
            skip_rows += chunk_size
    
    async def _validate_chunk_data(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Validate chunk data for security issues."""
        # Remove any potentially dangerous content
        dangerous_patterns = ['<script', 'javascript:', 'vbscript:', 'onload=', 'onerror=']
        
        for column in chunk.columns:
            if chunk[column].dtype == 'object':  # String columns
                for pattern in dangerous_patterns:
                    chunk[column] = chunk[column].astype(str).str.replace(pattern, '', case=False, regex=False)
        
        # Validate key identifiers
        if 'TableName' in chunk.columns:
            for idx, table_name in enumerate(chunk['TableName']):
                if pd.notna(table_name):
                    try:
                        validate_sql_identifier(str(table_name))
                    except ValueError as e:
                        logger.warning(f"Invalid table name in CSV row {idx}: {table_name} - {e}")
                        chunk.loc[idx, 'TableName'] = None
        
        return chunk
    
    async def _insert_chunk_secure(self, conn: Any, chunk_data: List[Dict[str, Any]], context: ProcessingContext) -> None:
        """Insert chunk data with security validation."""
        if not chunk_data:
            return
        
        table_name = f'TableUsedSelects_{self.DB_TYPE}' if self.DB_TYPE != 'Justice' else 'TableUsedSelects'
        
        # Validate table name
        safe_table_name = validate_table_name("dbo", table_name, self.settings.mssql_target_db_name)
        
        try:
            # Use pandas to_sql with security settings
            df = pd.DataFrame(chunk_data)
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: df.to_sql(
                    table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi',  # Use multi-row insert for performance
                    chunksize=1000,
                    dtype={'Select_Into': Text(), 'Drop_IfExists': Text()}
                )
            )
            
            self.processing_stats["rows_processed"] += len(chunk_data)
            
        except Exception as e:
            raise SecureETLException(
                f"Chunk insert failed for {len(chunk_data)} rows: {e}",
                context,
                original_error=e
            )
    
    async def _execute_table_operations_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Execute table operations with enhanced security."""
        context.operation_name = "table_operations"
        logger.info(f"Executing secure table operations for {self.DB_TYPE}")
        
        try:
            # Fetch table operations with validation
            table_operations = await self._fetch_table_operations_secure(conn)
            
            successful = 0
            failed = 0
            
            for operation in table_operations:
                operation_context = ProcessingContext(
                    "table_operation",
                    table_name=operation.get("TableName"),
                    row_id=operation.get("RowID"),
                    schema_name=operation.get("SchemaName")
                )
                
                try:
                    if await self._process_table_operation_secure(conn, operation, operation_context):
                        successful += 1
                        self.processing_stats["tables_processed"] += 1
                    else:
                        failed += 1
                        
                except SecureETLException as e:
                    if e.security_violation:
                        logger.critical(f"Security violation in table operation: {e}")
                        self.processing_stats["security_violations"] += 1
                    failed += 1
            
            logger.info(f"Table operations completed: {successful} successful, {failed} failed")
            
        except Exception as e:
            raise SecureETLException(
                f"Table operations execution failed: {e}",
                context,
                original_error=e
            )
    
    async def _process_table_operation_secure(
        self, 
        conn: Any, 
        operation: Dict[str, Any], 
        context: ProcessingContext
    ) -> bool:
        """Process individual table operation with security validation."""
        
        try:
            # Validate and sanitize identifiers
            schema_name = validate_sql_identifier(operation.get("SchemaName", "dbo"))
            table_name = validate_sql_identifier(operation.get("TableName", ""))
            
            if not table_name:
                logger.warning("Skipping operation with empty table name")
                return False
            
            # Validate SQL statements
            drop_sql = operation.get("Drop_IfExists", "")
            select_sql = operation.get("Select_Into", "")
            
            if drop_sql:
                drop_validation = self.sql_validator.validate_sql_statement(drop_sql, allow_ddl=True)
                if not drop_validation.is_valid:
                    raise SecureETLException(
                        f"Invalid DROP SQL: {'; '.join(drop_validation.issues)}",
                        context,
                        security_violation=True
                    )
            
            if select_sql:
                select_validation = self.sql_validator.validate_sql_statement(select_sql, allow_ddl=True)
                if not select_validation.is_valid:
                    raise SecureETLException(
                        f"Invalid SELECT SQL: {'; '.join(select_validation.issues)}",
                        context,
                        security_violation=True
                    )
            
            # Check if table should be processed
            scope_row_count = operation.get("ScopeRowCount", 0)
            if not self._should_process_table_secure(scope_row_count, schema_name, table_name):
                logger.debug(f"Skipping table {schema_name}.{table_name} (scope: {scope_row_count})")
                return True
            
            # Execute operations safely
            if drop_sql:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: conn.execute(sqlalchemy.text(drop_sql))
                )
            
            if select_sql:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: conn.execute(sqlalchemy.text(select_sql))
                )
            
            # Commit the transaction
            await asyncio.get_event_loop().run_in_executor(
                None,
                conn.commit
            )
            
            logger.debug(f"Successfully processed table {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            # Rollback on error
            try:
                await asyncio.get_event_loop().run_in_executor(None, conn.rollback)
            except:
                pass
            
            if isinstance(e, SecureETLException):
                raise
            else:
                raise SecureETLException(
                    f"Table operation failed: {e}",
                    context,
                    original_error=e
                )
    
    def _should_process_table_secure(self, scope_row_count: Any, schema_name: str, table_name: str) -> bool:
        """Determine if table should be processed with security considerations."""
        if self.settings.include_empty_tables:
            return True
        
        # Check always include list
        table_patterns = [
            f"{schema_name}.{table_name}".lower(),
            f"{self.settings.mssql_target_db_name}.{schema_name}.{table_name}".lower(),
            f"{self.DB_TYPE.lower()}.{schema_name}.{table_name}".lower()
        ]
        
        always_include = [pattern.lower() for pattern in self.settings.always_include_tables]
        
        if any(pattern in always_include for pattern in table_patterns):
            return True
        
        # Check row count
        try:
            return int(scope_row_count or 0) > 0
        except (ValueError, TypeError):
            return False
    
    def _get_csv_file_path(self) -> Path:
        """Get the CSV file path with validation."""
        csv_file = self.settings.ej_csv_dir / self.DEFAULT_CSV_FILE
        
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
        return csv_file
    
    async def _validate_csv_file_security(self, csv_path: Path) -> None:
        """Validate CSV file for security issues."""
        try:
            # Check file size (prevent DoS)
            file_size = csv_path.stat().st_size
            max_size = 1024 * 1024 * 1024  # 1GB limit
            
            if file_size > max_size:
                raise SecureETLException(
                    f"CSV file too large: {file_size} bytes (max: {max_size})",
                    ProcessingContext("csv_validation"),
                    security_violation=True
                )
            
            # Check file permissions
            if not os.access(csv_path, os.R_OK):
                raise SecureETLException(
                    f"CSV file not readable: {csv_path}",
                    ProcessingContext("csv_validation"),
                    security_violation=True
                )
            
        except Exception as e:
            if isinstance(e, SecureETLException):
                raise
            else:
                raise SecureETLException(
                    f"CSV validation failed: {e}",
                    ProcessingContext("csv_validation"),
                    original_error=e
                )
    
    def _validate_arguments(self, args: argparse.Namespace) -> None:
        """Validate command line arguments for security."""
        # Validate file paths
        if args.log_file:
            log_path = Path(args.log_file)
            if not log_path.parent.exists():
                raise ValueError(f"Log directory does not exist: {log_path.parent}")
        
        if args.csv_file:
            csv_path = Path(args.csv_file)
            if not csv_path.exists():
                raise ValueError(f"CSV file does not exist: {csv_path}")
        
        # Validate numeric arguments
        if args.csv_chunk_size:
            if not (1000 <= args.csv_chunk_size <= 1000000):
                raise ValueError("CSV chunk size must be between 1,000 and 1,000,000")
    
    def _log_processing_stats(self) -> None:
        """Log processing statistics."""
        logger.info("Processing Statistics:")
        for key, value in self.processing_stats.items():
            logger.info(f"  {key}: {value}")
    
    # Abstract methods to be implemented by subclasses
    async def execute_preprocessing_async(self, conn: Any, context: ProcessingContext) -> None:
        """Execute database-specific preprocessing. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement execute_preprocessing_async()")
    
    async def _update_joins_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Update joins securely. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _update_joins_secure()")
    
    async def _create_primary_keys_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Create primary keys securely. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _create_primary_keys_secure()")
    
    async def _fetch_table_operations_secure(self, conn: Any) -> List[Dict[str, Any]]:
        """Fetch table operations securely. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _fetch_table_operations_secure()")


# Example usage
async def main():
    """Example of running the secure importer."""
    importer = SecureBaseDBImporter()
    success = await importer.run_async()
    return success


if __name__ == "__main__":
    # Run the async main function
    result = asyncio.run(main())
    exit(0 if result else 1)