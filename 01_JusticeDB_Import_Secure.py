"""Secure Justice database import implementation."""

import logging
import asyncio
from typing import Any, Dict, List
import tkinter as tk
from tkinter import messagebox
from etl.secure_base_importer import SecureBaseDBImporter, ProcessingContext, SecureETLException
from utils.etl_helpers import load_sql, run_sql_script
from utils.sql_security import validate_sql_statement, SQLRiskLevel
import sqlalchemy

logger = logging.getLogger(__name__)


class SecureJusticeDBImporter(SecureBaseDBImporter):
    """Secure Justice database import implementation."""
    
    DB_TYPE = "Justice"
    DEFAULT_LOG_FILE = "PreDMSErrorLog_Justice.txt"
    DEFAULT_CSV_FILE = "EJ_Justice_Selects_ALL.csv"
    
    async def execute_preprocessing_async(self, conn: Any, context: ProcessingContext) -> None:
        """Execute Justice-specific preprocessing with security validation."""
        context.operation_name = "justice_preprocessing"
        logger.info("Defining supervision scope for Justice DB...")
        
        # Define the preprocessing steps for Justice DB
        preprocessing_steps = [
            {
                'name': 'GatherCaseIDs',
                'script': 'justice/gather_caseids.sql',
                'description': 'Gather supervision and court case IDs'
            },
            {
                'name': 'GatherChargeIDs',
                'script': 'justice/gather_chargeids.sql',
                'description': 'Gather charge IDs for supervision cases'
            },
            {
                'name': 'GatherPartyIDs',
                'script': 'justice/gather_partyids.sql',
                'description': 'Gather party IDs (defendants, associates, victims, etc.)'
            },
            {
                'name': 'GatherWarrantIDs',
                'script': 'justice/gather_warrantids.sql',
                'description': 'Gather warrant IDs for supervision cases'
            },
            {
                'name': 'GatherHearingIDs',
                'script': 'justice/gather_hearingids.sql',
                'description': 'Gather hearing IDs for supervision cases'
            },
            {
                'name': 'GatherEventIDs',
                'script': 'justice/gather_eventids.sql',
                'description': 'Gather event IDs for supervision cases'
            }
        ]
        
        try:
            for step in preprocessing_steps:
                step_context = ProcessingContext(
                    f"preprocessing_{step['name']}",
                    database_name=self.settings.mssql_target_db_name
                )
                
                await self._execute_preprocessing_step_secure(conn, step, step_context)
            
            logger.info("All Justice preprocessing steps completed successfully. Supervision scope defined.")
            
        except Exception as e:
            raise SecureETLException(
                f"Justice preprocessing failed: {e}",
                context,
                original_error=e
            )
    
    async def _execute_preprocessing_step_secure(
        self, 
        conn: Any, 
        step: Dict[str, str], 
        context: ProcessingContext
    ) -> None:
        """Execute a single preprocessing step with security validation."""
        
        logger.info(f"Executing step: {step['name']} - {step['description']}")
        
        try:
            # Load SQL script
            sql_content = load_sql(step['script'], self.settings.mssql_target_db_name)
            
            # Validate SQL for security issues
            validation_result = self.sql_validator.validate_sql_statement(sql_content, allow_ddl=True)
            
            if not validation_result.is_valid:
                raise SecureETLException(
                    f"SQL validation failed for {step['name']}: {'; '.join(validation_result.issues)}",
                    context,
                    security_violation=True
                )
            
            # Log security warnings for high-risk operations
            if validation_result.risk_level in [SQLRiskLevel.HIGH, SQLRiskLevel.CRITICAL]:
                logger.warning(f"High-risk SQL in {step['name']}: {validation_result.issues}")
                context.risk_level = validation_result.risk_level
            
            # Execute the validated SQL
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: run_sql_script(
                    conn, 
                    step['name'], 
                    validation_result.sanitized_sql, 
                    self.settings.sql_timeout
                )
            )
            
            logger.debug(f"Step {step['name']} completed successfully")
            
        except Exception as e:
            if isinstance(e, SecureETLException):
                raise
            else:
                raise SecureETLException(
                    f"Preprocessing step {step['name']} failed: {e}",
                    context,
                    original_error=e
                )
    
    async def _update_joins_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Update Justice table joins with security validation."""
        context.operation_name = "justice_update_joins"
        logger.info("Updating JOINS in TablesToConvert for Justice tables")
        
        try:
            # Load the update joins script
            update_joins_sql = load_sql('justice/update_joins.sql', self.settings.mssql_target_db_name)
            
            # Validate the SQL script
            validation_result = self.sql_validator.validate_sql_statement(update_joins_sql, allow_ddl=True)
            
            if not validation_result.is_valid:
                raise SecureETLException(
                    f"Update joins SQL validation failed: {'; '.join(validation_result.issues)}",
                    context,
                    security_violation=True
                )
            
            # Execute the validated SQL
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: run_sql_script(
                    conn, 
                    'justice_update_joins', 
                    validation_result.sanitized_sql, 
                    self.settings.sql_timeout
                )
            )
            
            logger.info("Justice table joins updated successfully")
            
        except Exception as e:
            if isinstance(e, SecureETLException):
                raise
            else:
                raise SecureETLException(
                    f"Update joins failed: {e}",
                    context,
                    original_error=e
                )
    
    async def _create_primary_keys_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Create primary keys for Justice tables with security validation."""
        context.operation_name = "justice_create_primary_keys"
        logger.info("Creating primary keys and constraints for Justice tables")
        
        try:
            # Generate primary key scripts
            await self._generate_pk_scripts_secure(conn, context)
            
            # Fetch and execute primary key operations
            pk_operations = await self._fetch_pk_operations_secure(conn)
            
            successful = 0
            failed = 0
            
            for operation in pk_operations:
                pk_context = ProcessingContext(
                    "create_primary_key",
                    table_name=operation.get("TableName"),
                    schema_name=operation.get("SchemaName")
                )
                
                try:
                    if await self._execute_pk_operation_secure(conn, operation, pk_context):
                        successful += 1
                    else:
                        failed += 1
                        
                except SecureETLException as e:
                    if e.security_violation:
                        logger.critical(f"Security violation in PK creation: {e}")
                        self.processing_stats["security_violations"] += 1
                    failed += 1
            
            logger.info(f"Primary key creation completed: {successful} successful, {failed} failed")
            
        except Exception as e:
            if isinstance(e, SecureETLException):
                raise
            else:
                raise SecureETLException(
                    f"Primary key creation failed: {e}",
                    context,
                    original_error=e
                )
    
    async def _generate_pk_scripts_secure(self, conn: Any, context: ProcessingContext) -> None:
        """Generate primary key scripts with security validation."""
        logger.info("Generating primary key scripts for Justice tables")
        
        try:
            # Load the PK generation script
            pk_script_sql = load_sql('justice/create_primarykeys.sql', self.settings.mssql_target_db_name)
            
            # Validate the script
            validation_result = self.sql_validator.validate_sql_statement(pk_script_sql, allow_ddl=True)
            
            if not validation_result.is_valid:
                raise SecureETLException(
                    f"PK script validation failed: {'; '.join(validation_result.issues)}",
                    context,
                    security_violation=True
                )
            
            # Execute the script to generate PK definitions
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: run_sql_script(
                    conn, 
                    'create_primarykeys_justice', 
                    validation_result.sanitized_sql, 
                    self.settings.sql_timeout
                )
            )
            
            logger.debug("Primary key scripts generated successfully")
            
        except Exception as e:
            if isinstance(e, SecureETLException):
                raise
            else:
                raise SecureETLException(
                    f"PK script generation failed: {e}",
                    context,
                    original_error=e
                )
    
    async def _fetch_table_operations_secure(self, conn: Any) -> List[Dict[str, Any]]:
        """Fetch Justice table operations with security validation."""
        table_name = "TablesToConvert"  # Justice uses the base name
        db_name = self.settings.mssql_target_db_name
        
        # Build secure query
        query_result = self.sql_builder.build_select_statement(
            columns=[
                "RowID", "DatabaseName", "SchemaName", "TableName", 
                "fConvert", "ScopeRowCount", "Drop_IfExists", "Select_Into", "Joins"
            ],
            from_table=table_name,
            schema="dbo",
            database=db_name,
            where_clause="fConvert = 1"
        )
        
        if not query_result.is_valid:
            raise SecureETLException(
                f"Table operations query validation failed: {'; '.join(query_result.issues)}",
                ProcessingContext("fetch_table_operations"),
                security_violation=True
            )
        
        try:
            # Execute the secure query
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: conn.execute(sqlalchemy.text(query_result.sanitized_sql))
            )
            
            # Convert to list of dictionaries
            columns = result.keys()
            rows = result.fetchall()
            
            operations = [dict(zip(columns, row)) for row in rows]
            
            # Add debug logging for Bond table
            table_names = [f"{op.get('SchemaName', '')}.{op.get('TableName', '')}" for op in operations]
            logger.info(f"Tables to process: {', '.join(table_names)}")
            
            bond_tables = [name for name in table_names if name.lower().endswith('.bond')]
            if bond_tables:
                logger.info(f"Bond tables found: {', '.join(bond_tables)}")
            else:
                logger.warning("No Bond tables found in operations list!")
            
            return operations
            
        except Exception as e:
            raise SecureETLException(
                f"Failed to fetch table operations: {e}",
                ProcessingContext("fetch_table_operations"),
                original_error=e
            )
    
    async def _fetch_pk_operations_secure(self, conn: Any) -> List[Dict[str, Any]]:
        """Fetch primary key operations with security validation."""
        pk_table = "PrimaryKeyScripts"  # Justice uses base name
        tables_table = "TablesToConvert"
        db_name = self.settings.mssql_target_db_name
        
        # Build secure query with CTEs
        query = f"""
        WITH CTE_PKS AS (
            SELECT 1 AS TYPEY, S.DatabaseName, S.SchemaName, S.TableName, S.Script
            FROM [{db_name}].dbo.[{pk_table}] S
            WHERE S.ScriptType='NOT_NULL'
            UNION
            SELECT 2 AS TYPEY, S.DatabaseName, S.SchemaName, S.TableName, S.Script
            FROM [{db_name}].dbo.[{pk_table}] S
            WHERE S.ScriptType='PK'
        )
        SELECT S.TYPEY, TTC.ScopeRowCount, S.DatabaseName, S.SchemaName, S.TableName,
               REPLACE(S.Script, 'FLAG NOT NULL', 'BIT NOT NULL') AS Script, TTC.fConvert
        FROM CTE_PKS S
        INNER JOIN [{db_name}].dbo.[{tables_table}] TTC WITH (NOLOCK)
            ON S.SCHEMANAME=TTC.SchemaName AND S.TABLENAME=TTC.TableName
        WHERE TTC.fConvert=1
        ORDER BY S.SCHEMANAME, S.TABLENAME, S.TYPEY
        """
        
        # Validate the query
        validation_result = self.sql_validator.validate_sql_statement(query)
        
        if not validation_result.is_valid:
            raise SecureETLException(
                f"PK operations query validation failed: {'; '.join(validation_result.issues)}",
                ProcessingContext("fetch_pk_operations"),
                security_violation=True
            )
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: conn.execute(sqlalchemy.text(validation_result.sanitized_sql))
            )
            
            columns = result.keys()
            rows = result.fetchall()
            
            operations = [dict(zip(columns, row)) for row in rows]
            
            logger.debug(f"Fetched {len(operations)} PK operations")
            return operations
            
        except Exception as e:
            raise SecureETLException(
                f"Failed to fetch PK operations: {e}",
                ProcessingContext("fetch_pk_operations"),
                original_error=e
            )
    
    async def _execute_pk_operation_secure(
        self, 
        conn: Any, 
        operation: Dict[str, Any], 
        context: ProcessingContext
    ) -> bool:
        """Execute a primary key operation with security validation."""
        
        try:
            script_sql = operation.get('Script', '')
            scope_row_count = operation.get('ScopeRowCount', 0)
            schema_name = operation.get('SchemaName', 'dbo')
            table_name = operation.get('TableName', '')
            
            if not script_sql or not table_name:
                logger.warning("Skipping PK operation with missing SQL or table name")
                return False
            
            # Validate identifiers
            safe_schema = self.sql_validator.validate_identifier(schema_name)
            safe_table = self.sql_validator.validate_identifier(table_name)
            
            if not safe_schema.is_valid or not safe_table.is_valid:
                raise SecureETLException(
                    f"Invalid identifiers - Schema: {safe_schema.issues}, Table: {safe_table.issues}",
                    context,
                    security_violation=True
                )
            
            # Validate the PK script
            script_validation = self.sql_validator.validate_sql_statement(script_sql, allow_ddl=True)
            
            if not script_validation.is_valid:
                raise SecureETLException(
                    f"PK script validation failed: {'; '.join(script_validation.issues)}",
                    context,
                    security_violation=True
                )
            
            # Check if table should be processed
            if not self._should_process_table_secure(scope_row_count, schema_name, table_name):
                logger.debug(f"Skipping PK creation for {schema_name}.{table_name} (scope: {scope_row_count})")
                return True
            
            # Execute the validated script
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: conn.execute(sqlalchemy.text(script_validation.sanitized_sql))
            )
            
            await asyncio.get_event_loop().run_in_executor(None, conn.commit)
            
            logger.debug(f"PK operation completed for {schema_name}.{table_name}")
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
                    f"PK operation failed: {e}",
                    context,
                    original_error=e
                )

    async def _process_table_operation_secure(
        self,
        conn: Any,
        operation: Dict[str, Any],
        context: ProcessingContext
    ) -> bool:
        """Process table operation with security validation and debugging for Bond table."""
        
        table_name = operation.get("TableName", "")
        schema_name = operation.get("SchemaName", "")
        
        # Log every table being processed
        logger.debug(f"Processing table: {schema_name}.{table_name}")
        
        # Debug breakpoint for Bond table
        if table_name and table_name.lower() == "bond":
            logger.info(f"BOND TABLE DETECTED: {schema_name}.{table_name}")
            
            # Collect SQL information
            drop_sql = operation.get("Drop_IfExists", "")
            select_sql = operation.get("Select_Into", "")
            
            # Get join information - use the column name that matches your DB schema
            join_sql = operation.get("Joins", "")
            if not join_sql:
                try:
                    # Query for join information
                    join_query = f"""
                        SELECT Joins 
                        FROM {self.settings.mssql_target_db_name}.dbo.TablesToConvert 
                        WHERE SchemaName = '{schema_name}' AND TableName = '{table_name}'
                    """
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: conn.execute(sqlalchemy.text(join_query))
                    )
                    join_data = result.fetchone()
                    join_sql = join_data[0] if join_data else "N/A"
                except Exception as e:
                    logger.warning(f"Failed to fetch join info for Bond table: {e}")
                    join_sql = "Error fetching join information"
            
            full_sql = f"{select_sql} {join_sql}"
            
            # Show debug message box
            try:
                root = tk.Tk()
                root.withdraw()
                debug_info = (
                    f"BOND TABLE PROCESSING BREAKPOINT\n\n"
                    f"Schema: {schema_name}\n"
                    f"Table: {table_name}\n\n"
                    f"DROP SQL:\n{drop_sql}\n\n"
                    f"SELECT INTO SQL:\n{select_sql}\n\n" 
                    f"JOIN SQL:\n{join_sql}\n\n"
                    f"COMBINED SQL:\n{full_sql}"
                )
                proceed = messagebox.askokcancel(
                    "Bond Table Debug", 
                    debug_info,
                    width=1000
                )
                root.destroy()
                
                if not proceed:
                    logger.warning("User aborted processing at Bond table")
                    return False
            except Exception as e:
                logger.error(f"Error showing Bond table debug dialog: {e}")
        
        # Call the parent implementation
        return await super()._process_table_operation_secure(conn, operation, context)


def main():
    """Main entry point for secure Justice DB Import."""
    from utils.logging_helper import setup_logging, operation_counts
    
    setup_logging()
    
    async def run_async():
        importer = SecureJusticeDBImporter()
        return await importer.run_async()
    
    try:
        success = asyncio.run(run_async())
        
        logger.info(
            f"Justice DB import completed - Success: {success}, "
            f"Operations - Success: {operation_counts['success']}, "
            f"Failures: {operation_counts['failure']}"
        )
        
        return success
        
    except KeyboardInterrupt:
        logger.info("Justice DB import interrupted by user")
        return False
    except Exception as e:
        logger.exception("Unexpected error in Justice DB import")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)