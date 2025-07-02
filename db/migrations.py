from __future__ import annotations
import sqlalchemy
from typing import Any, Optional

VERSION_TABLE = "MigrationHistory"


def _execute(
    conn: Any,
    sql: str,
    params: Optional[tuple[Any, ...]] = None,
    fetch: bool = False,
) -> Any:
    """Execute SQL using the provided connection."""
    # SQLAlchemy connection
    if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
        if params:
            # Convert positional parameters to named parameters for SQLAlchemy
            # Replace ? with :param1, :param2, etc.
            named_sql = sql
            param_dict = {}
            param_count = 0
            
            while '?' in named_sql:
                param_count += 1
                param_name = f"param{param_count}"
                named_sql = named_sql.replace('?', f":{param_name}", 1)
                
                # Build dictionary with param values
                if param_count <= len(params):
                    param_dict[param_name] = params[param_count-1]
            
            result = conn.execute(sqlalchemy.text(named_sql), param_dict)
        else:
            result = conn.execute(sqlalchemy.text(sql))
        
        if fetch:
            try:
                # For SQLAlchemy 1.4+ which returns CursorResult objects
                if hasattr(result, "mappings"):
                    return list(result.mappings())
                # For older SQLAlchemy versions
                return result.fetchall()
            except Exception:
                return None
        return None
    # DB-API connection remains unchanged
    else:
        with conn.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            result = None
            if fetch:
                try:
                    result = cur.fetchall()
                except Exception:
                    result = None
            conn.commit()
            return result


def ensure_version_table(conn: Any) -> None:
    """Ensure the version tracking table exists with ROWID as PK."""
    # Check if table exists with NOLOCK to prevent blocking
    check_sql = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WITH (NOLOCK) WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{VERSION_TABLE}'"
    table_exists = _execute(conn, check_sql, fetch=True)
    
    if not table_exists:
        # Create new table with ROWID as primary key
        create_sql = f"""
        CREATE TABLE dbo.{VERSION_TABLE}(
            ROWID INT IDENTITY(1,1) PRIMARY KEY,
            script_name NVARCHAR(255) NOT NULL,
            applied_at DATETIME DEFAULT GETDATE()
        )
        """
        _execute(conn, create_sql)
    else:
        # Check if we need to modify the table structure
        check_col_sql = f"SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK) WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{VERSION_TABLE}' AND COLUMN_NAME = 'ROWID'"
        rowid_exists = _execute(conn, check_col_sql, fetch=True)
        
        if not rowid_exists:
            # Get PK constraint name
            pk_sql = f"""
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WITH (NOLOCK)
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{VERSION_TABLE}'
            AND CONSTRAINT_TYPE = 'PRIMARY KEY'
            """
            pk_result = _execute(conn, pk_sql, fetch=True)
            if pk_result:
                pk_name = pk_result[0][0]
                
                # Drop the existing PK constraint
                _execute(conn, f"ALTER TABLE dbo.{VERSION_TABLE} DROP CONSTRAINT {pk_name}")
            
            # Add ROWID column
            _execute(conn, f"ALTER TABLE dbo.{VERSION_TABLE} ADD ROWID INT IDENTITY(1,1) PRIMARY KEY")
            
            # Add unique constraint on script_name if it doesn't already have one
            _execute(conn, f"ALTER TABLE dbo.{VERSION_TABLE} ADD CONSTRAINT UQ_{VERSION_TABLE}_script_name UNIQUE (script_name)")


def has_migration(conn: Any, name: str) -> bool:
    """Return True if the given migration name is recorded as applied."""
    # Use READ UNCOMMITTED to prevent blocking on any pending transactions
    sql = f"SELECT 1 FROM dbo.{VERSION_TABLE} WITH (NOLOCK) WHERE script_name = ?"
    result = _execute(conn, sql, (name,), fetch=True)
    return bool(result)


def record_migration(conn: Any, migration_name: str) -> None:
    """Record that a migration has been applied."""
    ensure_version_table(conn)
    
    sql = f"INSERT INTO dbo.{VERSION_TABLE} (script_name) VALUES (?)"
    
    # Check if we're dealing with a SQLAlchemy connection
    if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
        # For SQLAlchemy connections
        # Convert ? style parameter to SQLAlchemy named parameter format
        named_sql = sql.replace('?', ':param1')
        param_dict = {"param1": migration_name}
        
        try:
            # Execute within the existing connection
            conn.execute(sqlalchemy.text(named_sql), param_dict)
            
            # Commit if this connection can be committed directly
            if hasattr(conn, "commit") and callable(conn.commit):
                conn.commit()
        except Exception as e:
            # If there was an error and we can rollback, do so
            if hasattr(conn, "rollback") and callable(conn.rollback):
                conn.rollback()
            raise e
    else:
        # For DB-API connections
        _execute(conn, sql, (migration_name,))
        # The _execute function already commits for DB-API connections