"""SQL security validation and sanitization utilities."""

import re
import logging
from typing import Set, List, Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class SQLRiskLevel(Enum):
    """Risk levels for SQL operations."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class SQLValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    risk_level: SQLRiskLevel
    issues: List[str]
    sanitized_sql: Optional[str] = None


class SQLSecurityValidator:
    """Comprehensive SQL security validator."""
    
    # Dangerous SQL keywords that should be restricted in user input
    DANGEROUS_KEYWORDS = frozenset([
        'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE',
        'EXEC', 'EXECUTE', 'OPENROWSET', 'OPENQUERY', 
        'xp_cmdshell', 'sp_configure', 'sp_addlogin',
        'BULK', 'RESTORE', 'BACKUP'
    ])
    
    # System stored procedures that should be blocked
    DANGEROUS_PROCEDURES = frozenset([
        'xp_cmdshell', 'xp_regwrite', 'xp_regread', 'xp_fileexist',
        'sp_configure', 'sp_addlogin', 'sp_password', 'sp_addsrvrolemember'
    ])
    
    # Patterns that indicate potential injection attempts
    INJECTION_PATTERNS = [
        r";\s*(DROP|DELETE|TRUNCATE|ALTER)\s+",  # Statement chaining with dangerous commands
        r"UNION\s+SELECT.*--",                   # Union-based injection
        r"1\s*=\s*1|1\s*'\s*=\s*'1",           # Always true conditions
        r"'\s*OR\s*'.*'\s*=\s*'",              # OR-based injection
        r"--|\*\/|\/\*",                        # Comment injection
        r"char\s*\(\s*\d+\s*\)",               # Character encoding injection
        r"waitfor\s+delay",                     # Time-based injection
    ]
    
    # Valid SQL identifier pattern (letters, numbers, underscores)
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    def __init__(self):
        self.compiled_injection_patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in self.INJECTION_PATTERNS
        ]
    
    def validate_identifier(self, identifier: str) -> SQLValidationResult:
        """Validate a SQL identifier (table name, column name, etc.)."""
        issues = []
        
        if not identifier:
            issues.append("Identifier cannot be empty")
            return SQLValidationResult(False, SQLRiskLevel.HIGH, issues)
        
        if len(identifier) > 128:  # SQL Server limit
            issues.append(f"Identifier too long: {len(identifier)} characters (max 128)")
        
        if not self.IDENTIFIER_PATTERN.match(identifier):
            issues.append(f"Invalid identifier format: {identifier}")
        
        # Check for dangerous keywords
        if identifier.upper() in self.DANGEROUS_KEYWORDS:
            issues.append(f"Identifier matches dangerous keyword: {identifier}")
        
        # Check for reserved words that could cause issues
        sql_reserved_words = {'USER', 'ORDER', 'GROUP', 'SELECT', 'FROM', 'WHERE'}
        if identifier.upper() in sql_reserved_words:
            issues.append(f"Identifier is a SQL reserved word: {identifier}")
        
        risk_level = SQLRiskLevel.LOW
        if issues:
            risk_level = SQLRiskLevel.HIGH if any("dangerous" in issue for issue in issues) else SQLRiskLevel.MEDIUM
        
        return SQLValidationResult(
            is_valid=len(issues) == 0,
            risk_level=risk_level,
            issues=issues,
            sanitized_sql=f"[{identifier}]" if len(issues) == 0 else None
        )
    
    def validate_table_name(self, schema: str, table: str, database: Optional[str] = None) -> SQLValidationResult:
        """Validate and construct safe table names."""
        issues = []
        
        # Validate each component
        schema_result = self.validate_identifier(schema)
        table_result = self.validate_identifier(table)
        
        issues.extend(schema_result.issues)
        issues.extend(table_result.issues)
        
        sanitized_name = None
        if schema_result.is_valid and table_result.is_valid:
            if database:
                db_result = self.validate_identifier(database)
                if db_result.is_valid:
                    sanitized_name = f"[{database}].[{schema}].[{table}]"
                else:
                    issues.extend(db_result.issues)
            else:
                sanitized_name = f"[{schema}].[{table}]"
        
        risk_level = max(
            schema_result.risk_level,
            table_result.risk_level,
            key=lambda x: list(SQLRiskLevel).index(x)
        )
        
        return SQLValidationResult(
            is_valid=len(issues) == 0,
            risk_level=risk_level,
            issues=issues,
            sanitized_sql=sanitized_name
        )
    
    def validate_sql_statement(self, sql: str, allow_ddl: bool = False) -> SQLValidationResult:
        """Comprehensive SQL statement validation."""
        issues = []
        risk_level = SQLRiskLevel.LOW
        
        if not sql or not sql.strip():
            issues.append("SQL statement cannot be empty")
            return SQLValidationResult(False, SQLRiskLevel.HIGH, issues)
        
        sql_upper = sql.upper().strip()
        
        # Check for dangerous keywords in user-provided SQL
        for keyword in self.DANGEROUS_KEYWORDS:
            if keyword in sql_upper:
                if not allow_ddl or keyword in ['EXEC', 'EXECUTE', 'OPENROWSET', 'OPENQUERY']:
                    issues.append(f"Dangerous SQL keyword detected: {keyword}")
                    risk_level = SQLRiskLevel.CRITICAL
        
        # Check for dangerous stored procedures
        for proc in self.DANGEROUS_PROCEDURES:
            if proc.upper() in sql_upper:
                issues.append(f"Dangerous stored procedure detected: {proc}")
                risk_level = SQLRiskLevel.CRITICAL
        
        # Check for injection patterns
        for pattern in self.compiled_injection_patterns:
            if pattern.search(sql):
                issues.append(f"Potential SQL injection pattern detected")
                risk_level = SQLRiskLevel.HIGH
                break
        
        # Check for suspicious character sequences
        if any(seq in sql for seq in ["0x", "CAST(", "CONVERT(", "CHAR("]):
            issues.append("Suspicious encoding functions detected")
            risk_level = max(risk_level, SQLRiskLevel.MEDIUM, key=lambda x: list(SQLRiskLevel).index(x))
        
        # Validate statement structure for SELECT statements
        if sql_upper.startswith('SELECT'):
            select_issues = self._validate_select_statement(sql)
            issues.extend(select_issues)
        
        return SQLValidationResult(
            is_valid=len(issues) == 0,
            risk_level=risk_level,
            issues=issues,
            sanitized_sql=sql if len(issues) == 0 else None
        )
    
    def _validate_select_statement(self, sql: str) -> List[str]:
        """Validate SELECT statement structure."""
        issues = []
        sql_upper = sql.upper()
        
        # Check for required FROM clause (except for certain system queries)
        if 'FROM' not in sql_upper and 'GETDATE()' not in sql_upper and '@@' not in sql_upper:
            issues.append("SELECT statement missing FROM clause")
        
        # Check for UNION without proper validation
        if 'UNION' in sql_upper:
            # Count SELECT statements
            select_count = sql_upper.count('SELECT')
            union_count = sql_upper.count('UNION')
            if select_count != union_count + 1:
                issues.append("UNION statement structure appears malformed")
        
        return issues
    
    def sanitize_dynamic_sql(self, template: str, parameters: Dict[str, Any]) -> SQLValidationResult:
        """Safely sanitize dynamic SQL with parameters."""
        issues = []
        
        # Validate template
        if '{{' not in template or '}}' not in template:
            issues.append("Template missing proper parameter markers")
        
        # Extract parameter placeholders
        import re
        placeholders = re.findall(r'\{\{(\w+)\}\}', template)
        
        # Validate all parameters are provided
        missing_params = set(placeholders) - set(parameters.keys())
        if missing_params:
            issues.append(f"Missing parameters: {missing_params}")
        
        # Validate parameter values
        sanitized_params = {}
        for key, value in parameters.items():
            if key.endswith('_NAME') or key.endswith('_IDENTIFIER'):
                # This is an identifier, validate it
                result = self.validate_identifier(str(value))
                if not result.is_valid:
                    issues.extend([f"Parameter {key}: {issue}" for issue in result.issues])
                else:
                    sanitized_params[key] = result.sanitized_sql.strip('[]')
            else:
                # Regular parameter, basic validation
                sanitized_params[key] = str(value)
        
        # Build final SQL if validation passes
        sanitized_sql = None
        if not issues:
            try:
                sanitized_sql = template.format(**{
                    f"{{{{{key}}}}}": value for key, value in sanitized_params.items()
                })
            except KeyError as e:
                issues.append(f"Template parameter error: {e}")
        
        risk_level = SQLRiskLevel.HIGH if issues else SQLRiskLevel.LOW
        
        return SQLValidationResult(
            is_valid=len(issues) == 0,
            risk_level=risk_level,
            issues=issues,
            sanitized_sql=sanitized_sql
        )


class SafeSQLBuilder:
    """Builder for constructing safe SQL statements."""
    
    def __init__(self):
        self.validator = SQLSecurityValidator()
    
    def build_select_statement(
        self,
        columns: List[str],
        from_table: str,
        schema: str = "dbo",
        database: Optional[str] = None,
        where_clause: Optional[str] = None,
        joins: Optional[List[str]] = None
    ) -> SQLValidationResult:
        """Build a safe SELECT statement."""
        issues = []
        
        # Validate table name
        table_result = self.validator.validate_table_name(schema, from_table, database)
        if not table_result.is_valid:
            return table_result
        
        # Validate column names
        safe_columns = []
        for col in columns:
            if col == "*":
                safe_columns.append("*")
            else:
                col_result = self.validator.validate_identifier(col)
                if not col_result.is_valid:
                    issues.extend([f"Column {col}: {issue}" for issue in col_result.issues])
                else:
                    safe_columns.append(col_result.sanitized_sql)
        
        if issues:
            return SQLValidationResult(False, SQLRiskLevel.HIGH, issues)
        
        # Build the statement
        sql_parts = [
            "SELECT",
            ", ".join(safe_columns),
            "FROM",
            table_result.sanitized_sql
        ]
        
        # Add joins if provided
        if joins:
            for join in joins:
                # Validate join clause (simplified)
                if not re.match(r'^\s*(INNER|LEFT|RIGHT|FULL)\s+JOIN\s+', join.upper()):
                    issues.append(f"Invalid JOIN syntax: {join}")
                else:
                    sql_parts.append(join)
        
        # Add WHERE clause if provided
        if where_clause:
            where_result = self.validator.validate_sql_statement(f"SELECT 1 WHERE {where_clause}")
            if not where_result.is_valid:
                issues.extend([f"WHERE clause: {issue}" for issue in where_result.issues])
            else:
                sql_parts.extend(["WHERE", where_clause])
        
        if issues:
            return SQLValidationResult(False, SQLRiskLevel.HIGH, issues)
        
        final_sql = " ".join(sql_parts)
        
        return SQLValidationResult(
            is_valid=True,
            risk_level=SQLRiskLevel.LOW,
            issues=[],
            sanitized_sql=final_sql
        )
    
    def build_insert_statement(
        self,
        table: str,
        schema: str = "dbo",
        database: Optional[str] = None,
        columns: Optional[List[str]] = None,
        select_statement: Optional[str] = None
    ) -> SQLValidationResult:
        """Build a safe INSERT statement."""
        issues = []
        
        # Validate table name
        table_result = self.validator.validate_table_name(schema, table, database)
        if not table_result.is_valid:
            return table_result
        
        sql_parts = ["INSERT INTO", table_result.sanitized_sql]
        
        # Add column list if provided
        if columns:
            safe_columns = []
            for col in columns:
                col_result = self.validator.validate_identifier(col)
                if not col_result.is_valid:
                    issues.extend([f"Column {col}: {issue}" for issue in col_result.issues])
                else:
                    safe_columns.append(col_result.sanitized_sql)
            
            if issues:
                return SQLValidationResult(False, SQLRiskLevel.HIGH, issues)
            
            sql_parts.append(f"({', '.join(safe_columns)})")
        
        # Add SELECT statement if provided
        if select_statement:
            select_result = self.validator.validate_sql_statement(select_statement)
            if not select_result.is_valid:
                issues.extend([f"SELECT statement: {issue}" for issue in select_result.issues])
            else:
                sql_parts.append(select_result.sanitized_sql)
        
        if issues:
            return SQLValidationResult(False, SQLRiskLevel.HIGH, issues)
        
        final_sql = " ".join(sql_parts)
        
        return SQLValidationResult(
            is_valid=True,
            risk_level=SQLRiskLevel.LOW,
            issues=[],
            sanitized_sql=final_sql
        )


# Convenience functions for easy use
def validate_sql_identifier(identifier: str) -> str:
    """Validate and return a safe SQL identifier."""
    validator = SQLSecurityValidator()
    result = validator.validate_identifier(identifier)
    
    if not result.is_valid:
        raise ValueError(f"Invalid SQL identifier '{identifier}': {'; '.join(result.issues)}")
    
    return result.sanitized_sql.strip('[]')  # Return without brackets for internal use


def validate_table_name(schema: str, table: str, database: Optional[str] = None) -> str:
    """Validate and return a safe table name."""
    validator = SQLSecurityValidator()
    result = validator.validate_table_name(schema, table, database)
    
    if not result.is_valid:
        raise ValueError(f"Invalid table name: {'; '.join(result.issues)}")
    
    return result.sanitized_sql


def validate_sql_statement(sql: str, allow_ddl: bool = False) -> str:
    """Validate and return a safe SQL statement."""
    validator = SQLSecurityValidator()
    result = validator.validate_sql_statement(sql, allow_ddl)
    
    if not result.is_valid:
        raise ValueError(f"Invalid SQL statement: {'; '.join(result.issues)}")
    
    if result.risk_level in [SQLRiskLevel.HIGH, SQLRiskLevel.CRITICAL]:
        logger.warning(f"High-risk SQL statement validated: {result.issues}")
    
    return result.sanitized_sql


# Example usage and testing
if __name__ == "__main__":
    validator = SQLSecurityValidator()
    builder = SafeSQLBuilder()
    
    # Test identifier validation
    test_identifiers = ["ValidTable", "Invalid-Table", "DROP", "MyTable123", "", "a" * 200]
    
    for identifier in test_identifiers:
        result = validator.validate_identifier(identifier)
        print(f"Identifier '{identifier}': Valid={result.is_valid}, Issues={result.issues}")
    
    # Test SQL statement validation
    test_sqls = [
        "SELECT * FROM Users",
        "SELECT * FROM Users; DROP TABLE Users;",
        "SELECT * FROM Users WHERE 1=1",
        "SELECT * FROM Users WHERE Name = 'test' OR '1'='1'",
        "EXEC xp_cmdshell 'dir'"
    ]
    
    for sql in test_sqls:
        result = validator.validate_sql_statement(sql)
        print(f"SQL '{sql}': Valid={result.is_valid}, Risk={result.risk_level.value}")