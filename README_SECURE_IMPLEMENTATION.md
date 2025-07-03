# Secure ETL Implementation Guide

## Overview

This document describes the enhanced secure version of the ETL system for migrating supervision data. The secure implementation addresses security vulnerabilities, improves performance, and provides better error handling while maintaining backward compatibility.

## Key Security Enhancements

### 1. SQL Injection Prevention
- **Comprehensive SQL Validation**: All SQL statements are validated before execution using `SQLSecurityValidator`
- **Parameterized Queries**: All dynamic SQL uses parameterized queries to prevent injection attacks
- **Identifier Validation**: Table names, column names, and schema names are validated against strict patterns
- **Risk Assessment**: SQL statements are assigned risk levels (LOW, MEDIUM, HIGH, CRITICAL)

### 2. Secure Configuration Management
- **Encrypted Secrets**: Database passwords and connection strings are stored in the system keyring using `keyring` library
- **No Plaintext Passwords**: The legacy `config/values.json` no longer contains passwords
- **Environment Variable Support**: Sensitive configuration can be provided via environment variables
- **Validation**: All configuration values are validated using Pydantic models

### 3. Enhanced Error Handling
- **Detailed Context**: Errors include processing context (operation, table, row ID)
- **Security Violation Tracking**: Security violations are tracked separately from regular errors
- **Comprehensive Logging**: All operations are logged with correlation IDs for traceability
- **Graceful Degradation**: The system continues processing valid operations even if some fail

### 4. Performance Improvements
- **Asynchronous Processing**: Core operations use async/await for better concurrency
- **Connection Pooling**: Database connections are pooled for better resource utilization
- **Batch Processing**: Large CSV files are processed in configurable chunks
- **Progress Tracking**: Processing state is saved to allow resumption after interruption

## Migration Process

### Prerequisites
```bash
pip install keyring cryptography pydantic pydantic-settings
```

### Step 1: Validate Current System
```bash
python migrate_to_secure_system.py --validate-only
```

### Step 2: Run Migration
```bash
python migrate_to_secure_system.py
```

### Step 3: Verify Migration
The migration script will:
1. Create a timestamped backup in `migration_backup/`
2. Migrate configuration to secure storage
3. Update environment files
4. Validate SQL scripts for security issues
5. Provide a detailed migration summary

### Step 4: Test Secure System
```bash
# Test individual importers
python 01_JusticeDB_Import_Secure.py --verbose
python 02_OperationsDB_Import_Secure.py --verbose
python 03_FinancialDB_Import_Secure.py --verbose

# Or use the GUI
python run_etl.py
```

## Configuration

### Environment Variables
```bash
# Required
MSSQL_TARGET_CONN_STR  # Can be omitted if stored in keyring
EJ_CSV_DIR             # Directory containing CSV files
EJ_LOG_DIR             # Directory for log files

# Optional
SQL_TIMEOUT=300        # SQL operation timeout in seconds
CSV_CHUNK_SIZE=50000   # Rows per chunk for CSV processing
DB_POOL_SIZE=5         # Database connection pool size
MAX_RETRY_ATTEMPTS=3   # Retry attempts for transient failures
```

### Secure Configuration File
The system uses `config/secure_config.json` for non-sensitive settings:
```json
{
  "always_include_tables": ["schema.table1", "schema.table2"],
  "csv_chunk_size": 50000,
  "sql_timeout": 300
}
```

## Security Features

### SQL Validation Rules
1. **Identifier Validation**
   - Must start with letter or underscore
   - Can contain letters, numbers, underscores
   - Maximum 128 characters (SQL Server limit)
   - No SQL keywords allowed

2. **Statement Validation**
   - Dangerous keywords blocked: DROP, TRUNCATE, EXEC, xp_cmdshell
   - Injection patterns detected: UNION SELECT, OR 1=1, comment injection
   - DDL operations require explicit permission

3. **Risk Levels**
   - **LOW**: Safe SELECT statements
   - **MEDIUM**: Complex queries, JOINs
   - **HIGH**: DDL operations (when allowed)
   - **CRITICAL**: Dangerous operations (always blocked)

### Processing Statistics
The secure system tracks:
- Tables processed
- Rows processed
- Security violations
- Errors encountered
- Success/failure rates

## Troubleshooting

### Common Issues

1. **Missing Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Keyring Access Issues**
   - On Linux: Install `python3-keyring` package
   - On Windows: Should work out of the box
   - On macOS: May need to allow keychain access

3. **Connection String Not Found**
   ```bash
   # Store connection string manually
   python -c "from config.secure_settings import SecretManager; SecretManager().store_secret('mssql_connection', 'your_connection_string')"
   ```

4. **High Memory Usage**
   - Reduce `CSV_CHUNK_SIZE` environment variable
   - Increase system memory
   - Process databases sequentially instead of in parallel

### Debug Mode
Enable verbose logging:
```bash
python 01_JusticeDB_Import_Secure.py --verbose
```

### Log Files
- Migration log: `migration.log`
- ETL errors: `PreDMSErrorLog_[Database].txt`
- System logs: Check console output

## Performance Tuning

### Database Connection Pool
```python
# Adjust in environment or config
DB_POOL_SIZE=10        # Increase for more concurrent connections
DB_MAX_OVERFLOW=20     # Maximum overflow connections
DB_POOL_TIMEOUT=30     # Connection timeout in seconds
```

### CSV Processing
```python
# Adjust chunk size based on available memory
CSV_CHUNK_SIZE=100000  # Larger chunks = faster but more memory
```

### SQL Timeouts
```python
# Increase for long-running queries
SQL_TIMEOUT=600  # 10 minutes
```

## Security Best Practices

1. **Regular Updates**
   - Keep dependencies updated
   - Review security logs regularly
   - Update SQL validation rules as needed

2. **Access Control**
   - Limit database user permissions
   - Use read-only connections where possible
   - Implement network-level security

3. **Monitoring**
   - Monitor security violation logs
   - Set up alerts for critical errors
   - Track performance metrics

4. **Backup Strategy**
   - Regular backups before ETL runs
   - Test restore procedures
   - Keep migration backups for rollback

## Rollback Procedure

If issues occur after migration:

1. **Automatic Rollback** (within migration)
   - The migration script attempts automatic rollback on failure

2. **Manual Rollback**
   ```bash
   # Restore from backup
   cp -r migration_backup/[timestamp]/* .
   
   # Remove secure configuration
   rm config/secure_config.json
   
   # Clear keyring entries
   python -c "from config.secure_settings import SecretManager; SecretManager().delete_secret('mssql_connection')"
   ```

3. **Use Legacy System**
   ```bash
   # Original scripts still work
   python 01_JusticeDB_Import.py
   ```

## Fu