# EJ Supervision Import - ETL System

A comprehensive ETL (Extract, Transform, Load) system for migrating supervision data from multiple legacy databases (Justice, Operations, Financial) to a consolidated target database.

## Overview

This ETL system handles the migration of supervision-related data including:
- Court cases and criminal charges
- Supervision records and contacts
- Financial transactions and fee instances
- Documents and attachments
- Party information (defendants, victims, attorneys)
- Warrants, hearings, and events

## Features

- **Modular Architecture**: Separate importers for each source database
- **Data Integrity**: Primary key and constraint preservation
- **Selective Migration**: Only migrates data within supervision scope
- **Error Handling**: Comprehensive logging and error recovery
- **Progress Tracking**: Resume capability for interrupted migrations
- **GUI Interface**: User-friendly interface for configuration and execution
- **Security**: Enhanced secure version with SQL injection prevention

## Quick Start

### Prerequisites

- Python 3.8 or higher
- SQL Server with appropriate permissions
- ODBC Driver 17 for SQL Server
- Required Python packages (see Requirements section)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/your-org/ej-supervision-import.git
cd ej-supervision-import
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment:
```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your settings
# Required variables:
# MSSQL_TARGET_CONN_STR=Driver={ODBC Driver 17 for SQL Server};Server=your-server;Database=your-db;UID=user;PWD=password
# EJ_CSV_DIR=/path/to/csv/files
# EJ_LOG_DIR=/path/to/logs
```

### Running the ETL

#### Option 1: GUI Interface (Recommended)
```bash
python run_etl.py
```

The GUI allows you to:
- Enter database connection details
- Test connectivity
- Select CSV directory
- Run individual database imports
- Monitor progress in real-time

#### Option 2: Command Line
```bash
# Run individual importers
python 01_JusticeDB_Import.py
python 02_OperationsDB_Import.py
python 03_FinancialDB_Import.py
python 04_LOBColumns.py
```

#### Option 3: Secure Version
```bash
# For enhanced security (recommended for production)
python migrate_to_secure_system.py
python 01_JusticeDB_Import_Secure.py
```

## Architecture

### System Components

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Justice DB     │     │ Operations DB   │     │  Financial DB   │
│  (Court Data)   │     │  (Documents)    │     │  (Fees/Fines)   │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                         │
         └───────────────────────┴─────────────────────────┘
                                 │
                        ┌────────▼────────┐
                        │   ETL System    │
                        │  ┌───────────┐  │
                        │  │ Importers │  │
                        │  ├───────────┤  │
                        │  │ Validators│  │
                        │  ├───────────┤  │
                        │  │ Security  │  │
                        │  └───────────┘  │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │   Target DB     │
                        │ (Consolidated)  │
                        └─────────────────┘
```

### Directory Structure

```
ej-supervision-import/
├── config/                 # Configuration files
│   ├── settings.py        # Application settings
│   ├── secure_config.json # Stored non-sensitive defaults
│   └── values.json        # Legacy configuration
├── db/                    # Database connectivity
│   ├── mssql.py          # SQL Server connections
│   ├── mysql.py          # MySQL connections
│   └── migrations.py     # Migration tracking
├── etl/                   # Core ETL logic
│   ├── base_importer.py  # Base importer class
│   ├── secure_base_importer.py # Secure base class
│   ├── core.py           # Core utilities
│   └── runner.py         # Script orchestration
├── sql_scripts/          # SQL scripts by database
│   ├── justice/          # Justice DB scripts
│   ├── operations/       # Operations DB scripts
│   ├── financial/        # Financial DB scripts
│   └── lob/              # LOB column scripts
├── utils/                # Utility modules
│   ├── etl_helpers.py    # ETL helper functions
│   ├── logging_helper.py # Logging utilities
│   └── sql_security.py   # SQL security validation
├── tests/                # Unit tests
├── 01_JusticeDB_Import.py    # Justice importer
├── 02_OperationsDB_Import.py # Operations importer
├── 03_FinancialDB_Import.py  # Financial importer
├── 04_LOBColumns.py          # LOB column processor
└── run_etl.py               # GUI application
```

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `MSSQL_TARGET_CONN_STR` | Target database connection string | Yes | - |
| `EJ_CSV_DIR` | Directory containing CSV files | Yes | - |
| `EJ_LOG_DIR` | Directory for log files | No | Current directory |
| `SQL_TIMEOUT` | SQL operation timeout (seconds) | No | 300 |
| `CSV_CHUNK_SIZE` | Rows per chunk for CSV processing | No | 50000 |
| `INCLUDE_EMPTY_TABLES` | Include tables with no data | No | false |
| `FAIL_ON_MISMATCH` | Fail on row count mismatches | No | false |

### Configuration File

Create `config/values.json` for persistent settings:

```json
{
  "driver": "{ODBC Driver 17 for SQL Server}",
  "server": "your-server.database.windows.net,1433",
  "database": "YourDatabase",
  "user": "your-user",
  "password": "your-password",
  "csv_dir": "C:\\ETL\\CSV_Files\\",
  "include_empty_tables": false,
  "always_include_tables": [
    "Justice.dbo.xPartyGrpParty",
    "Justice.dbo.xPartyGrpCase"
  ],
  "ej_log_dir": "C:\\ETL\\Logs\\"
}
```

## ETL Process Flow

### 1. Scope Definition
Each database importer first defines its scope by identifying relevant records:
- **Justice**: Cases, charges, parties, warrants, hearings, events
- **Operations**: Documents linked to supervision records
- **Financial**: Fee instances associated with supervision cases

### 2. Table Preparation
- Generate DROP TABLE statements
- Create SELECT INTO statements with proper joins
- Update scope row counts from CSV files

### 3. Data Migration
- Execute DROP statements for existing tables
- Execute SELECT INTO statements to copy data
- Validate row counts match expectations

### 4. Schema Recreation
- Create primary keys
- Add NOT NULL constraints
- Preserve original table structures

## CSV File Format

The system expects pipe-delimited (|) CSV files with the following columns:
- DatabaseName
- SchemaName
- TableName
- Freq (frequency in source)
- InScopeFreq (frequency in scope)
- fConvert (1 to include, 0 to exclude)
- Drop_IfExists (DROP statement)
- Select_Only (SELECT statement)
- Selection (JOIN criteria)
- Select_Into (full SELECT INTO statement)

## Error Handling

### Log Files
- `PreDMSErrorLog_Justice.txt` - Justice import errors
- `PreDMSErrorLog_Operations.txt` - Operations import errors
- `PreDMSErrorLog_Financial.txt` - Financial import errors
- `migration.log` - System migration log

### Progress Files
The system creates progress files to track completed operations:
- `Justice_progress.json`
- `Operations_progress.json`
- `Financial_progress.json`

To resume an interrupted migration, keep these files and set `RESUME=1`.

## Performance Tuning

### Memory Usage
- Adjust `CSV_CHUNK_SIZE` for large files
- Default: 50,000 rows per chunk
- Increase for better performance, decrease for lower memory usage

### Database Connections
- Connection pooling enabled by default
- Adjust pool size with `DB_POOL_SIZE` (default: 5)
- Maximum overflow: `DB_MAX_OVERFLOW` (default: 10)

### Timeouts
- SQL operations: `SQL_TIMEOUT` (default: 300 seconds)
- Connection timeout: `CONNECTION_TIMEOUT` (default: 30 seconds)

## Security Considerations

### Secure Version Features
- SQL injection prevention
- Encrypted credential storage
- Comprehensive audit logging
- Input validation and sanitization

### Migration to Secure Version
```bash
python migrate_to_secure_system.py
```

See [README_SECURE_IMPLEMENTATION.md](README_SECURE_IMPLEMENTATION.md) for details.

### Best Practices
1. Use minimum required database permissions
2. Store credentials securely (use secure version)
3. Regularly review error logs
4. Validate CSV files before processing
5. Test in non-production environment first

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify ODBC driver installation
   - Check firewall rules
   - Confirm database credentials
   - Test with SQL Server Management Studio

2. **CSV File Not Found**
   - Check `EJ_CSV_DIR` path
   - Ensure files match expected names
   - Verify file permissions

3. **Memory Errors**
   - Reduce `CSV_CHUNK_SIZE`
   - Process databases sequentially
   - Increase system memory

4. **Timeout Errors**
   - Increase `SQL_TIMEOUT`
   - Check for blocking queries
   - Optimize source database indexes

5. **ScopeRowCount Update Error**
   - Versions prior to 1.5.1 could fail with a parameter count mismatch while updating `ScopeRowCount`, resulting in a `COUNT field incorrect or syntax error` during table conversions. Update to the latest code to resolve this issue.

### Debug Mode
Enable verbose logging:
```bash
python 01_JusticeDB_Import.py --verbose
```

## Requirements

### Python Packages
```
pandas>=1.3.0
pyodbc>=4.0.32
sqlalchemy>=1.4.0
tqdm>=4.62.0
python-dotenv>=0.19.0
pydantic>=1.9.0
pydantic-settings>=2.0.0
keyring>=23.0.0  # For secure version
cryptography>=3.4.0  # For secure version
```

### System Requirements
- Windows, Linux, or macOS
- Python 3.8+
- 8GB RAM minimum (16GB recommended)
- ODBC Driver 17 for SQL Server
- Network access to source and target databases

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Coding Standards
- Follow PEP 8
- Add type hints
- Document all functions
- Handle errors gracefully
- Log important operations

## License

Apache License 2.0 - See [LICENSE.txt](LICENSE.txt) for details.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review existing issues on GitHub
3. Create a new issue with:
   - Error messages
   - Log files
   - Environment details
   - Steps to reproduce

## Acknowledgments

- Tyler Technologies, Inc.
- The Apache Software Foundation
- Contributors and maintainers