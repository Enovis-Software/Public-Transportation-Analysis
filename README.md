# Database Sync: SQL Server → PostgreSQL

This script synchronizes both schema and data from a local SQL Server database to a cloud PostgreSQL database.

## 🔒 Security Guarantee

**IMPORTANT: The source (SQL Server) database is READ-ONLY and will NEVER be modified.**

- ✅ Only SELECT queries are executed on SQL Server
- ✅ All write operations (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP) are performed ONLY on PostgreSQL
- ✅ Query validation prevents accidental write operations
- ✅ The source database remains completely unchanged and safe

## Features

- **Automatic Schema Sync**: Detects and creates/updates tables in PostgreSQL based on SQL Server schema
- **Data Type Mapping**: Automatically converts SQL Server data types to PostgreSQL equivalents
- **Incremental Sync**: Uses UPSERT (INSERT ... ON CONFLICT) to handle updates efficiently
- **All Tables**: Automatically discovers and syncs all user tables
- **Error Handling**: Robust error handling with detailed logging
- **Primary Key Support**: Preserves primary key constraints
- **Comprehensive Logging**: Detailed connection verification, operation tracking, and progress reporting

## Prerequisites

1. **Python 3.7+** (Download from [python.org](https://www.python.org/downloads/))
2. **ODBC Driver for SQL Server** (Windows):
   - Download and install from [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
   - Common drivers: "ODBC Driver 18 for SQL Server" or "ODBC Driver 17 for SQL Server"
   - To check installed drivers: Open "ODBC Data Source Administrator" (odbcad32.exe)
3. **PostgreSQL client libraries** (automatically installed with psycopg2-binary)

## Installation (Windows)

1. **Install Python dependencies:**
   ```cmd
   pip install -r requirements.txt
   ```

2. **Set up environment variables** (choose one method):

   **Method A: Using setup_env.bat (Recommended for testing)**
   - Edit `setup_env.bat` and replace placeholder values with your actual credentials
   - Run `setup_env.bat` before running the sync script
   - Note: Variables set this way only last for the current command prompt session

   **Method B: System Environment Variables (Recommended for scheduled tasks)**
   - Open "System Properties" → "Environment Variables"
   - Add the following variables:
     ```
     SQL_SERVER_DRIVER=ODBC Driver 18 for SQL Server
     SQL_SERVER_HOST=localhost
     SQL_SERVER_DB=YourDatabaseName
     SQL_SERVER_USER=your_username
     SQL_SERVER_PASSWORD=your_password
     
     PG_HOST=your-postgres-host.com
     PG_PORT=5432
     PG_DB=your_database_name
     PG_USER=your_username
     PG_PASSWORD=your_password
     ```

   **Method C: Command Prompt (Temporary)**
   ```cmd
   set SQL_SERVER_HOST=localhost
   set SQL_SERVER_DB=YourDatabase
   set SQL_SERVER_USER=your_user
   set SQL_SERVER_PASSWORD=your_password
   
   set PG_HOST=your-postgres-host.com
   set PG_DB=your_database
   set PG_USER=your_user
   set PG_PASSWORD=your_password
   ```

3. **Windows Authentication (Optional):**
   If you want to use Windows Authentication for SQL Server:
   - Set `SQL_SERVER_USE_WINDOWS_AUTH=true`
   - You don't need to set `SQL_SERVER_USER` and `SQL_SERVER_PASSWORD`

## Usage (Windows)

### Run Once

**Option 1: Using the batch file (Easiest)**
```cmd
run_sync.bat
```

**Option 2: Direct Python execution**
```cmd
python main.py
```

**Note:** Make sure environment variables are set before running (see Installation step 2).

### Run on Schedule (Windows Task Scheduler)

1. **Open Task Scheduler** (search for "Task Scheduler" in Start menu)

2. **Create Basic Task:**
   - Right-click "Task Scheduler Library" → "Create Basic Task"
   - Name: "Database Sync"
   - Description: "Sync SQL Server to PostgreSQL"

3. **Set Trigger:**
   - Choose "Daily" or "When the computer starts" or "When I log on"
   - For hourly sync, choose "Daily" and set to repeat every hour

4. **Set Action:**
   - Action: "Start a program"
   - Program/script: `C:\Python3x\python.exe` (or full path to your Python executable)
   - Add arguments: `main.py`
   - Start in: `C:\path\to\Local-Cloud-Db-Sync` (full path to script directory)

5. **Configure Settings:**
   - Check "Run whether user is logged on or not"
   - Check "Run with highest privileges" (if needed)
   - Configure for: Windows 10/11

6. **Set Environment Variables:**
   - In Task Scheduler, edit the task → "Actions" tab → Edit action
   - Add environment variables in "Start in" or use "Environment Variables" in task settings
   - **OR** set them as System Environment Variables (recommended)

**Alternative: Use run_sync.bat in Task Scheduler**
- Program/script: `C:\path\to\Local-Cloud-Db-Sync\run_sync.bat`
- Start in: `C:\path\to\Local-Cloud-Db-Sync`

## How It Works

1. **Connection Verification**: Establishes and verifies connections to both databases with detailed logging
2. **Schema Detection**: Reads table schemas from SQL Server's `INFORMATION_SCHEMA` (READ-ONLY)
3. **Type Mapping**: Converts SQL Server data types to PostgreSQL equivalents
4. **Table Creation**: Creates tables in PostgreSQL if they don't exist (only writes to PostgreSQL)
5. **Data Sync**: Transfers all data using batch inserts with UPSERT logic (reads from SQL Server, writes to PostgreSQL)
6. **Logging**: Writes comprehensive logs to `db_sync.log` and console with progress tracking

## Data Type Mappings

Common mappings include:
- `bigint` → `BIGINT`
- `int` → `INTEGER`
- `varchar` → `VARCHAR`
- `datetime` → `TIMESTAMP`
- `uniqueidentifier` → `UUID`
- `text` → `TEXT`
- And many more...

## Logging

The script provides comprehensive logging for all operations:

**Connection Logging:**
- Detailed connection information for both databases
- Connection verification with version checks
- Authentication method confirmation
- Success/failure indicators

**Operation Logging:**
- Table discovery and listing
- Schema reading progress
- Table creation/verification status
- Data sync progress with row counts and sync rates
- Batch processing updates

**Summary Reports:**
- Synchronization summary with success/failure counts
- List of successfully synced tables
- List of failed tables with error messages
- Total duration and performance metrics

**Log Output:**
- Console (stdout) - Real-time progress
- File: `db_sync.log` - Persistent log file

Log levels include INFO, WARNING, and ERROR.

## Troubleshooting (Windows)

### Connection Issues

**SQL Server Connection:**
- Verify ODBC driver is installed: Open "ODBC Data Source Administrator" (search in Start menu)
- Check driver name matches exactly (e.g., "ODBC Driver 18 for SQL Server")
- For Windows Authentication: Ensure your Windows user has SQL Server access
- Test connection: Use SQL Server Management Studio (SSMS) to verify credentials
- If using named instance: Use `SERVERNAME\INSTANCENAME` format

**PostgreSQL Connection:**
- Verify network connectivity: `ping your-postgres-host.com`
- Check firewall settings (port 5432)
- Verify credentials are correct
- Test connection: Use pgAdmin or `psql` command-line tool

**Environment Variables:**
- Verify variables are set: Open Command Prompt and type `set SQL_SERVER_HOST`
- For Task Scheduler: Variables must be System Environment Variables, not User variables
- Restart Command Prompt/Task Scheduler after setting variables

**Common Errors:**
- `[Microsoft][ODBC Driver Manager] Data source name not found`: ODBC driver not installed or name mismatch
- `Login failed for user`: Incorrect credentials or Windows Authentication issue
- `Connection timeout`: Network/firewall issue or incorrect host/port

### Type Conversion Issues
- Check logs for specific column type errors
- Some SQL Server types may need manual adjustment in the `DataTypeMapper` class

### Performance
- Large tables are processed in batches (default: 1000 rows)
- Adjust `batch_size` in `sync_table_data()` if needed
- Consider adding indexes on PostgreSQL side for better performance

## Notes

- The script uses UPSERT (INSERT ... ON CONFLICT) for tables with primary keys
- Tables without primary keys will use simple INSERT (may create duplicates)
- Schema changes are detected but existing tables are not altered (only created if missing)
- For schema updates, you may need to manually alter PostgreSQL tables or drop/recreate them

## License

MIT License - feel free to modify for your needs.

