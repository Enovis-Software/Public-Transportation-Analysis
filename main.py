#!/usr/bin/env python3
"""
Database Sync Script: SQL Server (Local) -> PostgreSQL (Cloud)
Syncs both schema and data from local SQL Server to cloud PostgreSQL.

IMPORTANT SECURITY NOTE:
========================
This script is designed to be READ-ONLY on the source (SQL Server) database.
The source database is NEVER modified in any way. Only SELECT queries are executed
on SQL Server to read schema metadata and data. All write operations (INSERT, UPDATE,
DELETE, CREATE, ALTER, DROP, TRUNCATE) are performed ONLY on the destination
(PostgreSQL) database.

The source database remains completely unchanged and safe.
"""

import os
import logging
import pyodbc
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import sys
import platform

# File locking imports (platform-specific)
try:
    if platform.system() == 'Windows':
        import msvcrt
    else:
        import fcntl
except ImportError:
    pass  # Locking will be disabled if imports fail

# Configure logging with Windows-compatible paths
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'db_sync.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataTypeMapper:
    """Maps SQL Server data types to PostgreSQL equivalents."""
    
    TYPE_MAPPING = {
        # Numeric types
        'bigint': 'BIGINT',
        'int': 'INTEGER',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',  # PostgreSQL doesn't have tinyint
        'bit': 'BOOLEAN',
        'decimal': 'DECIMAL',
        'numeric': 'NUMERIC',
        'float': 'DOUBLE PRECISION',
        'real': 'REAL',
        'money': 'MONEY',
        'smallmoney': 'MONEY',
        
        # String types
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'text': 'TEXT',
        'nchar': 'CHAR',
        'nvarchar': 'VARCHAR',
        'ntext': 'TEXT',
        
        # Date/Time types
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',
        'smalldatetime': 'TIMESTAMP',
        'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
        'timestamp': 'BYTEA',  # SQL Server timestamp is binary
        
        # Binary types
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'image': 'BYTEA',
        
        # Other types
        'uniqueidentifier': 'UUID',
        'xml': 'XML',
        'json': 'JSONB',
    }
    
    @classmethod
    def map_type(cls, sql_server_type: str, max_length: Optional[int] = None, 
                 precision: Optional[int] = None, scale: Optional[int] = None) -> str:
        """
        Map SQL Server data type to PostgreSQL.
        
        Args:
            sql_server_type: SQL Server data type name
            max_length: Maximum length for string/binary types
            precision: Precision for numeric types
            scale: Scale for numeric types
            
        Returns:
            PostgreSQL data type string
        """
        base_type = sql_server_type.lower().split('(')[0].strip()
        pg_type = cls.TYPE_MAPPING.get(base_type, 'TEXT')  # Default to TEXT if unknown
        
        # Handle length/precision/scale
        if base_type in ['char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary']:
            if max_length == -1:  # MAX in SQL Server
                pg_type = 'TEXT' if 'char' in base_type else 'BYTEA'
            elif max_length:
                pg_type = f"{pg_type}({max_length})"
        elif base_type in ['decimal', 'numeric']:
            if precision and scale:
                pg_type = f"{pg_type}({precision},{scale})"
            elif precision:
                pg_type = f"{pg_type}({precision})"
        
        return pg_type


class DatabaseSync:
    """
    Handles synchronization between SQL Server and PostgreSQL.
    
    IMPORTANT: This class is designed to be READ-ONLY on the source (SQL Server) database.
    All write operations (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP) are performed
    ONLY on the destination (PostgreSQL) database. The source database is never modified.
    """
    
    def __init__(self, sql_server_config: Dict, postgres_config: Dict):
        """
        Initialize database connections.
        
        Args:
            sql_server_config: SQL Server connection parameters (source - READ-ONLY)
            postgres_config: PostgreSQL connection parameters (destination - READ/WRITE)
        """
        self.sql_server_config = sql_server_config
        self.postgres_config = postgres_config
        self.sql_conn = None
        self.pg_conn = None
        self.type_mapper = DataTypeMapper()
        
    def connect(self):
        """
        Establish connections to both databases.
        
        IMPORTANT: SQL Server connection is READ-ONLY. Only SELECT queries are executed.
        All write operations are performed on PostgreSQL only.
        """
        try:
            # SQL Server connection (READ-ONLY SOURCE)
            driver = self.sql_server_config.get('driver', 'ODBC Driver 18 for SQL Server')
            server = self.sql_server_config['server']
            database = self.sql_server_config['database']
            
            logger.info("=" * 60)
            logger.info("ESTABLISHING DATABASE CONNECTIONS")
            logger.info("=" * 60)
            logger.info(f"Connecting to SQL Server (SOURCE - READ-ONLY)...")
            logger.info(f"  Server: {server}")
            logger.info(f"  Database: {database}")
            logger.info(f"  Driver: {driver}")
            
            # Build connection string
            conn_str_parts = [
                f"DRIVER={{{driver}}};",
                f"SERVER={server};",
                f"DATABASE={database};"
            ]
            
            # Windows Authentication support (Integrated Security)
            use_windows_auth = self.sql_server_config.get('use_windows_auth', 'false').lower() == 'true'
            
            if use_windows_auth:
                conn_str_parts.append("Trusted_Connection=yes;")
                logger.info(f"  Authentication: Windows Authentication (Integrated Security)")
            else:
                conn_str_parts.append(f"UID={self.sql_server_config['user']};")
                conn_str_parts.append(f"PWD={self.sql_server_config['password']};")
                logger.info(f"  Authentication: SQL Server Authentication (User: {self.sql_server_config['user']})")
            
            # Add encryption/trust settings for Windows
            conn_str_parts.append("TrustServerCertificate=yes;")
            
            conn_str = ''.join(conn_str_parts)
            self.sql_conn = pyodbc.connect(conn_str)
            
            # Ensure SQL Server connection is read-only by setting autocommit and read-only mode
            # Note: pyodbc doesn't support direct read-only mode, but we ensure only SELECT queries
            # are executed through code validation
            self.sql_conn.autocommit = True  # Read-only connections don't need transactions
            
            # Verify SQL Server connection with a test query
            try:
                cursor = self.sql_conn.cursor()
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]
                cursor.close()
                logger.info(f"✓ SQL Server connection established successfully")
                logger.info(f"  SQL Server Version: {version.split(chr(10))[0] if version else 'Unknown'}")
                logger.info(f"  Mode: READ-ONLY (source database will not be modified)")
            except Exception as e:
                logger.warning(f"SQL Server connection established but version check failed: {e}")
            
            logger.info("")
            logger.info(f"Connecting to PostgreSQL (DESTINATION - READ/WRITE)...")
            logger.info(f"  Host: {self.postgres_config['host']}")
            logger.info(f"  Port: {self.postgres_config.get('port', 5432)}")
            logger.info(f"  Database: {self.postgres_config['database']}")
            logger.info(f"  User: {self.postgres_config['user']}")
            
            # PostgreSQL connection
            self.pg_conn = psycopg2.connect(
                host=self.postgres_config['host'],
                port=self.postgres_config.get('port', 5432),
                database=self.postgres_config['database'],
                user=self.postgres_config['user'],
                password=self.postgres_config['password'],
                connect_timeout=10
            )
            self.pg_conn.autocommit = False
            
            # Verify PostgreSQL connection with a test query
            try:
                pg_cursor = self.pg_conn.cursor()
                pg_cursor.execute("SELECT version()")
                pg_version = pg_cursor.fetchone()[0]
                pg_cursor.execute("SELECT current_database(), current_user")
                db_info = pg_cursor.fetchone()
                pg_cursor.close()
                logger.info(f"✓ PostgreSQL connection established successfully")
                logger.info(f"  PostgreSQL Version: {pg_version.split(',')[0] if pg_version else 'Unknown'}")
                logger.info(f"  Current Database: {db_info[0] if db_info else 'Unknown'}")
                logger.info(f"  Current User: {db_info[1] if db_info else 'Unknown'}")
                logger.info(f"  Mode: READ/WRITE (destination database will be modified)")
            except Exception as e:
                logger.warning(f"PostgreSQL connection established but version check failed: {e}")
            
            logger.info("")
            logger.info("=" * 60)
            logger.info("CONNECTION VERIFICATION COMPLETE")
            logger.info("=" * 60)
            logger.info("")
            
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            logger.error("Connection failed. Please check:")
            logger.error("  - Network connectivity")
            logger.error("  - Database credentials")
            logger.error("  - Firewall settings")
            logger.error("  - Database server status")
            raise
    
    def disconnect(self):
        """Close database connections."""
        logger.info("")
        logger.info("Closing database connections...")
        if self.sql_conn:
            try:
                self.sql_conn.close()
                logger.info("✓ Disconnected from SQL Server")
            except Exception as e:
                logger.warning(f"Error closing SQL Server connection: {e}")
        if self.pg_conn:
            try:
                self.pg_conn.close()
                logger.info("✓ Disconnected from PostgreSQL")
            except Exception as e:
                logger.warning(f"Error closing PostgreSQL connection: {e}")
        logger.info("")
    
    def _validate_read_only_query(self, query: str):
        """
        Validate that a query is read-only (SELECT only).
        
        Raises ValueError if query contains write operations.
        
        Args:
            query: SQL query string to validate
            
        Raises:
            ValueError: If query contains write operations
        """
        query_upper = query.strip().upper()
        write_keywords = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 
                         'TRUNCATE', 'MERGE', 'EXEC', 'EXECUTE', 'CALL']
        
        # Check if query starts with SELECT (or is a SELECT statement)
        if not query_upper.startswith('SELECT'):
            # Check for write operations
            for keyword in write_keywords:
                if keyword in query_upper:
                    raise ValueError(
                        f"SECURITY ERROR: Attempted write operation '{keyword}' on source database. "
                        f"Source database is READ-ONLY. Query: {query[:100]}..."
                    )
    
    def _quote_sql_server_identifier(self, identifier: str) -> str:
        """
        Quote SQL Server identifier using square brackets (SQL Server syntax).
        
        Args:
            identifier: Unquoted identifier (schema, table, or column name)
            
        Returns:
            Quoted identifier: [identifier]
        """
        # SQL Server uses square brackets for identifiers
        # Handle cases where identifier might already be quoted
        identifier = identifier.strip()
        if identifier.startswith('[') and identifier.endswith(']'):
            return identifier
        return f"[{identifier}]"
    
    def get_sql_server_tables(self) -> List[str]:
        """
        Get list of all user tables from SQL Server.
        
        READ-ONLY operation: Only queries INFORMATION_SCHEMA (metadata).
        """
        logger.info("Discovering tables in SQL Server database...")
        cursor = self.sql_conn.cursor()
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        tables = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        logger.info(f"✓ Found {len(tables)} table(s) in SQL Server")
        if tables:
            logger.info(f"  Tables: {', '.join(tables[:10])}{'...' if len(tables) > 10 else ''}")
        return tables
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """
        Get schema information for a SQL Server table.
        
        READ-ONLY operation: Only queries INFORMATION_SCHEMA (metadata).
        
        Args:
            table_name: Table name in format 'schema.table'
            
        Returns:
            List of column definitions
        """
        schema_name, table = table_name.split('.', 1)
        cursor = self.sql_conn.cursor()
        
        logger.debug(f"Reading schema for table {table_name}...")
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, schema_name, table)
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row[0],
                'data_type': row[1],
                'max_length': row[2],
                'precision': row[3],
                'scale': row[4],
                'is_nullable': row[5] == 'YES',
                'default': row[6],
                'ordinal_position': row[7]
            })
        
        cursor.close()
        logger.debug(f"  Found {len(columns)} column(s) in {table_name}")
        return columns
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        READ-ONLY operation: Only queries INFORMATION_SCHEMA (metadata).
        """
        schema_name, table = table_name.split('.', 1)
        cursor = self.sql_conn.cursor()
        
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = ? 
            AND TABLE_NAME = ?
            AND CONSTRAINT_NAME IN (
                SELECT CONSTRAINT_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
            )
            ORDER BY ORDINAL_POSITION
        """, schema_name, table, schema_name, table)
        
        pk_columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        if pk_columns:
            logger.debug(f"  Primary key columns: {', '.join(pk_columns)}")
        else:
            logger.debug(f"  No primary key found (table will use INSERT-only mode)")
        return pk_columns
    
    def create_pg_table(self, table_name: str, columns: List[Dict], primary_keys: List[str]):
        """
        Create table in PostgreSQL with the given schema.
        
        Args:
            table_name: Table name in format 'schema.table'
            columns: List of column definitions
            primary_keys: List of primary key column names
        """
        schema_name, table = table_name.split('.', 1)
        pg_cursor = self.pg_conn.cursor()
        
        try:
            # Create schema if it doesn't exist
            pg_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            # Build column definitions
            col_defs = []
            for col in columns:
                pg_type = self.type_mapper.map_type(
                    col['data_type'],
                    col['max_length'],
                    col['precision'],
                    col['scale']
                )
                nullable = "NULL" if col['is_nullable'] else "NOT NULL"
                col_def = f'"{col["name"]}" {pg_type} {nullable}'
                col_defs.append(col_def)
            
            # Build CREATE TABLE statement
            create_sql = f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table}" (\n'
            create_sql += ',\n'.join(col_defs)
            
            # Add primary key constraint if exists
            if primary_keys:
                pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
                create_sql += f',\nCONSTRAINT "{table}_pkey" PRIMARY KEY ({pk_cols})'
            
            create_sql += '\n)'
            
            pg_cursor.execute(create_sql)
            self.pg_conn.commit()
            logger.info(f"✓ Created/verified table {table_name} in PostgreSQL")
            logger.info(f"  Columns: {len(columns)}, Primary keys: {len(primary_keys)}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
        finally:
            pg_cursor.close()
    
    def sync_table_data(self, table_name: str, primary_keys: List[str], batch_size: int = 1000, 
                       incremental: bool = False, timestamp_column: Optional[str] = None):
        """
        Sync data from SQL Server to PostgreSQL table.
        
        IMPORTANT: This method only READS from SQL Server (source). All writes are to PostgreSQL only.
        The source database is never modified.
        
        Args:
            table_name: Table name in format 'schema.table'
            primary_keys: List of primary key column names
            batch_size: Number of rows to process per batch
            incremental: If True, only sync changed rows (requires timestamp_column)
            timestamp_column: Column name to use for incremental sync (e.g., 'updated_at', 'modified_date')
        """
        schema_name, table = table_name.split('.', 1)
        sql_cursor = self.sql_conn.cursor()
        pg_cursor = self.pg_conn.cursor()
        
        try:
            # Get all columns
            columns = self.get_table_schema(table_name)
            col_names = [col['name'] for col in columns]
            
            # Use SQL Server syntax for column names (square brackets) - for SELECT queries
            col_names_quoted_sql = [self._quote_sql_server_identifier(col) for col in col_names]
            
            # Use PostgreSQL syntax for column names (double quotes) - for INSERT queries
            col_names_quoted_pg = [f'"{col}"' for col in col_names]
            
            # Build SELECT query using SQL Server syntax (square brackets)
            # IMPORTANT: Only SELECT queries are executed on SQL Server - READ-ONLY
            schema_quoted_sql = self._quote_sql_server_identifier(schema_name)
            table_quoted_sql = self._quote_sql_server_identifier(table)
            
            logger.info(f"Reading data from SQL Server table {table_name}...")
            
            if incremental and timestamp_column:
                # Get max timestamp from PostgreSQL (destination database)
                try:
                    logger.info(f"  Checking for existing data in PostgreSQL (incremental mode)...")
                    pg_cursor.execute(f'''
                        SELECT MAX("{timestamp_column}") 
                        FROM "{schema_name}"."{table}"
                    ''')
                    result = pg_cursor.fetchone()
                    max_timestamp = result[0] if result[0] else None
                    
                    if max_timestamp:
                        # Sync only rows modified since last sync
                        # READ-ONLY query on SQL Server
                        timestamp_col_quoted = self._quote_sql_server_identifier(timestamp_column)
                        select_sql = f'''
                            SELECT {", ".join(col_names_quoted_sql)} 
                            FROM {schema_quoted_sql}.{table_quoted_sql}
                            WHERE {timestamp_col_quoted} > ?
                            ORDER BY {timestamp_col_quoted}
                        '''
                        # Validate query is read-only before execution
                        self._validate_read_only_query(select_sql)
                        sql_cursor.execute(select_sql, max_timestamp)
                        logger.info(f"  Incremental sync: fetching rows modified after {max_timestamp}")
                    else:
                        # First sync - get all rows (READ-ONLY)
                        select_sql = f'SELECT {", ".join(col_names_quoted_sql)} FROM {schema_quoted_sql}.{table_quoted_sql}'
                        self._validate_read_only_query(select_sql)
                        sql_cursor.execute(select_sql)
                        logger.info(f"  Incremental sync: first run, fetching all rows")
                except Exception as e:
                    # Fallback to full sync if incremental fails
                    logger.warning(f"  Incremental sync failed for {table_name}, falling back to full sync: {e}")
                    select_sql = f'SELECT {", ".join(col_names_quoted_sql)} FROM {schema_quoted_sql}.{table_quoted_sql}'
                    self._validate_read_only_query(select_sql)
                    sql_cursor.execute(select_sql)
            else:
                # Full sync - get all rows (READ-ONLY query on SQL Server)
                select_sql = f'SELECT {", ".join(col_names_quoted_sql)} FROM {schema_quoted_sql}.{table_quoted_sql}'
                self._validate_read_only_query(select_sql)
                sql_cursor.execute(select_sql)
                logger.info(f"  Full sync: fetching all rows from source table")
            
            # Build INSERT/UPDATE query (UPSERT)
            if primary_keys:
                # Use ON CONFLICT for UPSERT
                pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
                update_cols = ', '.join([
                    f'"{col}" = EXCLUDED."{col}"'
                    for col in col_names if col not in primary_keys
                ])
                
                insert_sql = f'''
                    INSERT INTO "{schema_name}"."{table}" ({", ".join(col_names_quoted_pg)})
                    VALUES %s
                    ON CONFLICT ({pk_cols}) DO UPDATE SET {update_cols}
                '''
            else:
                # No primary key, just INSERT (might cause duplicates)
                insert_sql = f'''
                    INSERT INTO "{schema_name}"."{table}" ({", ".join(col_names_quoted_pg)})
                    VALUES %s
                '''
            
            # Fetch and insert data in batches
            total_rows = 0
            batch_count = 0
            start_time = datetime.now()
            
            logger.info(f"  Writing data to PostgreSQL table {table_name}...")
            logger.info(f"  Batch size: {batch_size} rows")
            
            while True:
                rows = sql_cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                batch_count += 1
                # Convert rows to list of tuples
                row_data = [tuple(row) for row in rows]
                
                # Execute batch insert
                execute_values(pg_cursor, insert_sql, row_data)
                total_rows += len(rows)
                
                # Log progress every batch
                if batch_count % 10 == 0 or len(rows) < batch_size:  # Log every 10 batches or on last batch
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    logger.info(f"  Progress: {total_rows} rows synced ({batch_count} batches, {rate:.0f} rows/sec)")
            
            # Commit transaction
            self.pg_conn.commit()
            elapsed_total = (datetime.now() - start_time).total_seconds()
            rate_final = total_rows / elapsed_total if elapsed_total > 0 else 0
            logger.info(f"✓ Completed sync for {table_name}: {total_rows} total rows in {elapsed_total:.2f}s ({rate_final:.0f} rows/sec)")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to sync data for {table_name}: {e}")
            raise
        finally:
            sql_cursor.close()
            pg_cursor.close()
    
    def sync_all(self, incremental: bool = False, timestamp_column: Optional[str] = None):
        """
        Sync all tables (schema and data) from SQL Server to PostgreSQL.
        
        Args:
            incremental: If True, only sync changed rows (requires timestamp_column in tables)
            timestamp_column: Column name to use for incremental sync (e.g., 'updated_at')
        """
        sync_start_time = datetime.now()
        successful_tables = []
        failed_tables = []
        
        try:
            logger.info("")
            logger.info("=" * 60)
            logger.info("STARTING DATABASE SYNCHRONIZATION")
            logger.info("=" * 60)
            logger.info("")
            
            if incremental:
                logger.info(f"Mode: INCREMENTAL SYNC (using column: {timestamp_column})")
            else:
                logger.info(f"Mode: FULL SYNC (all data)")
            logger.info("")
            
            # Get all tables
            tables = self.get_sql_server_tables()
            
            if not tables:
                logger.warning("No tables found in SQL Server")
                return
            
            logger.info(f"Processing {len(tables)} table(s)...")
            logger.info("")
            
            # Sync each table
            for idx, table_name in enumerate(tables, 1):
                try:
                    logger.info(f"[{idx}/{len(tables)}] Processing table: {table_name}")
                    logger.info("-" * 60)
                    
                    # Get schema and primary keys
                    columns = self.get_table_schema(table_name)
                    primary_keys = self.get_primary_keys(table_name)
                    
                    # Create/update table schema
                    self.create_pg_table(table_name, columns, primary_keys)
                    
                    # Sync data
                    self.sync_table_data(table_name, primary_keys, incremental=incremental, 
                                       timestamp_column=timestamp_column)
                    
                    successful_tables.append(table_name)
                    logger.info(f"✓ Successfully synced {table_name}")
                    logger.info("")
                    
                except Exception as e:
                    failed_tables.append((table_name, str(e)))
                    logger.error(f"✗ Error syncing table {table_name}: {e}")
                    logger.error("  Continuing with next table...")
                    logger.info("")
                    # Continue with next table
                    continue
            
            # Summary
            sync_duration = (datetime.now() - sync_start_time).total_seconds()
            logger.info("")
            logger.info("=" * 60)
            logger.info("SYNCHRONIZATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total tables processed: {len(tables)}")
            logger.info(f"  ✓ Successful: {len(successful_tables)}")
            logger.info(f"  ✗ Failed: {len(failed_tables)}")
            logger.info(f"Total duration: {sync_duration:.2f} seconds")
            
            if successful_tables:
                logger.info("")
                logger.info("Successfully synced tables:")
                for table in successful_tables:
                    logger.info(f"  ✓ {table}")
            
            if failed_tables:
                logger.info("")
                logger.warning("Failed tables:")
                for table, error in failed_tables:
                    logger.warning(f"  ✗ {table}: {error}")
            
            logger.info("")
            logger.info("=" * 60)
            logger.info("DATABASE SYNCHRONIZATION COMPLETED")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error("")
            logger.error("=" * 60)
            logger.error("SYNCHRONIZATION FAILED")
            logger.error("=" * 60)
            logger.error(f"Error: {e}")
            raise


def load_config():
    """Load database configuration from environment variables."""
    # Check for Windows Authentication
    use_windows_auth = os.getenv('SQL_SERVER_USE_WINDOWS_AUTH', 'false').lower() == 'true'
    
    sql_server_config = {
        'driver': os.getenv('SQL_SERVER_DRIVER', 'ODBC Driver 18 for SQL Server'),
        'server': os.getenv('SQL_SERVER_HOST', 'localhost'),
        'database': os.getenv('SQL_SERVER_DB', ''),
        'use_windows_auth': use_windows_auth,
        'user': os.getenv('SQL_SERVER_USER', ''),
        'password': os.getenv('SQL_SERVER_PASSWORD', '')
    }
    
    postgres_config = {
        'host': os.getenv('PG_HOST', ''),
        'port': int(os.getenv('PG_PORT', '5432')),
        'database': os.getenv('PG_DB', ''),
        'user': os.getenv('PG_USER', ''),
        'password': os.getenv('PG_PASSWORD', '')
    }
    
    # Validate required config
    required_sql = ['server', 'database']
    if not use_windows_auth:
        required_sql.extend(['user', 'password'])
    
    required_pg = ['host', 'database', 'user', 'password']
    
    missing = []
    for key in required_sql:
        if not sql_server_config.get(key):
            missing.append(f'SQL_SERVER_{key.upper()}')
    for key in required_pg:
        if not postgres_config.get(key):
            missing.append(f'PG_{key.upper()}')
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    return sql_server_config, postgres_config


class LockFile:
    """Simple file-based lock to prevent overlapping sync runs."""
    
    def __init__(self, lock_file_path: str):
        self.lock_file_path = lock_file_path
        self.lock_file = None
    
    def acquire(self) -> bool:
        """Try to acquire lock. Returns True if successful, False if already locked."""
        try:
            # Check if lock file exists and is locked
            if os.path.exists(self.lock_file_path):
                # Try to read PID from existing lock file
                try:
                    with open(self.lock_file_path, 'r') as f:
                        pid = f.read().strip()
                        # Check if process is still running
                        if platform.system() == 'Windows':
                            # On Windows, try to check if process exists
                            # Simple approach: if file exists, assume locked
                            return False
                        else:
                            # On Unix, check if process exists
                            try:
                                os.kill(int(pid), 0)  # Signal 0 just checks if process exists
                                return False  # Process exists, still locked
                            except (OSError, ValueError):
                                # Process doesn't exist, remove stale lock
                                os.remove(self.lock_file_path)
                except Exception:
                    pass
            
            # Create lock file
            self.lock_file = open(self.lock_file_path, 'w')
            
            # Try platform-specific locking
            if platform.system() == 'Windows':
                try:
                    # Windows file locking
                    msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                except (NameError, AttributeError):
                    # Fallback: just create the file (less safe but works)
                    pass
            else:
                try:
                    # Unix file locking
                    fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except (NameError, AttributeError):
                    # Fallback: just create the file
                    pass
            
            # Write PID to lock file
            self.lock_file.write(str(os.getpid()))
            self.lock_file.flush()
            return True
        except (IOError, OSError):
            if self.lock_file:
                self.lock_file.close()
            return False
    
    def release(self):
        """Release the lock."""
        if self.lock_file:
            try:
                if platform.system() == 'Windows':
                    try:
                        msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                    except (NameError, AttributeError):
                        pass
                else:
                    try:
                        fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                    except (NameError, AttributeError):
                        pass
                self.lock_file.close()
                if os.path.exists(self.lock_file_path):
                    os.remove(self.lock_file_path)
            except Exception:
                pass


def main():
    """Main execution function."""
    script_start_time = datetime.now()
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("DATABASE SYNC SCRIPT STARTED")
    logger.info("=" * 60)
    logger.info(f"Start time: {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    lock_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.sync.lock')
    lock = LockFile(lock_file_path)
    
    # Try to acquire lock
    if not lock.acquire():
        logger.warning("Another sync is already running. Exiting.")
        sys.exit(0)
    
    try:
        logger.info("Loading configuration from environment variables...")
        # Load configuration
        sql_server_config, postgres_config = load_config()
        logger.info("✓ Configuration loaded successfully")
        logger.info("")
        
        # Check for incremental sync settings
        incremental = os.getenv('SYNC_INCREMENTAL', 'false').lower() == 'true'
        timestamp_column = os.getenv('SYNC_TIMESTAMP_COLUMN', 'updated_at')
        
        if incremental:
            logger.info(f"Incremental sync enabled (timestamp column: {timestamp_column})")
        else:
            logger.info("Full sync mode enabled")
        logger.info("")
        
        # Create sync instance
        logger.info("Initializing database sync instance...")
        sync = DatabaseSync(sql_server_config, postgres_config)
        logger.info("✓ Sync instance created")
        logger.info("")
        
        # Connect to databases
        sync.connect()
        
        # Perform synchronization
        sync.sync_all(incremental=incremental, timestamp_column=timestamp_column)
        
        # Disconnect
        sync.disconnect()
        
        script_duration = (datetime.now() - script_start_time).total_seconds()
        logger.info("")
        logger.info("=" * 60)
        logger.info("SCRIPT EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Start time: {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total duration: {script_duration:.2f} seconds ({script_duration/60:.2f} minutes)")
        logger.info("")
        logger.info("✓ Sync completed successfully")
        logger.info("=" * 60)
        
    except Exception as e:
        script_duration = (datetime.now() - script_start_time).total_seconds()
        logger.error("")
        logger.error("=" * 60)
        logger.error("SCRIPT EXECUTION FAILED")
        logger.error("=" * 60)
        logger.error(f"Fatal error: {e}")
        logger.error(f"Duration before failure: {script_duration:.2f} seconds")
        logger.error("=" * 60)
        sys.exit(1)
    finally:
        # Always release lock
        lock.release()


if __name__ == "__main__":
    main()
