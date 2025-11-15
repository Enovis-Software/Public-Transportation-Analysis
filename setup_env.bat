@echo off
REM Setup Environment Variables for Database Sync
REM Run this script to set environment variables for the current session
REM Or add these to System Environment Variables for permanent setup

echo Setting up environment variables for Database Sync...
echo.

REM SQL Server Configuration
set SQL_SERVER_DRIVER=ODBC Driver 18 for SQL Server
set SQL_SERVER_HOST=localhost
set SQL_SERVER_DB=YourDatabaseName
set SQL_SERVER_USER=your_username
set SQL_SERVER_PASSWORD=your_password

REM For Windows Authentication, uncomment the line below and comment out USER/PASSWORD lines above
REM set SQL_SERVER_USE_WINDOWS_AUTH=true

REM PostgreSQL Configuration
set PG_HOST=your-postgres-host.com
set PG_PORT=5432
set PG_DB=your_database_name
set PG_USER=your_username
set PG_PASSWORD=your_password

echo Environment variables set!
echo.
echo IMPORTANT: Edit this file and replace the placeholder values with your actual credentials.
echo.
echo To use Windows Authentication for SQL Server:
echo   1. Set SQL_SERVER_USE_WINDOWS_AUTH=true
echo   2. Remove or comment out SQL_SERVER_USER and SQL_SERVER_PASSWORD
echo.
pause

