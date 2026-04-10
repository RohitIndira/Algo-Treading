@echo off
rem Creates a fresh PostgreSQL database and applies all SQL migrations
rem Usage: set PGHOST=host && set PGPORT=5432 && set PGUSER=user && set PGPASSWORD=pass && scripts\create_db_from_scratch.bat

setlocal enableextensions

set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%SCRIPT_DIR%.."

if not defined PGHOST set "PGHOST=localhost"
if not defined PGPORT set "PGPORT=5432"
if not defined PGUSER set "PGUSER=postgres"
if not defined DBNAME set "DBNAME=trading_db"

if not defined PGPASSWORD (
  set /p PGPASSWORD=Enter Postgres password for %PGUSER%:
)

where psql >nul 2>&1
if errorlevel 1 (
  echo psql not found in PATH. Please install PostgreSQL client tools.
  exit /b 1
)

set "PGPASSWORD=%PGPASSWORD%"

echo Creating fresh database "%DBNAME%" on %PGHOST%:%PGPORT% as %PGUSER%
psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \"%DBNAME%\";"
if errorlevel 1 (
  echo Failed to drop database. Aborting.
  exit /b 1
)
psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"%DBNAME%\";"
if errorlevel 1 (
  echo Failed to create database. Aborting.
  exit /b 1
)

echo Applying base schema: deployments\docker\init_all_schemas.sql
psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -d %DBNAME% -v ON_ERROR_STOP=1 -f "%REPO_ROOT%\deployments\docker\init_all_schemas.sql"
if errorlevel 1 (
  echo Failed applying base schema. Aborting.
  exit /b 1
)

echo Applying service migrations...

rem Apply migrations from services/*/migrations
for /f "delims=" %%F in ('dir /b /s "%REPO_ROOT%\services\*\migrations\*.sql" 2^>nul ^| sort') do (
  echo -^> Applying: %%F
  psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -d %DBNAME% -f "%%F"
)

echo All migrations applied successfully. Database "%DBNAME%" is ready.
endlocal
