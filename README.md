# DbMigrator

![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?style=for-the-badge&logo=dotnet)
![SQL Server](https://img.shields.io/badge/SQL_Server-CC292B?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)

**DbMigrator** is a high-performance, robust command-line tool designed to seamlessly migrate both schema and data from **Microsoft SQL Server** to **PostgreSQL**. Built with .NET 10, it handles complex relational structures, massive datasets, and provides checkpoint/resume capabilities to ensure your migration completes successfully.

## Features

*   **High-Performance Data Migration**: Utilizes PostgreSQL bulk copy features for incredibly fast data transfer.
*   **Comprehensive Schema Migration**: Intelligently migrates tables, indexes, foreign keys, constraints (unique, check), sequences, views, and triggers.
*   **Automatic Dependency Resolution**: Automatically calculates topological sort order to handle foreign key dependencies during table creation and data insertion without conflicts.
*   **Resume Capability**: Built-in checkpointing system allows you to pause and resume migrations (e.g., if a network error occurs) without starting from scratch.
*   **Interactive & CLI Modes**: Run in pure headless mode via arguments or use the interactive step-by-step wizard.
*   **Real-time Progress UI**: Beautiful console UI showing exact progress, rows per second, and elapsed time per table. 

## Prerequisites

*   [.NET 10.0 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) or later.
*   Access to a source SQL Server database.
*   Access to a target PostgreSQL database.

## Configuration

Create a `config.json` file in your working directory. You can specify different configurations for testing or production.

```json
{
  "source": {
    "type": "MSSql",
    "connectionString": "Server=localhost;Database=SourceDB;User Id=sa;Password=YourPassword;TrustServerCertificate=True;"
  },
  "target": {
    "type": "Postgres",
    "connectionString": "Host=localhost;Port=5432;Database=TargetDB;Username=postgres;Password=postgres"
  },
  "options": {
    "dropTargetTables": false,
    "createSchema": true,
    "migrateViews": true,
    "migrateStoredProcedures": true,
    "migrateIndexes": true,
    "migrateForeignKeys": true,
    "migrateCheckConstraints": true,
    "migrateSequences": true,
    "batchSize": 10000
  },
  "tables": {
    "include": [],
    "exclude": ["sys_*", "AspNet*"]
  }
}
```

## Usage

Run the tool without arguments to start the **Interactive Mode**:

```bash
dotnet run --project DbMigrator.CLI
```

### CLI Commands

For automated or headless environments, use the CLI commands:

```bash
# Test connections to both databases
dotnet run --project DbMigrator.CLI -- test --config config.json

# Migrate only the schema (tables, keys, indexes)
dotnet run --project DbMigrator.CLI -- schema --config config.json

# Migrate only the data
dotnet run --project DbMigrator.CLI -- data --config config.json

# Migrate everything (schema + data)
dotnet run --project DbMigrator.CLI -- full --config config.json
```

### Advanced Options

*   `--tables Table1,Table2`: Migrate only specific tables.
*   `--resume`: Resume a previously failed or interrupted migration using the local checkpoint file.
*   `--clean`: Deletes existing checkpoint files to force a fresh migration.

**Example: Resuming a broken data migration**
```bash
dotnet run --project DbMigrator.CLI -- data --resume
```

## Project Structure

The solution consists of the following clearly separated layers:

*   **`DbMigrator.Core`**: Defines models, interfaces (`ISourceDatabase`, `ITargetDatabase`), and abstractions.
*   **`DbMigrator.MSSql`**: SQL Server specific implementation for reading schema and data.
*   **`DbMigrator.Postgres`**: PostgreSQL specific implementation for writing schema (DDL generation) and bulk data insertion.
*   **`DbMigrator.CLI`**: Command-line application, progress tracking, and logging logic.
*   **`DbMigrator.Tests`**: Unit tests for core logic such as topological sorting and type mapping.

## Known Limitations & Manual Steps

*   **Stored Procedures & Complex Triggers**: While the tool migrates basic triggers, complex T-SQL logic inside stored procedures and advanced triggers cannot be automatically translated to PL/pgSQL. These will be logged and created as empty placeholders/comments in PostgreSQL requiring manual review.
*   **User-Defined Functions (UDFs)**: Currently flagged for manual review and translation.

## Logging

Extensive logs are saved automatically.
*   `migration.log`: Contains detailed step-by-step logs, errors, and warnings.
*   A summary is generated at the end of the migration detailing success/failure counts and overall performance.

---
*Built with love for seamless database migrations.*
