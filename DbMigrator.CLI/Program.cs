using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using DbMigrator.Core.Models;
using DbMigrator.MSSql;
using DbMigrator.Postgres;

namespace DbMigrator.CLI;

class Program
{
    private static readonly ConcurrentDictionary<string, Regex> WildcardCache = new();
    private const int MaxBatchSize = 50_000;
    private static MigrationLogger? _logger;
    private static MigrationCheckpoint? _checkpoint;

    static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            return await InteractiveMigration.RunAsync();
        }

        var command = args[0].ToLower();
        var configPath = GetOption(args, "--config") ?? "config.json";
        var tablesFilter = GetOption(args, "--tables");
        var resumeFlag = HasOption(args, "--resume");
        var cleanFlag = HasOption(args, "--clean");

        if (cleanFlag)
        {
            MigrationCheckpoint.Delete();
            Console.WriteLine("[INFO] Checkpoint cleaned.");
            if (args.Length == 2) return 0;
        }

        try
        {
            int result;
            switch (command)
            {
                case "schema":
                    result = await MigrateSchemaAsync(configPath, tablesFilter, resumeFlag);
                    break;
                case "data":
                    result = await MigrateDataAsync(configPath, tablesFilter, resumeFlag);
                    break;
                case "full":
                    result = await MigrateFullAsync(configPath, tablesFilter, resumeFlag);
                    break;
                case "test":
                    result = await TestConnectionsAsync(configPath);
                    break;
                default:
                    PrintUsage();
                    result = 1;
                    break;
            }
            return result;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n[ERROR] {ex.Message}");
            Console.WriteLine(ex.StackTrace);
            return 1;
        }
        finally
        {
            if (_logger != null) await _logger.DisposeAsync();
        }
    }

    static void PrintUsage()
    {
        Console.WriteLine("""
        SQL Server to PostgreSQL Database Migration Tool

        Usage:
          db-migrate <command> [options]

        Commands:
          schema   Migrate database schema (tables, indexes, foreign keys, views)
          data     Migrate data from SQL Server to PostgreSQL
          full     Migrate both schema and data
          test     Test database connections

        Options:
          --config <path>   Path to configuration file (default: config.json)
          --tables <list>   Comma-separated list of tables to migrate
          --resume          Resume from last checkpoint
          --clean           Clear checkpoint file

        Examples:
          db-migrate schema --config myconfig.json
          db-migrate test
          db-migrate full --tables Users,Orders,Products
          db-migrate data --resume
        """);
    }

    static string? GetOption(string[] args, string option)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i].Equals(option, StringComparison.OrdinalIgnoreCase))
            {
                return args[i + 1];
            }
        }
        return null;
    }

    static bool HasOption(string[] args, string option)
    {
        return args.Any(a => a.Equals(option, StringComparison.OrdinalIgnoreCase));
    }

    static async Task<ConfigModel> LoadConfigAsync(string configPath)
    {
        if (!File.Exists(configPath))
        {
            throw new FileNotFoundException($"Configuration file not found: {configPath}");
        }

        string json;
        try
        {
            json = await File.ReadAllTextAsync(configPath);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException($"Could not read configuration file '{configPath}': {ex.Message}", ex);
        }

        try
        {
            return JsonConvert.DeserializeObject<ConfigModel>(json)
                ?? throw new InvalidOperationException("Configuration file is empty or invalid.");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Invalid JSON in configuration file '{configPath}': {ex.Message}", ex);
        }
    }

    static void ValidateConfig(ConfigModel config)
    {
        if (string.IsNullOrWhiteSpace(config.Source.ConnectionString))
            throw new InvalidOperationException("Source connection string is not configured.");

        if (string.IsNullOrWhiteSpace(config.Target.ConnectionString))
            throw new InvalidOperationException("Target connection string is not configured.");
    }

    static async Task<int> MigrateSchemaAsync(string configPath, string? tablesFilter, bool resume)
    {
        var config = await LoadConfigAsync(configPath);
        ValidateConfig(config);
        var tablesList = tablesFilter?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();

        _logger = new MigrationLogger(config.Options.LogFilePath);
        await _logger.InfoAsync("Starting Schema Migration");
        await _logger.InfoAsync($"Source: {config.Source.Type}");
        await _logger.InfoAsync($"Target: {config.Target.Type}");

        Console.WriteLine("Starting Schema Migration...");
        Console.WriteLine($"   Source: {config.Source.Type}");
        Console.WriteLine($"   Target: {config.Target.Type}");

        await using var sourceDb = new MsSqlConnection(config.Source.ConnectionString);
        await using var targetDb = new PostgresConnection(config.Target.ConnectionString);

        targetDb.SetSchemaMapping(config.Options.SchemaMapping);

        await sourceDb.ConnectAsync();
        Console.WriteLine("[OK] Connected to SQL Server");
        await _logger.SuccessAsync("Connected to SQL Server");

        await targetDb.ConnectAsync();
        Console.WriteLine("[OK] Connected to PostgreSQL");
        await _logger.SuccessAsync("Connected to PostgreSQL");

        var collation = await sourceDb.GetCollationAsync();
        if (!collation.Equals("SQL_Latin1_General_CP1_CI_AS", StringComparison.OrdinalIgnoreCase)
            && !collation.Contains("CI", StringComparison.OrdinalIgnoreCase))
        {
            var warn = $"Source database collation is '{collation}' - case sensitivity differences may occur";
            Console.WriteLine($"   [WARNING] {warn}");
            await _logger.WarnAsync(warn);
        }

        _checkpoint = resume ? await MigrationCheckpoint.LoadAsync() : new MigrationCheckpoint();

        var allTables = await sourceDb.GetTablesAsync();
        var tablesToMigrate = FilterTables(allTables, tablesList, config.Tables);

        Console.WriteLine($"\nFound {allTables.Count} tables, migrating {tablesToMigrate.Count} tables");
        await _logger.InfoAsync($"Found {allTables.Count} tables, migrating {tablesToMigrate.Count} tables");

        if (config.Options.DropTargetTables)
        {
            Console.WriteLine("\nDropping existing tables...");
            foreach (var table in tablesToMigrate)
            {
                var exists = await targetDb.TableExistsAsync(table.Schema, table.Name);
                if (exists)
                {
                    await targetDb.DropTableAsync(table.Schema, table.Name);
                    Console.WriteLine($"   Dropped {table.FullName}");
                    await _logger.InfoAsync($"Dropped {table.FullName}");
                }
            }
        }

        var schemaErrors = 0;
        using (var ui = new ProgressUI())
        {
            // ── Tables ──────────────────────────────────────────────────
            Console.WriteLine($"\nCreating {tablesToMigrate.Count} tables...");
            ui.Start(tablesToMigrate.Count);
            foreach (var table in tablesToMigrate)
            {
                var fullTableName = table.FullName;
                if (_checkpoint != null && _checkpoint.IsSchemaCompleted(fullTableName))
                {
                    ui.CompleteTable(true);
                    continue;
                }
                ui.SetCurrentTable(fullTableName, 0);
                try
                {
                    await targetDb.CreateTableAsync(table);
                    await _logger.SuccessAsync($"Created {fullTableName}");
                    _checkpoint?.MarkSchemaCompleted(fullTableName);
                    ui.CompleteTable(true);
                }
                catch (Exception ex)
                {
                    schemaErrors++;
                    var msg = $"Error creating {fullTableName}: {ex.Message}";
                    ui.PrintLine($"   [ERROR] {msg}");
                    await _logger.ErrorAsync(msg);
                    _checkpoint?.MarkFailed(fullTableName);
                    ui.CompleteTable(false);
                }
            }
        }

        if (config.Options.MigrateIndexes)
        {
            var allIndexes = tablesToMigrate.SelectMany(t => t.Indexes).ToList();
            Console.Write($"\nCreating {allIndexes.Count} indexes... ");
            var idxErrors = 0;
            foreach (var index in allIndexes)
            {
                try   { await targetDb.CreateIndexesAsync(new List<Core.Models.IndexModel> { index }); }
                catch (Exception ex)
                {
                    idxErrors++;
                    await _logger.WarnAsync($"Index {index.Name}: {ex.Message}");
                }
            }
            Console.WriteLine(idxErrors > 0 ? $"done ({idxErrors} errors)" : "done");
        }

        if (config.Options.MigrateForeignKeys)
        {
            var allForeignKeys = tablesToMigrate.SelectMany(t => t.ForeignKeys).ToList();
            Console.Write($"\nCreating {allForeignKeys.Count} foreign keys... ");
            var fkErrors = 0;
            foreach (var fk in allForeignKeys)
            {
                try   { await targetDb.CreateForeignKeysAsync(new List<Core.Models.ForeignKeyModel> { fk }); }
                catch (Exception ex)
                {
                    fkErrors++;
                    await _logger.WarnAsync($"FK {fk.Name}: {ex.Message}");
                }
            }
            Console.WriteLine(fkErrors > 0 ? $"done ({fkErrors} errors)" : "done");
        }

        if (config.Options.MigrateCheckConstraints)
        {
            var allChecks = tablesToMigrate.SelectMany(t => t.CheckConstraints).ToList();
            Console.Write($"\nCreating {allChecks.Count} check constraints... ");
            var ckErrors = 0;
            foreach (var check in allChecks)
            {
                try   { await targetDb.CreateCheckConstraintAsync(check); }
                catch (Exception ex)
                {
                    ckErrors++;
                    await _logger.WarnAsync($"Check {check.Name}: {ex.Message}");
                }
            }
            Console.WriteLine(ckErrors > 0 ? $"done ({ckErrors} errors)" : "done");

            var allUniques = tablesToMigrate.SelectMany(t => t.UniqueConstraints).ToList();
            Console.Write($"\nCreating {allUniques.Count} unique constraints... ");
            var uqErrors = 0;
            foreach (var unique in allUniques)
            {
                try   { await targetDb.CreateUniqueConstraintAsync(unique); }
                catch (Exception ex)
                {
                    uqErrors++;
                    await _logger.WarnAsync($"Unique {unique.Name}: {ex.Message}");
                }
            }
            Console.WriteLine(uqErrors > 0 ? $"done ({uqErrors} errors)" : "done");
        }

        if (config.Options.MigrateSequences)
        {
            var sequences = await sourceDb.GetSequencesAsync();
            if (sequences.Count > 0)
            {
                Console.Write($"\nCreating {sequences.Count} sequences... ");
                var seqErrors = 0;
                foreach (var seq in sequences)
                {
                    try   { await targetDb.CreateSequenceAsync(seq); }
                    catch (Exception ex)
                    {
                        seqErrors++;
                        await _logger.WarnAsync($"Sequence {seq.FullName}: {ex.Message}");
                    }
                }
                Console.WriteLine(seqErrors > 0 ? $"done ({seqErrors} errors)" : "done");
            }
        }

        // ── Triggers ─────────────────────────────────────────────────
        var triggers = await sourceDb.GetTriggersAsync();
        if (triggers.Count > 0)
        {
            Console.Write($"\nMigrating {triggers.Count} triggers... ");
            int autoConverted = 0;
            var placeholders = new List<Core.Models.TriggerModel>();
            foreach (var trigger in triggers)
            {
                try
                {
                    var result = await targetDb.CreateTriggerAsync(trigger);
                    if (result == Core.Models.TriggerConversionResult.AutoConverted)
                        autoConverted++;
                    else
                        placeholders.Add(trigger);
                }
                catch (Exception ex)
                {
                    await _logger.WarnAsync($"Trigger {trigger.Name}: {ex.Message}");
                }
            }
            Console.WriteLine($"done  ({autoConverted} converted, {placeholders.Count} placeholders)");

            if (placeholders.Count > 0)
            {
                Console.WriteLine($"\n[INFO] {placeholders.Count} trigger(s) require manual T-SQL → PL/pgSQL conversion:");
                foreach (var t in placeholders)
                {
                    var events = new List<string>();
                    if (t.IsInsert) events.Add("INSERT");
                    if (t.IsUpdate) events.Add("UPDATE");
                    if (t.IsDelete) events.Add("DELETE");
                    Console.WriteLine($"   ✗ {t.FullName}  (AFTER {string.Join("/", events)})");
                }
                Console.WriteLine("   Original T-SQL definitions saved as function comments in PostgreSQL.");
                await _logger.InfoAsync($"{placeholders.Count} trigger(s) created as placeholders: {string.Join(", ", placeholders.Select(t => t.Name))}");
            }
        }

        var udfs = await sourceDb.GetUserDefinedFunctionsAsync();
        if (udfs.Count > 0)
        {
            Console.WriteLine($"[INFO] {udfs.Count} user-defined function(s) require manual review");
            await _logger.InfoAsync($"Found {udfs.Count} UDF(s) requiring manual review: {string.Join(", ", udfs)}");
        }

        if (config.Options.MigrateViews)
        {
            var views = await sourceDb.GetViewsAsync();
            var sortedViews = TopologicalSortViews(views);
            Console.Write($"\nMigrating {sortedViews.Count} views... ");
            var viewErrors = 0;
            foreach (var view in sortedViews)
            {
                try   { await targetDb.CreateViewAsync(view); }
                catch (Exception ex)
                {
                    viewErrors++;
                    await _logger.WarnAsync($"View {view.FullName}: {ex.Message}");
                }
            }
            Console.WriteLine(viewErrors > 0 ? $"done ({viewErrors} errors)" : "done");
        }

        if (config.Options.MigrateStoredProcedures)
        {
            var procedures = await sourceDb.GetStoredProceduresAsync();
            if (procedures.Count > 0)
            {
                Console.Write($"\nProcessing {procedures.Count} stored procedures... ");
                foreach (var proc in procedures)
                {
                    try   { await targetDb.CreateStoredProcedureAsync(proc); }
                    catch (Exception ex) { await _logger.WarnAsync($"SP {proc.FullName}: {ex.Message}"); }
                }
                Console.WriteLine("done (require manual T-SQL → PL/pgSQL conversion)");
            }
        }

        if (_checkpoint != null)
            await _checkpoint.SaveAsync();

        if (schemaErrors > 0)
        {
            Console.WriteLine($"\nSchema migration completed with {schemaErrors} error(s).");
            await _logger.ErrorAsync($"Schema migration completed with {schemaErrors} error(s)");
            return 1;
        }

        Console.WriteLine("\nSchema migration completed successfully!");
        await _logger.SuccessAsync("Schema migration completed successfully");
        return 0;
    }

    static async Task<int> MigrateDataAsync(string configPath, string? tablesFilter, bool resume)
    {
        var config = await LoadConfigAsync(configPath);
        ValidateConfig(config);
        var tablesList = tablesFilter?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();

        _logger = new MigrationLogger(config.Options.LogFilePath);
        await _logger.InfoAsync("Starting Data Migration");

        Console.WriteLine("Starting Data Migration...");

        await using var sourceDb = new MsSqlConnection(config.Source.ConnectionString);
        await using var targetDb = new PostgresConnection(config.Target.ConnectionString);

        targetDb.SetSchemaMapping(config.Options.SchemaMapping);

        await sourceDb.ConnectAsync();
        Console.WriteLine("[OK] Connected to SQL Server");
        await _logger.SuccessAsync("Connected to SQL Server");

        await targetDb.ConnectAsync();
        Console.WriteLine("[OK] Connected to PostgreSQL");
        await _logger.SuccessAsync("Connected to PostgreSQL");

        var allTables = await sourceDb.GetTablesAsync();
        var tablesToMigrate = FilterTables(allTables, tablesList, config.Tables);
        tablesToMigrate = TopologicalSort(tablesToMigrate);

        Console.WriteLine($"\nMigrating data for {tablesToMigrate.Count} tables");
        await _logger.InfoAsync($"Migrating data for {tablesToMigrate.Count} tables");

        _checkpoint = resume ? await MigrationCheckpoint.LoadAsync() : new MigrationCheckpoint();

        var totalRows = 0L;
        var batchSize = config.Options.BatchSize > 0
            ? Math.Min(config.Options.BatchSize, MaxBatchSize)
            : 1000;
        var tablesOk = 0;
        var tablesEmpty = 0;
        var tablesFailed = 0;
        var validate = config.Options.ValidateData;

        var bulkMode = await targetDb.TryEnableBulkLoadModeAsync();
        if (bulkMode)
            Console.WriteLine("[OK] Bulk load mode enabled (FK checks deferred)");

        var migrationStart = DateTime.UtcNow;

        using (var ui = new ProgressUI())
        {
            ui.Start(tablesToMigrate.Count);

            foreach (var table in tablesToMigrate)
            {
                var fullTableName = table.FullName;

                if (_checkpoint != null && _checkpoint.IsDataCompleted(fullTableName))
                {
                    ui.CompleteTable(true);
                    continue;
                }

                if (_checkpoint != null && _checkpoint.FailedTables.Contains(fullTableName))
                {
                    await _logger.WarnAsync($"Skipping {fullTableName} (previously failed)");
                    ui.CompleteTable(false);
                    continue;
                }

                try
                {
                    var rowCount = await sourceDb.GetRowCountAsync(table.Schema, table.Name);

                    if (rowCount == 0)
                    {
                        tablesEmpty++;
                        _checkpoint?.MarkDataCompleted(fullTableName);
                        ui.CompleteTable(true);
                        continue;
                    }

                    ui.SetCurrentTable(fullTableName, rowCount);

                    var identityColumn = table.Columns.FirstOrDefault(c => c.IsIdentity);
                    var hasIdentity    = identityColumn != null;
                    var tableRows      = 0L;
                    var offset         = 0;
                    var tableStart     = DateTime.UtcNow;

                    while (offset < rowCount)
                    {
                        var (columns, rows) = await sourceDb.ReadTableDataBatchAsync(table.Schema, table.Name, offset, batchSize);
                        if (rows.Count == 0) break;

                        var inserted = await targetDb.InsertDataAsync(table.Schema, table.Name, columns, rows, hasIdentity);
                        tableRows += inserted;
                        offset    += rows.Count;

                        var elapsed        = (DateTime.UtcNow - tableStart).TotalSeconds;
                        var tableRowsPerSec = elapsed > 0 ? (long)(tableRows / elapsed) : 0;
                        ui.UpdateTableProgress(tableRows, tableRowsPerSec);
                    }

                    totalRows += tableRows;
                    tablesOk++;
                    await _logger.SuccessAsync($"{fullTableName}: {tableRows:N0} rows");

                    if (identityColumn != null && tableRows > 0)
                    {
                        try { await targetDb.ResetSequenceAsync(table.Schema, table.Name, identityColumn.Name); }
                        catch (Exception ex) { await _logger.WarnAsync(ex.Message); }
                    }

                    await targetDb.AnalyzeTableAsync(table.Schema, table.Name);

                    if (validate)
                    {
                        var targetRows = await targetDb.GetRowCountAsync(table.Schema, table.Name);
                        var sourceRows = await sourceDb.GetRowCountAsync(table.Schema, table.Name);
                        var match      = sourceRows == targetRows;
                        await _logger.WriteTableResultAsync(fullTableName, sourceRows, targetRows, match);
                        if (!match)
                        {
                            ui.PrintLine($"   [WARNING] {fullTableName}: row count mismatch  source={sourceRows:N0}  target={targetRows:N0}");
                            await _logger.WarnAsync($"{fullTableName} row count mismatch: source={sourceRows:N0}, target={targetRows:N0}");
                        }
                    }

                    _checkpoint?.MarkDataCompleted(fullTableName);
                    ui.CompleteTable(true);
                }
                catch (Exception ex)
                {
                    tablesFailed++;
                    var msg = $"{fullTableName}: {ex.Message}";
                    ui.PrintLine($"   [ERROR] {msg}");
                    await _logger.ErrorAsync(msg);
                    _checkpoint?.MarkFailed(fullTableName);
                    ui.CompleteTable(false);
                }
            }
        } // ui.Dispose() → restores cursor

        if (bulkMode)
            await targetDb.DisableBulkLoadModeAsync();

        var totalElapsed = DateTime.UtcNow - migrationStart;
        var rowsPerSec   = totalElapsed.TotalSeconds > 0 ? (long)(totalRows / totalElapsed.TotalSeconds) : 0;

        await _logger.WriteSummaryAsync(tablesOk, tablesFailed, totalRows, 0);

        Console.WriteLine($"\nData migration completed in {totalElapsed:hh\\:mm\\:ss}!");
        Console.WriteLine($"Tables with data: {tablesOk}  |  Empty (skipped): {tablesEmpty}  |  Failed: {tablesFailed}");
        Console.WriteLine($"Total rows: {totalRows:N0} ({rowsPerSec:N0} rows/sec)");

        if (_checkpoint != null)
            await _checkpoint.SaveAsync();

        if (tablesFailed > 0)
        {
            Console.WriteLine($"[WARNING] {tablesFailed} table(s) failed. Run with --resume to retry.");
            return 1;
        }
        return 0;
    }

    static async Task<int> MigrateFullAsync(string configPath, string? tablesFilter, bool resume)
    {
        var schemaResult = await MigrateSchemaAsync(configPath, tablesFilter, resume);
        if (schemaResult != 0) return schemaResult;

        var dataResult = await MigrateDataAsync(configPath, tablesFilter, resume);
        return dataResult;
    }

    static async Task<int> TestConnectionsAsync(string configPath)
    {
        var config = await LoadConfigAsync(configPath);
        ValidateConfig(config);

        Console.WriteLine("Testing database connections...");

        await using var sourceDb = new MsSqlConnection(config.Source.ConnectionString);
        var sourceOk = await sourceDb.TestConnectionAsync();
        Console.WriteLine($"   SQL Server: {(sourceOk ? "[OK]" : "[FAILED]")}");

        await using var targetDb = new PostgresConnection(config.Target.ConnectionString);
        targetDb.SetSchemaMapping(config.Options.SchemaMapping);

        bool targetOk;
        try
        {
            targetOk = await targetDb.TestConnectionAsync();
            Console.WriteLine($"   PostgreSQL: {(targetOk ? "[OK]" : "[FAILED]")}");
        }
        catch (DatabaseNotFoundException)
        {
            // Database doesn't exist - ask user to create it
            Console.WriteLine($"   PostgreSQL: [DATABASE NOT FOUND]");

            var builder = new Npgsql.NpgsqlConnectionStringBuilder(config.Target.ConnectionString);
            var dbName = builder.Database;

            Console.WriteLine($"\n   Database '{dbName}' does not exist in PostgreSQL.");
            Console.WriteLine($"   Would you like to create it? (Y/N): ");

            var response = Console.ReadLine();
            if (response?.Trim().ToUpper() == "Y" && dbName != null)
            {
                try
                {
                    await targetDb.CreateDatabaseAsync(dbName);
                    targetOk = true;
                    Console.WriteLine($"   PostgreSQL: [OK] - Database created and connected");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"   PostgreSQL: [FAILED] - Could not create database: {ex.Message}");
                    targetOk = false;
                }
            }
            else
            {
                Console.WriteLine("   Skipping database creation.");
                targetOk = false;
            }
        }

        return (sourceOk && targetOk) ? 0 : 1;
    }

    static List<Core.Models.TableModel> TopologicalSort(List<Core.Models.TableModel> tables)
    {
        var sorted = new List<Core.Models.TableModel>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var visiting = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tableMap = new Dictionary<string, Core.Models.TableModel>(StringComparer.OrdinalIgnoreCase);

        foreach (var t in tables)
        {
            tableMap.TryAdd(t.FullName, t);
            tableMap.TryAdd(t.Name, t);
        }

        bool Visit(Core.Models.TableModel table)
        {
            if (visited.Contains(table.FullName)) return true;

            if (visiting.Contains(table.FullName))
            {
                Console.WriteLine($"   [WARNING] Circular FK dependency detected involving {table.FullName}, breaking cycle.");
                return false;
            }

            visiting.Add(table.FullName);

            foreach (var fk in table.ForeignKeys)
            {
                var refName = fk.ToTable;
                if (!tableMap.TryGetValue(refName, out var refTable))
                {
                    var shortName = refName.Contains('.') ? refName.Split('.')[1] : refName;
                    tableMap.TryGetValue(shortName, out refTable);
                }

                if (refTable != null && refTable.FullName != table.FullName)
                    Visit(refTable);
            }

            visiting.Remove(table.FullName);
            visited.Add(table.FullName);
            sorted.Add(table);
            return true;
        }

        foreach (var table in tables)
            Visit(table);

        return sorted;
    }

    static List<ViewModel> TopologicalSortViews(List<ViewModel> views)
    {
        var sorted = new List<ViewModel>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var visiting = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var viewMap = views.ToDictionary(v => v.FullName, v => v, StringComparer.OrdinalIgnoreCase);

        bool Visit(ViewModel view)
        {
            if (visited.Contains(view.FullName)) return true;

            if (visiting.Contains(view.FullName))
            {
                Console.WriteLine($"   [WARNING] Circular view dependency detected involving {view.FullName}");
                return false;
            }

            visiting.Add(view.FullName);

            foreach (var otherView in views)
            {
                if (otherView.FullName != view.FullName &&
                    view.Definition.Contains(otherView.Name, StringComparison.OrdinalIgnoreCase))
                {
                    if (viewMap.TryGetValue(otherView.FullName, out var depView))
                        Visit(depView);
                }
            }

            visiting.Remove(view.FullName);
            visited.Add(view.FullName);
            sorted.Add(view);
            return true;
        }

        foreach (var view in views)
            Visit(view);

        return sorted;
    }

    static bool CanAutoConvertTrigger(Core.Models.TriggerModel trigger)
    {
        var def = trigger.Definition.ToUpperInvariant();

        // Check for simple timestamp update pattern
        // Supports: UpdatedAt, CreatedAt, ModifiedDate, ChangedAt, ModifiedAt, LastModified, UpdateTime
        if (def.Contains("UPDATE") && def.Contains("SET") &&
            (def.Contains("UPDATEDAT") || def.Contains("CREATEDAT") || def.Contains("MODIFIEDDATE") ||
             def.Contains("CHANGEDAT") || def.Contains("MODIFIEDAT") || def.Contains("LASTMODIFIED") ||
             def.Contains("UPDATETIME") || def.Contains("MODIFIED")) &&
            (def.Contains("GETDATE()") || def.Contains("SYSDATETIME()") || def.Contains("CURRENT_TIMESTAMP")))
        {
            return true;
        }

        return false;
    }

    static List<Core.Models.TableModel> FilterTables(
        List<Core.Models.TableModel> allTables,
        List<string>? includeList,
        Core.Models.TableFilter filter)
    {
        var result = allTables;

        if (includeList != null && includeList.Count > 0)
        {
            return result.Where(t => includeList.Any(name =>
                t.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                t.FullName.Equals(name, StringComparison.OrdinalIgnoreCase)
            )).ToList();
        }

        if (filter.Include.Count > 0)
        {
            result = result.Where(t => filter.Include.Any(pattern =>
                WildcardMatch(t.Name, pattern) || WildcardMatch(t.FullName, pattern)
            )).ToList();
        }

        if (filter.Exclude.Count > 0)
        {
            result = result.Where(t => !filter.Exclude.Any(pattern =>
                WildcardMatch(t.Name, pattern) || WildcardMatch(t.FullName, pattern)
            )).ToList();
        }

        return result;
    }

    static bool WildcardMatch(string text, string pattern)
    {
        var regex = WildcardCache.GetOrAdd(pattern, p =>
        {
            var regexPattern = "^" + Regex.Escape(p)
                .Replace("\\*", ".*")
                .Replace("\\?", ".") + "$";
            return new Regex(regexPattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);
        });
        return regex.IsMatch(text);
    }
}
