using Spectre.Console;
using Newtonsoft.Json;
using DbMigrator.Core.Models;
using DbMigrator.MSSql;
using DbMigrator.Postgres;

namespace DbMigrator.CLI;

static class InteractiveMigration
{
    private static MigrationLogger? _logger;

    public static async Task<int> RunAsync()
    {
        AnsiConsole.Clear();
        AnsiConsole.Write(new FigletText("DB Migrator").Color(Color.CornflowerBlue));
        AnsiConsole.MarkupLine("[grey]SQL Server → PostgreSQL Database Migration Tool[/]");
        AnsiConsole.Write(new Rule().RuleStyle("grey"));
        AnsiConsole.WriteLine();

        AnsiConsole.Write(new Rule("[bold yellow]1. Source Database (SQL Server)[/]").LeftJustified());
        AnsiConsole.WriteLine();
        var sourceConnStr = BuildSqlServerConnectionString();

        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Rule("[bold yellow]2. Target Database (PostgreSQL)[/]").LeftJustified());
        AnsiConsole.WriteLine();
        var targetConnStr = BuildPostgresConnectionString();

        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Rule("[bold yellow]3. Testing Connections[/]").LeftJustified());
        AnsiConsole.WriteLine();

        MsSqlConnection sourceDb;
        PostgresConnection targetDb;
        string sourceDbName, targetDbName, sourceVersion, targetVersion;
        List<TableModel> allTables;
        List<ViewModel> views;
        List<StoredProcedureModel> storedProcedures;
        List<SequenceModel> sequences;
        List<TriggerModel> triggers;
        List<(string Schema, string Name, string Definition)> udfs;
        Dictionary<string, long> rowCounts;
        List<string> targetExistingTables;
        string collation;

        try
        {
            (sourceDb, targetDb, sourceDbName, targetDbName, sourceVersion, targetVersion,
             allTables, views, storedProcedures, sequences, triggers, udfs, rowCounts, targetExistingTables, collation) =
                await ConnectAndGatherInfoAsync(sourceConnStr, targetConnStr);
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red bold]Connection failed:[/] {Markup.Escape(ex.Message)}");
            return 1;
        }

        try
        {
            ShowDatabaseInfo(sourceDbName, sourceVersion, targetDbName, targetVersion,
                allTables, views, storedProcedures, sequences, triggers, udfs, rowCounts, targetExistingTables, collation);

            if (allTables.Count == 0)
            {
                AnsiConsole.MarkupLine("[red]Source database has no tables. Nothing to migrate.[/]");
                return 0;
            }

            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule("[bold yellow]4. Select Tables[/]").LeftJustified());
            AnsiConsole.WriteLine();

            var selectedTables = SelectTables(allTables, rowCounts);
            if (selectedTables.Count == 0)
            {
                AnsiConsole.MarkupLine("[yellow]No tables selected. Exiting.[/]");
                return 0;
            }

            AnsiConsole.MarkupLine($"[green]{selectedTables.Count}[/] table(s) selected.\n");

            AnsiConsole.Write(new Rule("[bold yellow]5. Migration Settings[/]").LeftJustified());
            AnsiConsole.WriteLine();

            var mode = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("[bold]What would you like to migrate?[/]")
                    .HighlightStyle("cyan")
                    .AddChoices(
                        "Full Migration (Schema + Data)",
                        "Schema Only (tables, indexes, FKs, views)",
                        "Data Only (requires existing schema)"));

            AnsiConsole.WriteLine();

            var options = ConfigureOptions(views.Count > 0, storedProcedures.Count > 0, sequences.Count > 0, targetExistingTables.Count > 0);

            ConfigureSchemaMapping(options);

            if (AnsiConsole.Confirm("[grey]Enable migration logging?[/]", false))
            {
                options.LogFilePath = AnsiConsole.Prompt(
                    new TextPrompt<string>("[green]?[/] Log file path:")
                        .DefaultValue("migration.log")
                        .PromptStyle("white"));
            }

            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule("[bold yellow]6. Migration Summary[/]").LeftJustified());
            AnsiConsole.WriteLine();

            var totalRows = selectedTables.Sum(t => rowCounts.GetValueOrDefault(t.FullName, 0));

            var summaryTable = new Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Color.Grey)
                .AddColumn(new TableColumn("[bold]Setting[/]").Width(25))
                .AddColumn(new TableColumn("[bold]Value[/]"));

            summaryTable.AddRow("[cyan]Source[/]", $"{sourceDbName} (SQL Server)");
            summaryTable.AddRow("[cyan]Target[/]", $"{targetDbName} (PostgreSQL)");
            summaryTable.AddRow("[cyan]Mode[/]", mode.Split('(')[0].Trim());
            summaryTable.AddRow("[cyan]Tables[/]", $"{selectedTables.Count} table(s)");
            summaryTable.AddRow("[cyan]Estimated Rows[/]", $"~{totalRows:N0}");
            summaryTable.AddRow("[cyan]Drop Existing[/]", options.DropTargetTables ? "[red]Yes[/]" : "No");
            summaryTable.AddRow("[cyan]Indexes[/]", options.MigrateIndexes ? "Yes" : "No");
            summaryTable.AddRow("[cyan]Foreign Keys[/]", options.MigrateForeignKeys ? "Yes" : "No");
            summaryTable.AddRow("[cyan]Check Constraints[/]", options.MigrateCheckConstraints ? "Yes" : "No");
            summaryTable.AddRow("[cyan]Sequences[/]", options.MigrateSequences ? "Yes" : "No");
            summaryTable.AddRow("[cyan]Views[/]", options.MigrateViews ? "Yes" : "No");
            summaryTable.AddRow("[cyan]Stored Procs[/]", options.MigrateStoredProcedures ? "Yes" : "No");
            summaryTable.AddRow("[cyan]Validate Data[/]", options.ValidateData ? "Yes" : "No");
            summaryTable.AddRow("[cyan]Batch Size[/]", $"{options.BatchSize:N0}");

            if (options.SchemaMapping.Count > 0)
            {
                var mappings = string.Join(", ", options.SchemaMapping.Select(kv => $"{kv.Key}→{kv.Value}"));
                summaryTable.AddRow("[cyan]Schema Mapping[/]", mappings);
            }

            AnsiConsole.Write(summaryTable);
            AnsiConsole.WriteLine();

            if (AnsiConsole.Confirm("[grey]Show selected tables list?[/]", false))
            {
                var tablesPanel = new Table().Border(TableBorder.Simple).BorderColor(Color.Grey)
                    .AddColumn("Table").AddColumn(new TableColumn("~Rows").RightAligned());
                foreach (var t in selectedTables.OrderBy(t => t.FullName))
                {
                    var rc = rowCounts.GetValueOrDefault(t.FullName, 0);
                    tablesPanel.AddRow(t.FullName, $"{rc:N0}");
                }
                AnsiConsole.Write(tablesPanel);
                AnsiConsole.WriteLine();
            }

            if (!AnsiConsole.Confirm("[bold]Proceed with migration?[/]"))
            {
                AnsiConsole.MarkupLine("[yellow]Migration cancelled.[/]");
                return 0;
            }

            AnsiConsole.WriteLine();
            if (AnsiConsole.Confirm("[grey]Save this configuration for future use?[/]", false))
            {
                await SaveConfigAsync(sourceConnStr, targetConnStr, selectedTables, options);
            }

            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule("[bold green]Migration Started[/]"));
            AnsiConsole.WriteLine();

            var startTime = DateTime.UtcNow;
            var migrationMode = mode.StartsWith("Full") ? "full"
                : mode.StartsWith("Schema") ? "schema" : "data";

            var result = await ExecuteMigrationAsync(
                sourceDb, targetDb, selectedTables, views, storedProcedures, sequences,
                migrationMode, options, rowCounts);

            var elapsed = DateTime.UtcNow - startTime;

            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule(result == 0
                ? "[bold green]Migration Complete[/]"
                : "[bold red]Migration Completed with Errors[/]"));
            AnsiConsole.MarkupLine($"\n[grey]Duration: {elapsed:hh\\:mm\\:ss}[/]\n");

            if (_logger != null)
            {
                AnsiConsole.MarkupLine($"[grey]Log saved to: {Markup.Escape(options.LogFilePath ?? "migration.log")}[/]");
            }

            return result;
        }
        finally
        {
            await sourceDb.DisposeAsync();
            await targetDb.DisposeAsync();
            if (_logger != null) await _logger.DisposeAsync();
        }
    }
    static string BuildSqlServerConnectionString()
    {
        var inputMode = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[bold]How would you like to configure the connection?[/]")
                .HighlightStyle("cyan")
                .AddChoices("Enter details step by step", "Paste a full connection string"));

        if (inputMode.StartsWith("Paste"))
        {
            return AnsiConsole.Prompt(
                new TextPrompt<string>("[green]?[/] Connection string:")
                    .PromptStyle("white"));
        }

        var server = AnsiConsole.Prompt(
            new TextPrompt<string>("[green]?[/] Server/Host:")
                .DefaultValue("localhost")
                .PromptStyle("white"));

        var port = AnsiConsole.Prompt(
            new TextPrompt<int>("[green]?[/] Port:")
                .DefaultValue(1433)
                .PromptStyle("white"));

        var database = AnsiConsole.Prompt(
            new TextPrompt<string>("[green]?[/] Database name:")
                .PromptStyle("white"));

        var authMode = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[green]?[/] [bold]Authentication:[/]")
                .HighlightStyle("cyan")
                .AddChoices("Windows Authentication (Trusted)", "SQL Server Authentication"));

        var connStr = $"Server={server},{port};Database={database};";

        if (authMode.StartsWith("Windows"))
        {
            connStr += "Trusted_Connection=True;";
        }
        else
        {
            var username = AnsiConsole.Prompt(
                new TextPrompt<string>("[green]?[/] Username:")
                    .DefaultValue("sa")
                    .PromptStyle("white"));
            var password = AnsiConsole.Prompt(
                new TextPrompt<string>("[green]?[/] Password:")
                    .PromptStyle("white")
                    .Secret());
            connStr += $"User Id={username};Password={password};";
        }

        var trustCert = AnsiConsole.Confirm("[green]?[/] Trust Server Certificate?", true);
        if (trustCert)
            connStr += "TrustServerCertificate=True;";

        AnsiConsole.MarkupLine($"[grey]Connection string: {Markup.Escape(MaskPassword(connStr))}[/]");
        return connStr;
    }

    static string BuildPostgresConnectionString()
    {
        var inputMode = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[bold]How would you like to configure the connection?[/]")
                .HighlightStyle("cyan")
                .AddChoices("Enter details step by step", "Paste a full connection string"));

        if (inputMode.StartsWith("Paste"))
        {
            return AnsiConsole.Prompt(
                new TextPrompt<string>("[green]?[/] Connection string:")
                    .PromptStyle("white"));
        }

        var host = AnsiConsole.Prompt(
            new TextPrompt<string>("[green]?[/] Host:")
                .DefaultValue("localhost")
                .PromptStyle("white"));

        var port = AnsiConsole.Prompt(
            new TextPrompt<int>("[green]?[/] Port:")
                .DefaultValue(5432)
                .PromptStyle("white"));

        var database = AnsiConsole.Prompt(
            new TextPrompt<string>("[green]?[/] Database name:")
                .PromptStyle("white"));

        var username = AnsiConsole.Prompt(
            new TextPrompt<string>("[green]?[/] Username:")
                .DefaultValue("postgres")
                .PromptStyle("white"));

        var password = AnsiConsole.Prompt(
            new TextPrompt<string>("[green]?[/] Password:")
                .PromptStyle("white")
                .Secret());

        var connStr = $"Host={host};Port={port};Database={database};Username={username};Password={password};";
        AnsiConsole.MarkupLine($"[grey]Connection string: {Markup.Escape(MaskPassword(connStr))}[/]");
        return connStr;
    }

    static string MaskPassword(string connStr)
    {
        return System.Text.RegularExpressions.Regex.Replace(
            connStr, @"(Password|Pwd)\s*=\s*[^;]+", "$1=****",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }
    static async Task<(
        MsSqlConnection sourceDb,
        PostgresConnection targetDb,
        string sourceDbName,
        string targetDbName,
        string sourceVersion,
        string targetVersion,
        List<TableModel> allTables,
        List<ViewModel> views,
        List<StoredProcedureModel> storedProcedures,
        List<SequenceModel> sequences,
        List<TriggerModel> triggers,
        List<(string Schema, string Name, string Definition)> udfs,
        Dictionary<string, long> rowCounts,
        List<string> targetExistingTables,
        string collation
    )> ConnectAndGatherInfoAsync(string sourceConnStr, string targetConnStr)
    {
        var sourceDb = new MsSqlConnection(sourceConnStr);
        var targetDb = new PostgresConnection(targetConnStr);

        string sourceDbName = "", targetDbName = "", sourceVersion = "", targetVersion = "", collation = "";
        List<TableModel> allTables = [];
        List<ViewModel> views = [];
        List<StoredProcedureModel> storedProcedures = [];
        List<SequenceModel> sequences = [];
        List<TriggerModel> triggers = [];
        List<(string Schema, string Name, string Definition)> udfs = [];
        Dictionary<string, long> rowCounts = [];
        List<string> targetExistingTables = [];

        await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .SpinnerStyle(Style.Parse("cyan"))
            .StartAsync("Connecting to SQL Server...", async ctx =>
            {
                await sourceDb.ConnectAsync();
                AnsiConsole.MarkupLine("[green]  ✓[/] Connected to SQL Server");

                ctx.Status("Connecting to PostgreSQL...");
                await targetDb.ConnectAsync();
                AnsiConsole.MarkupLine("[green]  ✓[/] Connected to PostgreSQL");

                ctx.Status("Reading source database info...");
                sourceDbName = await sourceDb.GetDatabaseNameAsync();
                sourceVersion = await sourceDb.GetServerVersionAsync();
                collation = await sourceDb.GetCollationAsync();
                allTables = await sourceDb.GetTablesAsync();
                rowCounts = await sourceDb.GetApproximateRowCountsAsync();
                views = await sourceDb.GetViewsAsync();
                storedProcedures = await sourceDb.GetStoredProceduresAsync();
                sequences = await sourceDb.GetSequencesAsync();
                triggers = await sourceDb.GetTriggersAsync();
                udfs = await sourceDb.GetUserDefinedFunctionsAsync();
                AnsiConsole.MarkupLine("[green]  ✓[/] Source database info loaded");

                ctx.Status("Reading target database info...");
                targetDbName = await targetDb.GetDatabaseNameAsync();
                targetVersion = await targetDb.GetServerVersionAsync();
                targetExistingTables = await targetDb.GetExistingTablesAsync();
                AnsiConsole.MarkupLine("[green]  ✓[/] Target database info loaded");
            });

        return (sourceDb, targetDb, sourceDbName, targetDbName, sourceVersion, targetVersion,
                allTables, views, storedProcedures, sequences, triggers, udfs, rowCounts, targetExistingTables, collation);
    }
    static void ShowDatabaseInfo(
        string sourceDbName, string sourceVersion,
        string targetDbName, string targetVersion,
        List<TableModel> allTables,
        List<ViewModel> views,
        List<StoredProcedureModel> storedProcedures,
        List<SequenceModel> sequences,
        List<TriggerModel> triggers,
        List<(string Schema, string Name, string Definition)> udfs,
        Dictionary<string, long> rowCounts,
        List<string> targetExistingTables,
        string collation)
    {
        var schemas = allTables.Select(t => t.Schema).Distinct().OrderBy(s => s).ToList();
        var totalRows = rowCounts.Values.Sum();

        AnsiConsole.WriteLine();

        var sourceTable = new Table()
            .Title("[bold cornflowerblue]Source Database[/]")
            .Border(TableBorder.Rounded)
            .BorderColor(Color.CornflowerBlue)
            .AddColumn(new TableColumn("[bold]Property[/]").Width(20))
            .AddColumn(new TableColumn("[bold]Value[/]"));

        sourceTable.AddRow("Database", sourceDbName);
        sourceTable.AddRow("Server", Markup.Escape(sourceVersion.Length > 60 ? sourceVersion[..60] + "..." : sourceVersion));
        sourceTable.AddRow("Collation", collation);
        sourceTable.AddRow("Tables", $"[green]{allTables.Count}[/]");
        sourceTable.AddRow("Views", $"{views.Count}");
        sourceTable.AddRow("Stored Procedures", $"{storedProcedures.Count}");
        sourceTable.AddRow("Sequences", $"{sequences.Count}");
        sourceTable.AddRow("Triggers", $"{triggers.Count}");
        sourceTable.AddRow("UDFs", $"{udfs.Count}");
        sourceTable.AddRow("Schemas", string.Join(", ", schemas));
        sourceTable.AddRow("Total Rows (approx)", $"[bold]~{totalRows:N0}[/]");

        if (!collation.Contains("CI", StringComparison.OrdinalIgnoreCase))
        {
            sourceTable.AddRow("", "[yellow]⚠ Case-sensitive collation[/]");
        }

        AnsiConsole.Write(sourceTable);
        AnsiConsole.WriteLine();

        var targetTable = new Table()
            .Title("[bold green]Target Database[/]")
            .Border(TableBorder.Rounded)
            .BorderColor(Color.Green)
            .AddColumn(new TableColumn("[bold]Property[/]").Width(20))
            .AddColumn(new TableColumn("[bold]Value[/]"));

        targetTable.AddRow("Database", targetDbName);
        targetTable.AddRow("Server", Markup.Escape(targetVersion.Length > 60 ? targetVersion[..60] + "..." : targetVersion));
        targetTable.AddRow("Existing Tables", targetExistingTables.Count > 0
            ? $"[yellow]{targetExistingTables.Count}[/]"
            : "[green]0 (empty)[/]");

        AnsiConsole.Write(targetTable);

        if (targetExistingTables.Count > 0)
        {
            AnsiConsole.MarkupLine($"\n[yellow]  ⚠ Target database already has {targetExistingTables.Count} table(s).[/]");
        }

        if (triggers.Count > 0 || udfs.Count > 0)
        {
            AnsiConsole.WriteLine();
            var warnings = new List<string>();
            if (triggers.Count > 0) warnings.Add($"{triggers.Count} trigger(s)");
            if (udfs.Count > 0) warnings.Add($"{udfs.Count} user-defined function(s)");
            AnsiConsole.MarkupLine($"[yellow]  ⚠ Found {string.Join(" and ", warnings)} requiring manual review.[/]");
        }
    }
    static List<TableModel> SelectTables(List<TableModel> allTables, Dictionary<string, long> rowCounts)
    {
        var selectMode = AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("[bold]How would you like to select tables?[/]")
                .HighlightStyle("cyan")
                .AddChoices(
                    "Migrate all tables",
                    "Choose tables manually",
                    "Exclude tables by pattern (e.g. sys_*, AspNet*)"));

        if (selectMode.StartsWith("Migrate all"))
        {
            return allTables;
        }

        if (selectMode.StartsWith("Exclude"))
        {
            var patterns = AnsiConsole.Prompt(
                new TextPrompt<string>("[green]?[/] Enter exclude patterns (comma separated):")
                    .DefaultValue("sys_*, AspNet*")
                    .PromptStyle("white"));

            var excludePatterns = patterns.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var filtered = allTables.Where(t => !excludePatterns.Any(p =>
                WildcardMatch(t.Name, p) || WildcardMatch(t.FullName, p))).ToList();

            AnsiConsole.MarkupLine($"[grey]Excluded {allTables.Count - filtered.Count} table(s), {filtered.Count} remaining.[/]");
            return filtered;
        }

        var prompt = new MultiSelectionPrompt<string>()
            .Title("[bold]Select tables to migrate:[/]")
            .PageSize(20)
            .HighlightStyle("cyan")
            .InstructionsText("[grey](↑↓ navigate, space toggle, enter confirm)[/]");

        var groupedBySchema = allTables.GroupBy(t => t.Schema).OrderBy(g => g.Key);

        var labelToTable = new Dictionary<string, TableModel>(StringComparer.OrdinalIgnoreCase);

        foreach (var group in groupedBySchema)
        {
            var items = group.OrderBy(t => t.Name)
                .Select(t =>
                {
                    var rc = rowCounts.GetValueOrDefault(t.FullName, 0);
                    var label = rc > 0 ? $"{t.FullName} (~{rc:N0} rows)" : t.FullName;
                    labelToTable[label] = t;
                    return Markup.Escape(label);
                }).ToArray();

            prompt.AddChoiceGroup(Markup.Escape($"[{group.Key}]"), items);
        }

        foreach (var t in allTables)
        {
            var rc = rowCounts.GetValueOrDefault(t.FullName, 0);
            var label = rc > 0 ? $"{t.FullName} (~{rc:N0} rows)" : t.FullName;
            prompt.Select(Markup.Escape(label));
        }

        var selected = AnsiConsole.Prompt(prompt);

        return selected
            .Select(s => labelToTable.TryGetValue(s, out var t) ? t : null)
            .Where(t => t != null)
            .ToList()!;
    }

    static bool WildcardMatch(string text, string pattern)
    {
        var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("\\*", ".*")
            .Replace("\\?", ".") + "$";
        return System.Text.RegularExpressions.Regex.IsMatch(text, regexPattern,
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }
    static MigrationOptions ConfigureOptions(bool hasViews, bool hasStoredProcs, bool hasSequences, bool targetHasTables)
    {
        var options = new MigrationOptions();

        if (targetHasTables)
        {
            options.DropTargetTables = AnsiConsole.Confirm("[red]?[/] Drop existing tables in target database?", false);
        }

        var features = new List<string>();
        features.Add("Migrate Indexes");
        features.Add("Migrate Foreign Keys");
        features.Add("Migrate Check & Unique Constraints");
        if (hasSequences) features.Add("Migrate Sequences");
        if (hasViews) features.Add("Migrate Views");
        if (hasStoredProcs) features.Add("Migrate Stored Procedures (placeholder only)");

        var prompt = new MultiSelectionPrompt<string>()
            .Title("[bold]Select additional migration features:[/]")
            .HighlightStyle("cyan")
            .InstructionsText("[grey](space toggle, enter confirm)[/]")
            .AddChoices(features)
            .Select("Migrate Indexes")
            .Select("Migrate Foreign Keys")
            .Select("Migrate Check & Unique Constraints");

        if (hasSequences) prompt.Select("Migrate Sequences");
        if (hasViews) prompt.Select("Migrate Views");

        var selectedFeatures = AnsiConsole.Prompt(prompt);

        options.MigrateIndexes = selectedFeatures.Contains("Migrate Indexes");
        options.MigrateForeignKeys = selectedFeatures.Contains("Migrate Foreign Keys");
        options.MigrateCheckConstraints = selectedFeatures.Contains("Migrate Check & Unique Constraints");
        options.MigrateSequences = selectedFeatures.Contains("Migrate Sequences");
        options.MigrateViews = selectedFeatures.Contains("Migrate Views");
        options.MigrateStoredProcedures = selectedFeatures.Contains("Migrate Stored Procedures (placeholder only)");

        options.BatchSize = AnsiConsole.Prompt(
            new TextPrompt<int>("[green]?[/] Batch size for data migration:")
                .DefaultValue(1000)
                .PromptStyle("white")
                .Validate(v => v switch
                {
                    < 100 => ValidationResult.Error("[red]Minimum batch size is 100[/]"),
                    > 50000 => ValidationResult.Error("[red]Maximum batch size is 50,000[/]"),
                    _ => ValidationResult.Success()
                }));

        options.ValidateData = AnsiConsole.Confirm("[green]?[/] Validate data after migration (row count check)?", true);

        return options;
    }

    static void ConfigureSchemaMapping(MigrationOptions options)
    {
        if (!AnsiConsole.Confirm("[grey]Configure schema mapping (e.g. dbo → public)?[/]", false))
            return;

        AnsiConsole.MarkupLine("[grey]Enter schema mappings (source→target). Empty line when done.[/]");
        while (true)
        {
            var mapping = AnsiConsole.Prompt(
                new TextPrompt<string>("[green]?[/] Mapping (or press Enter to finish):")
                    .AllowEmpty()
                    .PromptStyle("white"));

            if (string.IsNullOrWhiteSpace(mapping)) break;

            var parts = mapping.Split(new[] { '→', '-', '>' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 2)
            {
                var source = parts[0].Trim();
                var target = parts[1].Trim();
                options.SchemaMapping[source] = target;
                AnsiConsole.MarkupLine($"[grey]  Added: {source} → {target}[/]");
            }
            else
            {
                AnsiConsole.MarkupLine("[red]Invalid format. Use: sourceSchema→targetSchema[/]");
            }
        }
    }
    static async Task SaveConfigAsync(string sourceConnStr, string targetConnStr,
        List<TableModel> selectedTables, MigrationOptions options)
    {
        var path = AnsiConsole.Prompt(
            new TextPrompt<string>("[green]?[/] Config file path:")
                .DefaultValue("config.json")
                .PromptStyle("white"));

        var config = new ConfigModel
        {
            Source = new SourceConfig { Type = "MSSql", ConnectionString = sourceConnStr },
            Target = new TargetConfig { Type = "Postgres", ConnectionString = targetConnStr },
            Options = options,
            Tables = new TableFilter
            {
                Include = selectedTables.Select(t => t.FullName).ToList(),
                Exclude = []
            }
        };

        var json = JsonConvert.SerializeObject(config, Formatting.Indented);
        await File.WriteAllTextAsync(path, json);
        AnsiConsole.MarkupLine($"[green]  ✓[/] Config saved to [bold]{Markup.Escape(path)}[/]");
    }
    static async Task<int> ExecuteMigrationAsync(
        MsSqlConnection sourceDb,
        PostgresConnection targetDb,
        List<TableModel> tables,
        List<ViewModel> views,
        List<StoredProcedureModel> storedProcedures,
        List<SequenceModel> sequences,
        string mode,
        MigrationOptions options,
        Dictionary<string, long> rowCounts)
    {
        if (sourceDb == null) throw new ArgumentNullException(nameof(sourceDb));
        if (targetDb == null) throw new ArgumentNullException(nameof(targetDb));
        if (tables == null) throw new ArgumentNullException(nameof(tables));
        options ??= new MigrationOptions();

        _logger = !string.IsNullOrEmpty(options.LogFilePath)
            ? new MigrationLogger(options.LogFilePath)
            : null;

        if (options.SchemaMapping != null)
            targetDb.SetSchemaMapping(options.SchemaMapping);

        var errors = new List<string>();
        var schemaErrors = 0;
        var tablesOk = 0;
        var tablesFailed = 0;
        var totalRows = 0L;

        if (mode is "schema" or "full")
        {
            if (options.DropTargetTables)
            {
                await AnsiConsole.Status()
                    .Spinner(Spinner.Known.Dots)
                    .StartAsync("Dropping existing tables...", async ctx =>
                    {
                        foreach (var table in tables)
                        {
                            var exists = await targetDb.TableExistsAsync(table.Schema, table.Name);
                            if (exists)
                            {
                                await targetDb.DropTableAsync(table.Schema, table.Name);
                                AnsiConsole.MarkupLine($"[red]  ✗[/] Dropped {Markup.Escape(table.FullName)}");
                            }
                        }
                    });
                AnsiConsole.WriteLine();
            }

            AnsiConsole.MarkupLine("[bold]Creating tables...[/]");
            await AnsiConsole.Progress()
                .AutoRefresh(true)
                .HideCompleted(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask("[cyan]Tables[/]", maxValue: tables.Count);
                    foreach (var table in tables)
                    {
                        try
                        {
                            await targetDb.CreateTableAsync(table);
                            if (_logger != null)
                                await _logger.SuccessAsync($"Created table {table.FullName}");
                        }
                        catch (Exception ex)
                        {
                            errors.Add($"Table {table.FullName}: {ex.Message}");
                            schemaErrors++;
                        }
                        task.Increment(1);
                    }
                });

            if (options.MigrateIndexes)
            {
                var allIndexes = tables.SelectMany(t => t.Indexes).ToList();
                if (allIndexes.Count > 0)
                {
                    await AnsiConsole.Progress()
                        .AutoRefresh(true)
                        .HideCompleted(false)
                        .Columns(
                            new TaskDescriptionColumn(),
                            new ProgressBarColumn(),
                            new PercentageColumn(),
                            new SpinnerColumn())
                        .StartAsync(async ctx =>
                        {
                            var task = ctx.AddTask("[cyan]Indexes[/]", maxValue: allIndexes.Count);
                            foreach (var index in allIndexes)
                            {
                                try
                                {
                                    await targetDb.CreateIndexesAsync([index]);
                                }
                                catch (Exception ex)
                                {
                                    errors.Add($"Index {index.Name}: {ex.Message}");
                                }
                                task.Increment(1);
                            }
                        });
                }
            }

            if (options.MigrateForeignKeys)
            {
                var allFks = tables.SelectMany(t => t.ForeignKeys).ToList();
                if (allFks.Count > 0)
                {
                    await AnsiConsole.Progress()
                        .AutoRefresh(true)
                        .HideCompleted(false)
                        .Columns(
                            new TaskDescriptionColumn(),
                            new ProgressBarColumn(),
                            new PercentageColumn(),
                            new SpinnerColumn())
                        .StartAsync(async ctx =>
                        {
                            var task = ctx.AddTask("[cyan]Foreign Keys[/]", maxValue: allFks.Count);
                            foreach (var fk in allFks)
                            {
                                try
                                {
                                    await targetDb.CreateForeignKeysAsync([fk]);
                                }
                                catch (Exception ex)
                                {
                                    errors.Add($"FK {fk.Name}: {ex.Message}");
                                }
                                task.Increment(1);
                            }
                        });
                }
            }

            if (options.MigrateCheckConstraints)
            {
                var allChecks = tables.SelectMany(t => t.CheckConstraints).ToList();
                if (allChecks.Count > 0)
                {
                    AnsiConsole.MarkupLine("[bold]Creating check constraints...[/]");
                    await AnsiConsole.Progress()
                        .AutoRefresh(true)
                        .HideCompleted(false)
                        .Columns(
                            new TaskDescriptionColumn(),
                            new ProgressBarColumn(),
                            new PercentageColumn(),
                            new SpinnerColumn())
                        .StartAsync(async ctx =>
                        {
                            var task = ctx.AddTask("[cyan]Check Constraints[/]", maxValue: allChecks.Count);
                            foreach (var check in allChecks)
                            {
                                try
                                {
                                    await targetDb.CreateCheckConstraintAsync(check);
                                }
                                catch (Exception ex)
                                {
                                    errors.Add($"Check {check.Name}: {ex.Message}");
                                }
                                task.Increment(1);
                            }
                        });
                }

                var allUniques = tables.SelectMany(t => t.UniqueConstraints).ToList();
                if (allUniques.Count > 0)
                {
                    AnsiConsole.MarkupLine("[bold]Creating unique constraints...[/]");
                    await AnsiConsole.Progress()
                        .AutoRefresh(true)
                        .HideCompleted(false)
                        .Columns(
                            new TaskDescriptionColumn(),
                            new ProgressBarColumn(),
                            new PercentageColumn(),
                            new SpinnerColumn())
                        .StartAsync(async ctx =>
                        {
                            var task = ctx.AddTask("[cyan]Unique Constraints[/]", maxValue: allUniques.Count);
                            foreach (var unique in allUniques)
                            {
                                try
                                {
                                    await targetDb.CreateUniqueConstraintAsync(unique);
                                }
                                catch (Exception ex)
                                {
                                    errors.Add($"Unique {unique.Name}: {ex.Message}");
                                }
                                task.Increment(1);
                            }
                        });
                }
            }

            if (options.MigrateSequences && sequences.Count > 0)
            {
                AnsiConsole.MarkupLine("[bold]Creating sequences...[/]");
                await AnsiConsole.Progress()
                    .AutoRefresh(true)
                    .HideCompleted(false)
                    .Columns(
                        new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new PercentageColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx =>
                    {
                        var task = ctx.AddTask("[cyan]Sequences[/]", maxValue: sequences.Count);
                        foreach (var seq in sequences)
                        {
                            try
                            {
                                await targetDb.CreateSequenceAsync(seq);
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"Sequence {seq.FullName}: {ex.Message}");
                            }
                            task.Increment(1);
                        }
                    });
            }

            if (options.MigrateViews && views.Count > 0)
            {
                var sortedViews = TopologicalSortViews(views);
                await AnsiConsole.Progress()
                    .AutoRefresh(true)
                    .HideCompleted(false)
                    .Columns(
                        new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new PercentageColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx =>
                    {
                        var task = ctx.AddTask("[cyan]Views[/]", maxValue: sortedViews.Count);
                        foreach (var view in sortedViews)
                        {
                            try
                            {
                                await targetDb.CreateViewAsync(view);
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"View {view.FullName}: {ex.Message}");
                            }
                            task.Increment(1);
                        }
                    });
            }

            if (options.MigrateStoredProcedures && storedProcedures.Count > 0)
            {
                await AnsiConsole.Progress()
                    .AutoRefresh(true)
                    .HideCompleted(false)
                    .Columns(
                        new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new PercentageColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx =>
                    {
                        var task = ctx.AddTask("[cyan]Stored Procedures[/]", maxValue: storedProcedures.Count);
                        foreach (var sp in storedProcedures)
                        {
                            try
                            {
                                await targetDb.CreateStoredProcedureAsync(sp);
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"SP {sp.FullName}: {ex.Message}");
                            }
                            task.Increment(1);
                        }
                    });
            }

            AnsiConsole.MarkupLine("[green]  ✓[/] Schema migration phase complete.");
            AnsiConsole.WriteLine();
        }

        if (mode is "data" or "full")
        {
            var sortedTables = TopologicalSort(tables);
            var batchSize = Math.Clamp(options.BatchSize, 100, 50_000);

            var bulkMode = await targetDb.TryEnableBulkLoadModeAsync();
            if (bulkMode)
                AnsiConsole.MarkupLine("[green]  ✓[/] Bulk load mode enabled (FK checks deferred)");

            AnsiConsole.MarkupLine("[bold]Migrating data...[/]\n");

            foreach (var table in sortedTables)
            {
                var rowCount = rowCounts.GetValueOrDefault(table.FullName, 0);
                if (rowCount == 0)
                {
                    AnsiConsole.MarkupLine($"  [grey]⊘ {Markup.Escape(table.FullName)} (empty)[/]");
                    continue;
                }

                var identityColumn = table.Columns.FirstOrDefault(c => c.IsIdentity);
                var hasIdentity = identityColumn != null;
                var tableRows = 0L;

                try
                {
                    await AnsiConsole.Progress()
                        .AutoRefresh(true)
                        .HideCompleted(false)
                        .Columns(
                            new TaskDescriptionColumn() { Alignment = Justify.Left },
                            new ProgressBarColumn(),
                            new PercentageColumn(),
                            new RemainingTimeColumn(),
                            new SpinnerColumn())
                        .StartAsync(async ctx =>
                        {
                            var progressTask = ctx.AddTask(
                                $"[cyan]{Markup.Escape(table.FullName)}[/]",
                                maxValue: rowCount);

                            var offset = 0;
                            while (offset < rowCount)
                            {
                                var (columns, rows) = await sourceDb.ReadTableDataBatchAsync(
                                    table.Schema, table.Name, offset, batchSize);
                                if (rows.Count == 0) break;

                                var inserted = await targetDb.InsertDataAsync(
                                    table.Schema, table.Name, columns, rows, hasIdentity);
                                tableRows += inserted;
                                offset += rows.Count;
                                progressTask.Value = offset;
                            }

                            progressTask.Value = progressTask.MaxValue;
                        });

                    if (identityColumn != null)
                    {
                        await targetDb.ResetSequenceAsync(table.Schema, table.Name, identityColumn.Name);
                    }

                    await targetDb.AnalyzeTableAsync(table.Schema, table.Name);

                    if (options.ValidateData)
                    {
                        var targetRows = await targetDb.GetRowCountAsync(table.Schema, table.Name);
                        var sourceRows = await sourceDb.GetRowCountAsync(table.Schema, table.Name);
                        var match = sourceRows == targetRows;

                        if (_logger != null)
                            await _logger.WriteTableResultAsync(table.FullName, sourceRows, targetRows, match);

                        if (!match)
                        {
                            AnsiConsole.MarkupLine($"  [yellow]⚠[/] {Markup.Escape(table.FullName)}: row count mismatch [red]source={sourceRows:N0}[/] vs [green]target={targetRows:N0}[/]");
                            tablesFailed++;
                        }
                        else
                        {
                            tablesOk++;
                        }
                    }
                    else
                    {
                        tablesOk++;
                    }

                    totalRows += tableRows;
                    AnsiConsole.MarkupLine($"  [green]✓[/] {Markup.Escape(table.FullName)}: [bold]{tableRows:N0}[/] rows");
                }
                catch (Exception ex)
                {
                    tablesFailed++;
                    errors.Add($"Data {table.FullName}: {ex.Message}");
                    AnsiConsole.MarkupLine($"  [red]✗[/] {Markup.Escape(table.FullName)}: {Markup.Escape(ex.Message)}");
                }
            }

            if (bulkMode)
                await targetDb.DisableBulkLoadModeAsync();

            AnsiConsole.WriteLine();
            var rowsPerSec = totalRows > 0 ? totalRows : 0;
            AnsiConsole.MarkupLine($"[green]  ✓[/] Data migration complete. [bold]{totalRows:N0}[/] total rows migrated.");

            if (tablesFailed > 0)
            {
                AnsiConsole.MarkupLine($"[red]  ✗ {tablesFailed} table(s) failed or had validation errors.[/]");
            }

            if (_logger != null)
            {
                await _logger.WriteSummaryAsync(tablesOk, tablesFailed, totalRows, schemaErrors);
            }
        }

        if (errors.Count > 0)
        {
            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule("[red]Errors[/]").LeftJustified());
            var errorTable = new Table()
                .Border(TableBorder.Simple)
                .BorderColor(Color.Red)
                .AddColumn("#")
                .AddColumn("Error");
            for (int i = 0; i < errors.Count; i++)
            {
                errorTable.AddRow($"{i + 1}", Markup.Escape(errors[i]));
            }
            AnsiConsole.Write(errorTable);
            return 1;
        }

        return 0;
    }
    static List<TableModel> TopologicalSort(List<TableModel> tables)
    {
        var sorted = new List<TableModel>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var visiting = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tableMap = new Dictionary<string, TableModel>(StringComparer.OrdinalIgnoreCase);

        foreach (var t in tables)
        {
            tableMap.TryAdd(t.FullName, t);
            tableMap.TryAdd(t.Name, t);
        }

        void Visit(TableModel table)
        {
            if (visited.Contains(table.FullName)) return;
            if (visiting.Contains(table.FullName)) return; // cycle

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
                AnsiConsole.MarkupLine($"[yellow]  ⚠ Circular view dependency: {view.FullName}[/]");
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
}
