using Newtonsoft.Json;

namespace DbMigrator.Core.Models;

public class ConfigModel
{
    [JsonProperty("source")]
    public SourceConfig Source { get; set; } = new();

    [JsonProperty("target")]
    public TargetConfig Target { get; set; } = new();

    [JsonProperty("options")]
    public MigrationOptions Options { get; set; } = new();

    [JsonProperty("tables")]
    public TableFilter Tables { get; set; } = new();
}

public class SourceConfig
{
    [JsonProperty("type")]
    public string Type { get; set; } = "MSSql";

    [JsonProperty("connectionString")]
    public string ConnectionString { get; set; } = string.Empty;
}

public class TargetConfig
{
    [JsonProperty("type")]
    public string Type { get; set; } = "Postgres";

    [JsonProperty("connectionString")]
    public string ConnectionString { get; set; } = string.Empty;
}

public class MigrationOptions
{
    [JsonProperty("dropTargetTables")]
    public bool DropTargetTables { get; set; } = false;

    [JsonProperty("createSchema")]
    public bool CreateSchema { get; set; } = true;

    [JsonProperty("migrateViews")]
    public bool MigrateViews { get; set; } = true;

    [JsonProperty("migrateStoredProcedures")]
    public bool MigrateStoredProcedures { get; set; } = true;

    [JsonProperty("migrateIndexes")]
    public bool MigrateIndexes { get; set; } = true;

    [JsonProperty("migrateForeignKeys")]
    public bool MigrateForeignKeys { get; set; } = true;

    [JsonProperty("migrateCheckConstraints")]
    public bool MigrateCheckConstraints { get; set; } = true;

    [JsonProperty("migrateSequences")]
    public bool MigrateSequences { get; set; } = true;

    [JsonProperty("batchSize")]
    public int BatchSize { get; set; } = 1000;

    [JsonProperty("parallelTables")]
    public int ParallelTables { get; set; } = 1;

    [JsonProperty("validateData")]
    public bool ValidateData { get; set; } = true;

    [JsonProperty("logFilePath")]
    public string? LogFilePath { get; set; }

    [JsonProperty("schemaMapping")]
    public Dictionary<string, string> SchemaMapping { get; set; } = new();
}

public class TableFilter
{
    [JsonProperty("include")]
    public List<string> Include { get; set; } = new();

    [JsonProperty("exclude")]
    public List<string> Exclude { get; set; } = new() { "sys_*" };
}
