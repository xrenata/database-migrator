using Newtonsoft.Json;

namespace DbMigrator.CLI;

class MigrationCheckpoint
{
    private const string DefaultPath = ".migration_checkpoint.json";

    [JsonProperty("startedAt")]
    public DateTime StartedAt { get; set; } = DateTime.UtcNow;

    [JsonProperty("completedSchemas")]
    public HashSet<string> CompletedSchemas { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    [JsonProperty("completedData")]
    public HashSet<string> CompletedData { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    [JsonProperty("failedTables")]
    public HashSet<string> FailedTables { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public bool IsSchemaCompleted(string fullTableName) => CompletedSchemas.Contains(fullTableName);
    public bool IsDataCompleted(string fullTableName) => CompletedData.Contains(fullTableName);

    public void MarkSchemaCompleted(string fullTableName) => CompletedSchemas.Add(fullTableName);
    public void MarkDataCompleted(string fullTableName) => CompletedData.Add(fullTableName);
    public void MarkFailed(string fullTableName) => FailedTables.Add(fullTableName);

    public async Task SaveAsync(string path = DefaultPath)
    {
        var json = JsonConvert.SerializeObject(this, Formatting.Indented);
        await File.WriteAllTextAsync(path, json);
    }

    public static async Task<MigrationCheckpoint?> LoadAsync(string path = DefaultPath)
    {
        if (!File.Exists(path)) return null;
        try
        {
            var json = await File.ReadAllTextAsync(path);
            return JsonConvert.DeserializeObject<MigrationCheckpoint>(json);
        }
        catch
        {
            return null;
        }
    }

    public static void Delete(string path = DefaultPath)
    {
        if (File.Exists(path)) File.Delete(path);
    }
}
