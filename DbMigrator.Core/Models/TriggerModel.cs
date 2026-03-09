namespace DbMigrator.Core.Models;

public class TriggerModel
{
    public string Schema { get; set; } = string.Empty;
    public string Table { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Definition { get; set; } = string.Empty;
    public bool IsAfter { get; set; } = true;
    public bool IsInsert { get; set; }
    public bool IsUpdate { get; set; }
    public bool IsDelete { get; set; }

    public string FullName => $"{Schema}.{Table}.{Name}";
}
