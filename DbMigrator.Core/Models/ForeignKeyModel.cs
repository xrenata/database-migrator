namespace DbMigrator.Core.Models;

public class ForeignKeyModel
{
    public string Name { get; set; } = string.Empty;
    public string FromTable { get; set; } = string.Empty;
    public List<string> FromColumns { get; set; } = new();
    public string ToTable { get; set; } = string.Empty;
    public List<string> ToColumns { get; set; } = new();
    public string? OnDelete { get; set; }
    public string? OnUpdate { get; set; }
}
