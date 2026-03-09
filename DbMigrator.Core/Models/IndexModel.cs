namespace DbMigrator.Core.Models;

public class IndexModel
{
    public string Name { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = new();
    public bool IsUnique { get; set; }
    public bool IsClustered { get; set; }
}
