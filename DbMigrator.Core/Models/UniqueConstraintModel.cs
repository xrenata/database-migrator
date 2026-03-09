namespace DbMigrator.Core.Models;

public class UniqueConstraintModel
{
    public string Name { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public string TableName { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = new();
    public string FullTableName => $"{Schema}.{TableName}";
}
