namespace DbMigrator.Core.Models;

public class CheckConstraintModel
{
    public string Name { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public string TableName { get; set; } = string.Empty;
    public string Expression { get; set; } = string.Empty;
    public string FullTableName => $"{Schema}.{TableName}";
}
