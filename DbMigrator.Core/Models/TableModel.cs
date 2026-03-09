namespace DbMigrator.Core.Models;

public class TableModel
{
    public string Schema { get; set; } = "dbo";
    public string Name { get; set; } = string.Empty;
    public List<ColumnModel> Columns { get; set; } = new();
    public List<ForeignKeyModel> ForeignKeys { get; set; } = new();
    public List<IndexModel> Indexes { get; set; } = new();
    public List<CheckConstraintModel> CheckConstraints { get; set; } = new();
    public List<UniqueConstraintModel> UniqueConstraints { get; set; } = new();

    public string FullName => $"{Schema}.{Name}";

    public List<string> GetPrimaryKeyColumns()
    {
        return Columns
            .Where(c => c.IsPrimaryKey)
            .OrderBy(c => c.PrimaryKeyOrder)
            .Select(c => c.Name)
            .ToList();
    }
}
