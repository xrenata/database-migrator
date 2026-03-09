namespace DbMigrator.Core.Models;

public class ColumnModel
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
    public bool IsNullable { get; set; }
    public string? DefaultValue { get; set; }
    public bool IsIdentity { get; set; }
    public bool IsPrimaryKey { get; set; }
    public int? PrimaryKeyOrder { get; set; }
    public bool IsComputed { get; set; }
    public string? ComputedExpression { get; set; }
    public bool IsPersisted { get; set; }
}
