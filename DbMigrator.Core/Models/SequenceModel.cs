namespace DbMigrator.Core.Models;

public class SequenceModel
{
    public string Schema { get; set; } = "dbo";
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = "bigint";
    public long StartValue { get; set; } = 1;
    public long IncrementBy { get; set; } = 1;
    public long? MinValue { get; set; }
    public long? MaxValue { get; set; }
    public bool IsCycling { get; set; }
    public long CurrentValue { get; set; }
    public string FullName => $"{Schema}.{Name}";
}
