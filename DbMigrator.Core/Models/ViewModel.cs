namespace DbMigrator.Core.Models;

public class ViewModel
{
    public string Schema { get; set; } = "dbo";
    public string Name { get; set; } = string.Empty;
    public string Definition { get; set; } = string.Empty;

    public string FullName => $"{Schema}.{Name}";
}
