namespace DbMigrator.Core.Models;

public class StoredProcedureModel
{
    public string Schema { get; set; } = "dbo";
    public string Name { get; set; } = string.Empty;
    public string Definition { get; set; } = string.Empty;
    public List<ParameterModel> Parameters { get; set; } = new();
    
    public string FullName => $"{Schema}.{Name}";
}

public class ParameterModel
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public int? MaxLength { get; set; }
    public bool IsOutput { get; set; }
    
}
