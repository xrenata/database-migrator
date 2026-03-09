namespace DbMigrator.Core.Models;

public enum TriggerConversionResult
{
    /// <summary>Trigger was auto-converted to valid PL/pgSQL.</summary>
    AutoConverted,

    /// <summary>Trigger was created as a placeholder — manual T-SQL→PL/pgSQL conversion required.</summary>
    Placeholder,
}
