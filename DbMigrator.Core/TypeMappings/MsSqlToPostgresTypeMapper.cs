using DbMigrator.Core.Interfaces;

namespace DbMigrator.Core.TypeMappings;

public class MsSqlToPostgresTypeMapper : ITypeMapper
{
    private static readonly Dictionary<string, string> TypeMappings = new(StringComparer.OrdinalIgnoreCase)
    {
        ["tinyint"] = "SMALLINT",
        ["smallint"] = "SMALLINT",
        ["int"] = "INTEGER",
        ["bigint"] = "BIGINT",

        ["decimal"] = "DECIMAL",
        ["numeric"] = "DECIMAL",
        ["money"] = "NUMERIC(19,4)",
        ["smallmoney"] = "NUMERIC(10,4)",
        ["float"] = "DOUBLE PRECISION",
        ["real"] = "REAL",

        ["char"] = "CHAR",
        ["varchar"] = "VARCHAR",
        ["nchar"] = "CHAR",
        ["nvarchar"] = "VARCHAR",
        ["text"] = "TEXT",
        ["ntext"] = "TEXT",
        ["sysname"] = "VARCHAR(128)",

        ["binary"] = "BYTEA",
        ["varbinary"] = "BYTEA",
        ["image"] = "BYTEA",
        ["timestamp"] = "BYTEA",
        ["rowversion"] = "BYTEA",

        ["bit"] = "BOOLEAN",

        ["date"] = "DATE",
        ["datetime"] = "TIMESTAMP",
        ["datetime2"] = "TIMESTAMP",
        ["smalldatetime"] = "TIMESTAMP",
        ["datetimeoffset"] = "TIMESTAMPTZ",
        ["time"] = "TIME",

        ["uniqueidentifier"] = "UUID",

        ["xml"] = "XML",

        ["json"] = "JSONB",

        ["geography"] = "TEXT",
        ["geometry"] = "TEXT",
        ["hierarchyid"] = "TEXT",

        ["sql_variant"] = "TEXT",
    };

    public string ConvertType(string sourceType, int? maxLength = null, int? precision = null, int? scale = null)
    {
        var normalizedType = sourceType.Trim();
        var baseType = normalizedType.Split('(')[0].ToLowerInvariant();

        if (!TypeMappings.TryGetValue(baseType, out var postgresType))
        {
            Console.WriteLine($"   [WARNING] Unknown SQL Server type '{sourceType}', defaulting to TEXT");
            return "TEXT";
        }

        if (postgresType.Contains('('))
            return postgresType;

        return baseType switch
        {
            "char" or "varchar" or "nchar" or "nvarchar" when maxLength.HasValue && maxLength.Value == -1
                => "TEXT",
            "char" or "varchar" or "nchar" or "nvarchar" when maxLength.HasValue && maxLength.Value > 0
                => $"{postgresType}({maxLength.Value})",
            "char" or "varchar" or "nchar" or "nvarchar"
                => $"{postgresType}(255)",
            "decimal" or "numeric" when precision.HasValue && scale.HasValue
                => $"{postgresType}({precision.Value},{scale.Value})",
            "decimal" or "numeric" when precision.HasValue
                => $"{postgresType}({precision.Value})",
            "datetime2" when precision.HasValue
                => $"{postgresType}({precision.Value})",
            "datetimeoffset" when precision.HasValue
                => $"{postgresType}({precision.Value})",
            "time" when precision.HasValue
                => $"{postgresType}({precision.Value})",
            _ => postgresType
        };
    }

    public static string ConvertSequenceType(string sqlServerType)
    {
        return sqlServerType.ToLowerInvariant() switch
        {
            "tinyint" or "smallint" => "SMALLINT",
            "int" => "INTEGER",
            "bigint" or "numeric" or "decimal" => "BIGINT",
            _ => "BIGINT"
        };
    }
}
