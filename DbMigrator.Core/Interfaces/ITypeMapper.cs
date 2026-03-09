namespace DbMigrator.Core.Interfaces;

public interface ITypeMapper
{
    string ConvertType(string sourceType, int? maxLength = null, int? precision = null, int? scale = null);
}
