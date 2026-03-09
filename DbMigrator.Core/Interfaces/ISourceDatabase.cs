using DbMigrator.Core.Models;

namespace DbMigrator.Core.Interfaces;

public interface ISourceDatabase
{
    Task ConnectAsync();
    Task DisconnectAsync();
    Task<bool> TestConnectionAsync();

    Task<List<TableModel>> GetTablesAsync();
    Task<List<ViewModel>> GetViewsAsync();
    Task<List<StoredProcedureModel>> GetStoredProceduresAsync();
    Task<List<string>> GetTableNamesAsync();
    Task<long> GetRowCountAsync(string schema, string tableName);
    Task<(List<string> Columns, List<List<object?>> Rows)> ReadTableDataBatchAsync(string schema, string tableName, int offset, int batchSize);

    Task<List<SequenceModel>> GetSequencesAsync() => Task.FromResult(new List<SequenceModel>());
    Task<string> GetCollationAsync() => Task.FromResult("unknown");
    Task<string> GetDatabaseNameAsync() => Task.FromResult("unknown");
}
