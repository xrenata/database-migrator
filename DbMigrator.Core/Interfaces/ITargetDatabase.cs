using DbMigrator.Core.Models;

namespace DbMigrator.Core.Interfaces;

public interface ITargetDatabase
{
    Task ConnectAsync();
    Task DisconnectAsync();
    Task<bool> TestConnectionAsync();

    Task CreateTableAsync(TableModel table);
    Task CreateForeignKeysAsync(List<ForeignKeyModel> foreignKeys);
    Task CreateIndexesAsync(List<IndexModel> indexes);
    Task CreateViewAsync(ViewModel view);
    Task CreateStoredProcedureAsync(StoredProcedureModel storedProcedure);

    Task<bool> TableExistsAsync(string schema, string tableName);
    Task DropTableAsync(string schema, string tableName);

    Task<int> InsertDataAsync(string schema, string tableName, List<string> columns, List<List<object?>> data, bool hasIdentityColumn = false);
    Task ResetSequenceAsync(string schema, string tableName, string identityColumn);

    Task CreateCheckConstraintAsync(CheckConstraintModel constraint) => Task.CompletedTask;
    Task CreateUniqueConstraintAsync(UniqueConstraintModel constraint) => Task.CompletedTask;
    Task CreateSequenceAsync(SequenceModel sequence) => Task.CompletedTask;
    Task<long> GetRowCountAsync(string schema, string tableName) => Task.FromResult(0L);
}
