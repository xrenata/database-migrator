using Microsoft.Data.SqlClient;
using DbMigrator.Core.Interfaces;
using DbMigrator.Core.Models;

namespace DbMigrator.MSSql;

public class MsSqlConnection : ISourceDatabase, IAsyncDisposable, IDisposable
{
    private readonly string _connectionString;
    private SqlConnection? _connection;

    public MsSqlConnection(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));
        _connectionString = connectionString;
    }

    private void EnsureConnected()
    {
        if (_connection == null || _connection.State != System.Data.ConnectionState.Open)
            throw new InvalidOperationException("Database connection is not open. Call ConnectAsync() first.");
    }

    public async Task ConnectAsync()
    {
        _connection = new SqlConnection(_connectionString);
        await _connection.OpenAsync();
    }

    public async Task DisconnectAsync()
    {
        if (_connection != null)
        {
            if (_connection.State != System.Data.ConnectionState.Closed)
                await _connection.CloseAsync();
            await _connection.DisposeAsync();
            _connection = null;
        }
    }

    public async Task<bool> TestConnectionAsync()
    {
        try
        {
            await ConnectAsync();
            await DisconnectAsync();
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   Connection test failed: {ex.Message}");
            try { await DisconnectAsync(); } catch { }
            return false;
        }
    }
    public async Task<string> GetDatabaseNameAsync()
    {
        EnsureConnected();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT DB_NAME()";
        var result = await cmd.ExecuteScalarAsync();
        return result?.ToString() ?? "Unknown";
    }

    public async Task<string> GetServerVersionAsync()
    {
        EnsureConnected();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT @@VERSION";
        var result = await cmd.ExecuteScalarAsync();
        var full = result?.ToString() ?? "";
        var idx = full.IndexOf('\n');
        return idx > 0 ? full[..idx].Trim() : full;
    }

    public async Task<string> GetCollationAsync()
    {
        EnsureConnected();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation')";
        var result = await cmd.ExecuteScalarAsync();
        return result?.ToString() ?? "unknown";
    }

    public async Task<Dictionary<string, long>> GetApproximateRowCountsAsync()
    {
        EnsureConnected();
        var counts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT s.name + '.' + t.name, SUM(p.rows)
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            counts[reader.GetString(0)] = reader.GetInt64(1);
        return counts;
    }
    public async Task<List<string>> GetTableNamesAsync()
    {
        EnsureConnected();
        var tables = new List<string>();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            tables.Add(reader.GetString(0));
        return tables;
    }
    public async Task<List<TableModel>> GetTablesAsync()
    {
        EnsureConnected();
        var tables = new Dictionary<string, TableModel>();

        using (var cmd = _connection!.CreateCommand())
        {
            cmd.CommandText = """
                SELECT
                    t.TABLE_SCHEMA, t.TABLE_NAME,
                    c.COLUMN_NAME, c.DATA_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
                    c.IS_NULLABLE, c.COLUMN_DEFAULT,
                    COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
                    COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME), c.COLUMN_NAME, 'IsComputed') as IS_COMPUTED
                FROM INFORMATION_SCHEMA.TABLES t
                INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
                """;
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var schema = reader.GetString(0);
                var tableName = reader.GetString(1);
                var fullName = $"{schema}.{tableName}";
                if (!tables.ContainsKey(fullName))
                    tables[fullName] = new TableModel { Schema = schema, Name = tableName };

                var isComputed = !reader.IsDBNull(10) && reader.GetInt32(10) == 1;
                tables[fullName].Columns.Add(new ColumnModel
                {
                    Name = reader.GetString(2),
                    DataType = reader.GetString(3),
                    MaxLength = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                    Precision = reader.IsDBNull(5) ? null : (int?)Convert.ToInt32(reader.GetValue(5)),
                    Scale = reader.IsDBNull(6) ? null : (int?)reader.GetInt32(6),
                    IsNullable = reader.GetString(7) == "YES",
                    DefaultValue = reader.IsDBNull(8) ? null : reader.GetString(8),
                    IsIdentity = !reader.IsDBNull(9) && reader.GetInt32(9) == 1,
                    IsComputed = isComputed
                });
            }
        }

        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = """
                SELECT OBJECT_SCHEMA_NAME(object_id), OBJECT_NAME(object_id), name, definition, is_persisted
                FROM sys.computed_columns
                ORDER BY OBJECT_SCHEMA_NAME(object_id), OBJECT_NAME(object_id)
                """;
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var fullName = $"{reader.GetString(0)}.{reader.GetString(1)}";
                if (tables.TryGetValue(fullName, out var table))
                {
                    var col = table.Columns.FirstOrDefault(c => c.Name == reader.GetString(2));
                    if (col != null)
                    {
                        col.IsComputed = true;
                        col.ComputedExpression = reader.IsDBNull(3) ? null : reader.GetString(3);
                        col.IsPersisted = reader.GetBoolean(4);
                    }
                }
            }
        }

        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = """
                SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.ORDINAL_POSITION
                """;
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var fullName = $"{reader.GetString(0)}.{reader.GetString(1)}";
                if (tables.TryGetValue(fullName, out var table))
                {
                    var col = table.Columns.FirstOrDefault(c => c.Name == reader.GetString(2));
                    if (col != null) { col.IsPrimaryKey = true; col.PrimaryKeyOrder = reader.GetInt32(3); }
                }
            }
        }

        var fkMap = new Dictionary<string, ForeignKeyModel>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = """
                SELECT fk.name,
                    OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id),
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id),
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id), OBJECT_NAME(fk.referenced_object_id),
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id),
                    delete_referential_action_desc, update_referential_action_desc
                FROM sys.foreign_keys fk
                INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                ORDER BY fk.name, fkc.constraint_column_id
                """;
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var fkName = reader.GetString(0);
                var fromSchema = reader.GetString(1); var fromTable = reader.GetString(2);
                var toSchema = reader.GetString(4); var toTable = reader.GetString(5);

                if (!fkMap.TryGetValue(fkName, out var fk))
                {
                    fk = new ForeignKeyModel
                    {
                        Name = fkName,
                        FromTable = $"{fromSchema}.{fromTable}",
                        ToTable = $"{toSchema}.{toTable}",
                        OnDelete = MapAction(reader.GetString(7)),
                        OnUpdate = MapAction(reader.GetString(8))
                    };
                    fkMap[fkName] = fk;
                }
                fk.FromColumns.Add(reader.GetString(3));
                fk.ToColumns.Add(reader.GetString(6));
            }
        }
        foreach (var fk in fkMap.Values)
        {
            if (tables.TryGetValue(fk.FromTable, out var table))
                table.ForeignKeys.Add(fk);
        }

        var indexes = new Dictionary<string, IndexModel>();
        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = """
                SELECT i.name, OBJECT_SCHEMA_NAME(i.object_id), OBJECT_NAME(i.object_id),
                    COL_NAME(i.object_id, ic.column_id), i.is_unique, i.type_desc
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                WHERE i.is_primary_key = 0 AND i.is_unique_constraint = 0
                    AND OBJECT_SCHEMA_NAME(i.object_id) != 'sys' AND i.name IS NOT NULL
                ORDER BY OBJECT_SCHEMA_NAME(i.object_id), OBJECT_NAME(i.object_id), i.name, ic.key_ordinal
                """;
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var key = $"{reader.GetString(1)}.{reader.GetString(2)}.{reader.GetString(0)}";
                if (!indexes.ContainsKey(key))
                    indexes[key] = new IndexModel
                    {
                        Name = reader.GetString(0),
                        TableName = $"{reader.GetString(1)}.{reader.GetString(2)}",
                        IsUnique = reader.GetBoolean(4),
                        IsClustered = reader.GetString(5) == "CLUSTERED",
                        Columns = new List<string>()
                    };
                indexes[key].Columns.Add(reader.GetString(3));
            }
        }
        foreach (var ix in indexes.Values)
            if (tables.TryGetValue(ix.TableName, out var t)) t.Indexes.Add(ix);

        var uqMap = new Dictionary<string, UniqueConstraintModel>();
        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = """
                SELECT tc.CONSTRAINT_NAME, tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                WHERE tc.CONSTRAINT_TYPE = 'UNIQUE'
                ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                """;
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var name = reader.GetString(0);
                if (!uqMap.TryGetValue(name, out var uq))
                {
                    uq = new UniqueConstraintModel
                    {
                        Name = name,
                        Schema = reader.GetString(1),
                        TableName = reader.GetString(2)
                    };
                    uqMap[name] = uq;
                }
                uq.Columns.Add(reader.GetString(3));
            }
        }
        foreach (var uq in uqMap.Values)
            if (tables.TryGetValue(uq.FullTableName, out var t)) t.UniqueConstraints.Add(uq);

        using (var cmd = _connection.CreateCommand())
        {
            cmd.CommandText = """
                SELECT cc.name, OBJECT_SCHEMA_NAME(cc.parent_object_id), OBJECT_NAME(cc.parent_object_id), cc.definition
                FROM sys.check_constraints cc
                WHERE cc.is_disabled = 0
                ORDER BY OBJECT_SCHEMA_NAME(cc.parent_object_id), OBJECT_NAME(cc.parent_object_id)
                """;
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var schema = reader.GetString(1); var tblName = reader.GetString(2);
                var fullName = $"{schema}.{tblName}";
                if (tables.TryGetValue(fullName, out var table))
                    table.CheckConstraints.Add(new CheckConstraintModel
                    {
                        Name = reader.GetString(0),
                        Schema = schema,
                        TableName = tblName,
                        Expression = reader.GetString(3)
                    });
            }
        }

        return tables.Values.OrderBy(t => t.Schema).ThenBy(t => t.Name).ToList();
    }
    public async Task<List<SequenceModel>> GetSequencesAsync()
    {
        EnsureConnected();
        var sequences = new List<SequenceModel>();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT s.name, seq.name,
                TYPE_NAME(seq.system_type_id),
                CAST(seq.start_value AS BIGINT), CAST(seq.increment AS BIGINT),
                CAST(seq.minimum_value AS BIGINT), CAST(seq.maximum_value AS BIGINT),
                seq.is_cycling, CAST(seq.current_value AS BIGINT)
            FROM sys.sequences seq
            INNER JOIN sys.schemas s ON seq.schema_id = s.schema_id
            ORDER BY s.name, seq.name
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            sequences.Add(new SequenceModel
            {
                Schema = reader.GetString(0),
                Name = reader.GetString(1),
                DataType = reader.GetString(2),
                StartValue = reader.GetInt64(3),
                IncrementBy = reader.GetInt64(4),
                MinValue = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                MaxValue = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                IsCycling = reader.GetBoolean(7),
                CurrentValue = reader.IsDBNull(8) ? 0 : reader.GetInt64(8)
            });
        }
        return sequences;
    }
    public async Task<List<ViewModel>> GetViewsAsync()
    {
        EnsureConnected();
        var views = new List<ViewModel>();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA != 'sys'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            views.Add(new ViewModel
            {
                Schema = reader.GetString(0),
                Name = reader.GetString(1),
                Definition = reader.IsDBNull(2) ? "" : reader.GetString(2)
            });
        return views;
    }
    public async Task<List<StoredProcedureModel>> GetStoredProceduresAsync()
    {
        EnsureConnected();
        var procedures = new List<StoredProcedureModel>();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_DEFINITION
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE' AND SPECIFIC_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
            ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            procedures.Add(new StoredProcedureModel
            {
                Schema = reader.GetString(0),
                Name = reader.GetString(1),
                Definition = reader.IsDBNull(2) ? "-- encrypted" : reader.GetString(2)
            });
        return procedures;
    }
    public async Task<List<TriggerModel>> GetTriggersAsync()
    {
        EnsureConnected();
        var triggers = new List<TriggerModel>();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT
                OBJECT_SCHEMA_NAME(t.parent_id),
                OBJECT_NAME(t.parent_id),
                t.name,
                OBJECT_DEFINITION(t.object_id),
                CASE WHEN OBJECTPROPERTY(t.object_id, 'ExecIsInsertTrigger') = 1 THEN 1 ELSE 0 END,
                CASE WHEN OBJECTPROPERTY(t.object_id, 'ExecIsUpdateTrigger') = 1 THEN 1 ELSE 0 END,
                CASE WHEN OBJECTPROPERTY(t.object_id, 'ExecIsDeleteTrigger') = 1 THEN 1 ELSE 0 END,
                CASE WHEN OBJECTPROPERTY(t.object_id, 'ExecIsAfterTrigger') = 1 THEN 1 ELSE 0 END
            FROM sys.triggers t
            WHERE t.parent_class = 1
            ORDER BY OBJECT_SCHEMA_NAME(t.parent_id), OBJECT_NAME(t.parent_id), t.name
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            triggers.Add(new TriggerModel
            {
                Schema = reader.GetString(0),
                Table = reader.GetString(1),
                Name = reader.GetString(2),
                Definition = reader.IsDBNull(3) ? "-- encrypted" : reader.GetString(3),
                IsInsert = reader.GetInt32(4) == 1,
                IsUpdate = reader.GetInt32(5) == 1,
                IsDelete = reader.GetInt32(6) == 1,
                IsAfter = reader.GetInt32(7) == 1
            });
        }
        return triggers;
    }
    public async Task<List<(string Schema, string Name, string Definition)>> GetUserDefinedFunctionsAsync()
    {
        EnsureConnected();
        var funcs = new List<(string, string, string)>();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_DEFINITION
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'FUNCTION' AND SPECIFIC_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
            ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME
            """;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            funcs.Add((reader.GetString(0), reader.GetString(1),
                reader.IsDBNull(2) ? "-- encrypted" : reader.GetString(2)));
        return funcs;
    }
    public async Task<long> GetRowCountAsync(string schema, string tableName)
    {
        EnsureConnected();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = """
            SELECT SUM(p.rows)
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
            WHERE s.name = @schema AND t.name = @tableName
            """;
        cmd.Parameters.Add(new SqlParameter("@schema", schema));
        cmd.Parameters.Add(new SqlParameter("@tableName", tableName));
        var result = await cmd.ExecuteScalarAsync();
        return result != null && result != DBNull.Value ? Convert.ToInt64(result) : 0;
    }
    public async Task<(List<string> Columns, List<List<object?>> Rows)> ReadTableDataBatchAsync(
        string schema, string tableName, int offset, int batchSize)
    {
        EnsureConnected();
        var columns = new List<string>();
        var rows = new List<List<object?>>(batchSize);

        using var cmd = _connection!.CreateCommand();
        cmd.CommandTimeout = 0;
        cmd.CommandText = $"""
            SELECT * FROM [{schema}].[{tableName}]
            ORDER BY (SELECT NULL)
            OFFSET @offset ROWS FETCH NEXT @batchSize ROWS ONLY
            """;
        cmd.Parameters.Add(new SqlParameter("@offset", offset));
        cmd.Parameters.Add(new SqlParameter("@batchSize", batchSize));

        using var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess);
        for (int i = 0; i < reader.FieldCount; i++)
            columns.Add(reader.GetName(i));

        while (await reader.ReadAsync())
        {
            var row = new List<object?>(reader.FieldCount);
            for (int i = 0; i < reader.FieldCount; i++)
                row.Add(reader.IsDBNull(i) ? null : reader.GetValue(i));
            rows.Add(row);
        }
        return (columns, rows);
    }

    public async Task<(List<string> Columns, List<List<object?>> Rows)> ReadTableDataBatchWithPkAsync(
        string schema, string tableName, List<string> pkColumns, int offset, int batchSize)
    {
        EnsureConnected();
        var columns = new List<string>();
        var rows = new List<List<object?>>(batchSize);

        var orderBy = string.Join(", ", pkColumns.Select(c => $"[{c}]"));

        using var cmd = _connection!.CreateCommand();
        cmd.CommandTimeout = 0;
        cmd.CommandText = $"""
            SELECT * FROM [{schema}].[{tableName}]
            ORDER BY {orderBy}
            OFFSET @offset ROWS FETCH NEXT @batchSize ROWS ONLY
            """;
        cmd.Parameters.Add(new SqlParameter("@offset", offset));
        cmd.Parameters.Add(new SqlParameter("@batchSize", batchSize));

        using var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess);
        for (int i = 0; i < reader.FieldCount; i++)
            columns.Add(reader.GetName(i));

        while (await reader.ReadAsync())
        {
            var row = new List<object?>(reader.FieldCount);
            for (int i = 0; i < reader.FieldCount; i++)
                row.Add(reader.IsDBNull(i) ? null : reader.GetValue(i));
            rows.Add(row);
        }
        return (columns, rows);
    }

    private static string? MapAction(string action) => action switch
    {
        "CASCADE" => "CASCADE",
        "NO_ACTION" => "NO ACTION",
        "SET_NULL" => "SET NULL",
        "SET_DEFAULT" => "SET DEFAULT",
        _ => null
    };

    public async ValueTask DisposeAsync() => await DisconnectAsync();
    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();
}
