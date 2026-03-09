using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using DbMigrator.Core.Interfaces;
using DbMigrator.Core.Models;
using DbMigrator.Core.TypeMappings;

namespace DbMigrator.Postgres;

public class PostgresConnection : ITargetDatabase, IAsyncDisposable, IDisposable
{
    private readonly string _connectionString;
    private readonly ITypeMapper _typeMapper;
    private NpgsqlConnection? _connection;
    private Dictionary<string, string> _schemaMapping = new();

    public PostgresConnection(string connectionString, ITypeMapper? typeMapper = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        _connectionString = connectionString;
        _typeMapper = typeMapper ?? new MsSqlToPostgresTypeMapper();
    }

    public void SetSchemaMapping(Dictionary<string, string> mapping)
    {
        _schemaMapping = new Dictionary<string, string>(mapping, StringComparer.OrdinalIgnoreCase);
    }

    private string MapSchema(string sourceSchema)
    {
        if (_schemaMapping == null)
            return sourceSchema;
        return _schemaMapping.TryGetValue(sourceSchema, out var mapped) ? mapped : sourceSchema;
    }

    private void EnsureConnected()
    {
        if (_connection == null || _connection.State != System.Data.ConnectionState.Open)
            throw new InvalidOperationException("Database connection is not open. Call ConnectAsync() first.");
    }

    public async Task ConnectAsync()
    {
        _connection = new NpgsqlConnection(_connectionString);
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
        catch (PostgresException ex) when (ex.SqlState == "3D000")
        {
            // Database does not exist - throw special exception
            try { await DisconnectAsync(); } catch { }
            throw new DatabaseNotFoundException(ex.Message);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   Connection test failed: {ex.Message}");
            try { await DisconnectAsync(); } catch { }
            return false;
        }
    }

    public async Task<bool> TableExistsAsync(string schema, string tableName)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(schema);
        using var command = _connection!.CreateCommand();
        command.CommandText = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = @schema
                AND table_name = @tableName
            )
            """;
        command.Parameters.AddWithValue("@schema", mappedSchema);
        command.Parameters.AddWithValue("@tableName", tableName);

        var result = await command.ExecuteScalarAsync();
        return Convert.ToBoolean(result);
    }

    public async Task DropTableAsync(string schema, string tableName)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(schema);
        using var command = _connection!.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS \"{mappedSchema}\".\"{tableName}\" CASCADE";
        await command.ExecuteNonQueryAsync();
    }

    public async Task CreateTableAsync(TableModel table)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(table.Schema);
        var columnDefs = new List<string>();

        foreach (var column in table.Columns)
        {
            if (column.IsComputed && !column.IsPersisted)
                continue;

            var columnDef = $"    \"{column.Name}\" {_typeMapper.ConvertType(column.DataType, column.MaxLength, column.Precision, column.Scale)}";

            if (column.IsIdentity)
            {
                columnDef += " GENERATED ALWAYS AS IDENTITY";
            }

            if (!column.IsNullable)
            {
                columnDef += " NOT NULL";
            }

            if (!string.IsNullOrEmpty(column.DefaultValue) && !column.IsIdentity && !column.IsComputed)
            {
                var defaultValue = ConvertDefaultValue(column.DefaultValue, column.DataType);
                if (!string.IsNullOrEmpty(defaultValue))
                {
                    columnDef += $" DEFAULT {defaultValue}";
                }
            }

            columnDefs.Add(columnDef);
        }

        var primaryKeyColumns = table.Columns
            .Where(c => c.IsPrimaryKey)
            .OrderBy(c => c.PrimaryKeyOrder)
            .Select(c => $"\"{c.Name}\"")
            .ToList();

        if (primaryKeyColumns.Count > 0)
        {
            columnDefs.Add($"    PRIMARY KEY ({string.Join(", ", primaryKeyColumns)})");
        }

        var sql = $"CREATE TABLE \"{mappedSchema}\".\"{table.Name}\" (\n{string.Join(",\n", columnDefs)}\n)";

        await CreateSchemaIfNotExistsAsync(mappedSchema);

        using var command = _connection!.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static string QuoteTable(string tableName)
    {
        var parts = tableName.Split('.');
        return parts.Length == 2 ? $"\"{parts[0]}\".\"{parts[1]}\"" : $"\"{tableName}\"";
    }

    private string QuoteTableMapped(string tableName)
    {
        var parts = tableName.Split('.');
        if (parts.Length == 2)
        {
            var mappedSchema = MapSchema(parts[0]);
            return $"\"{mappedSchema}\".\"{parts[1]}\"";
        }
        return $"\"{tableName}\"";
    }

    public async Task CreateForeignKeysAsync(List<ForeignKeyModel> foreignKeys)
    {
        EnsureConnected();
        foreach (var fk in foreignKeys)
        {
            var fromCols = string.Join(", ", fk.FromColumns.Select(c => $"\"{c}\""));
            var toCols = string.Join(", ", fk.ToColumns.Select(c => $"\"{c}\""));

            var sql = $"""
                ALTER TABLE {QuoteTableMapped(fk.FromTable)}
                ADD CONSTRAINT "{fk.Name}"
                FOREIGN KEY ({fromCols})
                REFERENCES {QuoteTableMapped(fk.ToTable)} ({toCols})
                """;

            if (!string.IsNullOrEmpty(fk.OnDelete))
            {
                sql += $" ON DELETE {fk.OnDelete}";
            }

            if (!string.IsNullOrEmpty(fk.OnUpdate))
            {
                sql += $" ON UPDATE {fk.OnUpdate}";
            }

            using var command = _connection!.CreateCommand();
            command.CommandText = sql;
            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (PostgresException ex) when (ex.SqlState == "42P07" || ex.SqlState == "42710")
            {
                // already exists — skip silently
            }
        }
    }

    public async Task CreateIndexesAsync(List<IndexModel> indexes)
    {
        EnsureConnected();
        foreach (var index in indexes)
        {
            var uniquePart = index.IsUnique ? "UNIQUE " : "";
            var columns = string.Join(", ", index.Columns.Select(c => $"\"{c}\""));
            var sql = $"CREATE {uniquePart}INDEX IF NOT EXISTS \"{index.Name}\" ON {QuoteTableMapped(index.TableName)} ({columns})";

            using var command = _connection!.CreateCommand();
            try
            {
                command.CommandText = sql;
                await command.ExecuteNonQueryAsync();
            }
            catch (PostgresException ex) when (ex.SqlState == "42P07")
            {
                // already exists — skip silently
            }
        }
    }

    public async Task CreateViewAsync(ViewModel view)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(view.Schema);
        var convertedDefinition = ConvertViewDefinition(view.Definition);

        var sql = $"""
            CREATE OR REPLACE VIEW "{mappedSchema}"."{view.Name}" AS
            {convertedDefinition}
            """;

        await CreateSchemaIfNotExistsAsync(mappedSchema);

        using var command = _connection!.CreateCommand();
        try
        {
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex)
        {
            throw new InvalidOperationException($"Could not create view {view.FullName}: {ex.Message}", ex);
        }
    }

    public async Task CreateStoredProcedureAsync(StoredProcedureModel storedProcedure)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(storedProcedure.Schema);
        await CreateSchemaIfNotExistsAsync(mappedSchema);

        var escapedDefinition = storedProcedure.Definition
            .Replace("*/", "* /");

        var sql = $"""
            CREATE OR REPLACE FUNCTION "{mappedSchema}"."{storedProcedure.Name}_todo"()
            RETURNS VOID AS $$
            BEGIN
                RAISE NOTICE 'Stored procedure %.% requires manual conversion from T-SQL to PL/pgSQL.',
                    '{mappedSchema.Replace("'", "''")}',
                    '{storedProcedure.Name.Replace("'", "''")}';
            END;
            $$ LANGUAGE plpgsql;
            COMMENT ON FUNCTION "{mappedSchema}"."{storedProcedure.Name}_todo"() IS
            'Original T-SQL definition - requires manual conversion:
            {escapedDefinition.Replace("'", "''")}';
            """;

        using var command = _connection!.CreateCommand();
        try
        {
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
        catch (PostgresException)
        {
        }
    }

    public async Task<TriggerConversionResult> CreateTriggerAsync(TriggerModel trigger)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(trigger.Schema);
        await CreateSchemaIfNotExistsAsync(mappedSchema);

        // 1. Try timestamp auto-conversion (existing pattern)
        var simple = TryConvertTrigger(trigger);
        if (simple.IsConverted)
        {
            using var cmd = _connection!.CreateCommand();
            try
            {
                cmd.CommandText = simple.Sql;
                await cmd.ExecuteNonQueryAsync();
                return TriggerConversionResult.AutoConverted;
            }
            catch (PostgresException)
            {
                // fall through to audit log attempt
            }
        }

        // 2. Try audit log pattern (AFTER DELETE/UPDATE → INSERT INTO LOG_<table>)
        if (trigger.IsDelete || trigger.IsUpdate)
        {
            var auditSql = await TryBuildAuditLogTriggerAsync(trigger, mappedSchema);
            if (auditSql != null)
            {
                using var cmd = _connection!.CreateCommand();
                try
                {
                    cmd.CommandText = auditSql;
                    await cmd.ExecuteNonQueryAsync();
                    return TriggerConversionResult.AutoConverted;
                }
                catch (PostgresException)
                {
                    // fall through to placeholder
                }
            }
        }

        // 3. Create placeholder for complex / unrecognised triggers
        await CreatePlaceholderTriggerAsync(trigger);
        return TriggerConversionResult.Placeholder;
    }

    /// <summary>
    /// Detects the common audit-log pattern:
    ///   AFTER DELETE/UPDATE → INSERT INTO LOG_&lt;table&gt; (log_type, cols…) VALUES ('D'/'U', OLD.cols…)
    /// Returns ready-to-execute SQL or null if the LOG table doesn't exist.
    /// </summary>
    private async Task<string?> TryBuildAuditLogTriggerAsync(TriggerModel trigger, string mappedSchema)
    {
        var logTable = $"LOG_{trigger.Table}";

        // Check LOG table exists
        using var checkCmd = _connection!.CreateCommand();
        checkCmd.CommandText = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = @schema AND table_name = @logTable
            )
            """;
        checkCmd.Parameters.AddWithValue("@schema", mappedSchema);
        checkCmd.Parameters.AddWithValue("@logTable", logTable);
        var exists = Convert.ToBoolean(await checkCmd.ExecuteScalarAsync());
        if (!exists) return null;

        // Get non-identity, non-log_type columns of LOG table
        using var colCmd = _connection.CreateCommand();
        colCmd.CommandText = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = @schema
              AND table_name   = @logTable
              AND is_identity  = 'NO'
              AND column_name != 'log_type'
            ORDER BY ordinal_position
            """;
        colCmd.Parameters.AddWithValue("@schema", mappedSchema);
        colCmd.Parameters.AddWithValue("@logTable", logTable);

        var cols = new List<string>();
        using var reader = await colCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            cols.Add(reader.GetString(0));

        if (cols.Count == 0) return null;

        var logTypeVal = trigger.IsDelete ? "D" : "U";
        var colList  = string.Join(", ", cols.Select(c => $"\"{c}\""));
        var valList  = string.Join(", ", cols.Select(c => $"OLD.\"{c}\""));
        var funcName = $"fn_{trigger.Table}_{trigger.Name}".ToLowerInvariant();

        return $"""
            CREATE OR REPLACE FUNCTION "{mappedSchema}"."{funcName}"()
            RETURNS TRIGGER AS $t$
            BEGIN
                INSERT INTO "{mappedSchema}"."{logTable}" ("log_type", {colList})
                VALUES ('{logTypeVal}', {valList});
                RETURN OLD;
            END;
            $t$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS "{trigger.Name}" ON "{mappedSchema}"."{trigger.Table}";

            CREATE TRIGGER "{trigger.Name}"
            AFTER {(trigger.IsDelete ? "DELETE" : "UPDATE")} ON "{mappedSchema}"."{trigger.Table}"
            FOR EACH ROW EXECUTE FUNCTION "{mappedSchema}"."{funcName}"();
            """;
    }

    private async Task CreatePlaceholderTriggerAsync(TriggerModel trigger)
    {
        var mappedSchema = MapSchema(trigger.Schema);
        var escapedDefinition = trigger.Definition.Replace("'", "''").Replace("*/", "* /");

        var events = new List<string>();
        if (trigger.IsInsert) events.Add("INSERT");
        if (trigger.IsUpdate) events.Add("UPDATE");
        if (trigger.IsDelete) events.Add("DELETE");
        var eventList = string.Join(" OR ", events);

        var sql = $"""
            CREATE OR REPLACE FUNCTION "{mappedSchema}".fn_{trigger.Table}_{trigger.Name}_todo()
            RETURNS TRIGGER AS $$
            BEGIN
                RAISE NOTICE 'Trigger {trigger.Name} on {trigger.Table} requires manual conversion from T-SQL to PL/pgSQL.';
                RAISE NOTICE 'Events: {eventList}';
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS "{trigger.Name}" ON "{mappedSchema}"."{trigger.Table}";

            CREATE TRIGGER "{trigger.Name}"
            AFTER {eventList} ON "{mappedSchema}"."{trigger.Table}"
            FOR EACH ROW
            EXECUTE FUNCTION "{mappedSchema}".fn_{trigger.Table}_{trigger.Name}_todo();

            COMMENT ON FUNCTION "{mappedSchema}".fn_{trigger.Table}_{trigger.Name}_todo() IS
            'Original T-SQL trigger - requires manual conversion:
            {escapedDefinition}';
            """;

        using var command = _connection!.CreateCommand();
        try
        {
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
            // Placeholder created — caller reports summary, no per-trigger console noise
        }
        catch (PostgresException ex)
        {
            Console.WriteLine($"   [WARN] Could not create placeholder for {trigger.FullName}: {ex.Message}");
        }
    }

    private (bool IsConverted, string Sql) TryConvertTrigger(TriggerModel trigger)
    {
        var def = trigger.Definition.ToUpperInvariant();

        // Pattern 1: Simple timestamp update (UpdatedAt, CreatedAt, ModifiedDate, etc.)
        // UPDATE t SET CreatedAt = SYSDATETIME() WHERE Id IN (SELECT Id FROM inserted)
        if (def.Contains("UPDATE") && def.Contains("SET") &&
            (def.Contains("UPDATEDAT") || def.Contains("CREATEDAT") || def.Contains("MODIFIEDDATE") ||
             def.Contains("CHANGEDAT") || def.Contains("MODIFIEDAT") || def.Contains("LASTMODIFIED") ||
             def.Contains("UPDATETIME") || def.Contains("MODIFIED")) &&
            (def.Contains("GETDATE()") || def.Contains("SYSDATETIME()") || def.Contains("CURRENT_TIMESTAMP")))
        {
            var mappedSchema = MapSchema(trigger.Schema);
            var events = new List<string>();
            if (trigger.IsInsert) events.Add("INSERT");
            if (trigger.IsUpdate) events.Add("UPDATE");
            var eventList = string.Join(" OR ", events);

            // Find the column name (UpdatedAt, CreatedAt, ModifiedDate, etc.)
            string? timestampColumn = null;
            var columns = new[] { "UpdatedAt", "CreatedAt", "MODIFIEDDATE", "CHANGEDAT",
                                   "MODIFIEDAT", "LastModified", "UpdateTime" };
            foreach (var col in columns)
            {
                if (def.Contains(col.ToUpperInvariant()))
                {
                    timestampColumn = col;
                    break;
                }
            }

            if (timestampColumn != null)
            {
                var sql = $"""
                    CREATE OR REPLACE FUNCTION "{mappedSchema}".fn_{trigger.Table}_{trigger.Name}()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW."{timestampColumn}" = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;

                    DROP TRIGGER IF EXISTS "{trigger.Name}" ON "{mappedSchema}"."{trigger.Table}";

                    CREATE TRIGGER "{trigger.Name}"
                    BEFORE {eventList} ON "{mappedSchema}"."{trigger.Table}"
                    FOR EACH ROW
                    EXECUTE FUNCTION "{mappedSchema}".fn_{trigger.Table}_{trigger.Name}();
                    """;
                return (true, sql);
            }
        }

        // Pattern 2: Simple status/audit log (could add more patterns here)
        // For now, mark as not convertible
        return (false, string.Empty);
    }

    public async Task CreateCheckConstraintAsync(CheckConstraintModel constraint)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(constraint.Schema);

        var expression = ConvertCheckExpression(constraint.Expression);

        var sql = $"""
            ALTER TABLE "{mappedSchema}"."{constraint.TableName}"
            ADD CONSTRAINT "{constraint.Name}" CHECK ({expression})
            """;

        using var command = _connection!.CreateCommand();
        try
        {
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex) when (ex.SqlState == "42710") // duplicate
        {
            // already exists — skip silently
        }
    }

    public async Task CreateUniqueConstraintAsync(UniqueConstraintModel constraint)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(constraint.Schema);
        var columns = string.Join(", ", constraint.Columns.Select(c => $"\"{c}\""));

        var sql = $"""
            ALTER TABLE "{mappedSchema}"."{constraint.TableName}"
            ADD CONSTRAINT "{constraint.Name}" UNIQUE ({columns})
            """;

        using var command = _connection!.CreateCommand();
        try
        {
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex) when (ex.SqlState == "42710")
        {
            // already exists — skip silently
        }
    }

    public async Task CreateSequenceAsync(SequenceModel sequence)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(sequence.Schema);
        var pgType = MsSqlToPostgresTypeMapper.ConvertSequenceType(sequence.DataType);

        await CreateSchemaIfNotExistsAsync(mappedSchema);

        var sb = new StringBuilder();
        sb.Append($"CREATE SEQUENCE IF NOT EXISTS \"{mappedSchema}\".\"{sequence.Name}\" AS {pgType}");
        sb.Append($" START WITH {sequence.CurrentValue}");
        sb.Append($" INCREMENT BY {sequence.IncrementBy}");
        if (sequence.MinValue.HasValue) sb.Append($" MINVALUE {sequence.MinValue.Value}");
        if (sequence.MaxValue.HasValue) sb.Append($" MAXVALUE {sequence.MaxValue.Value}");
        if (sequence.IsCycling) sb.Append(" CYCLE");
        else sb.Append(" NO CYCLE");

        using var command = _connection!.CreateCommand();
        command.CommandText = sb.ToString();
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex) when (ex.SqlState == "42P07")
        {
            // already exists — skip silently
        }
    }

    public async Task<long> GetRowCountAsync(string schema, string tableName)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(schema);
        using var command = _connection!.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM \"{mappedSchema}\".\"{tableName}\"";
        command.CommandTimeout = 0;
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    public async Task<int> InsertDataAsync(string schema, string tableName, List<string> columns, List<List<object?>> data, bool hasIdentityColumn = false)
    {
        EnsureConnected();
        if (data.Count == 0) return 0;

        var mappedSchema = MapSchema(schema);
        var columnList = string.Join(", ", columns.Select(c => $"\"{c}\""));
        var overriding = hasIdentityColumn ? " OVERRIDING SYSTEM VALUE" : "";

        var maxRowsPerBatch = Math.Clamp(65000 / Math.Max(1, columns.Count), 1, 500);
        var rowsAffected = 0;

        using var transaction = await _connection!.BeginTransactionAsync();
        try
        {
            for (int batchStart = 0; batchStart < data.Count; batchStart += maxRowsPerBatch)
            {
                var batchEnd = Math.Min(batchStart + maxRowsPerBatch, data.Count);
                var batchCount = batchEnd - batchStart;

                using var cmd = _connection.CreateCommand();
                cmd.Transaction = transaction;
                cmd.CommandTimeout = 0;

                var sql = new StringBuilder(256 + batchCount * columns.Count * 8);
                sql.Append($"INSERT INTO \"{mappedSchema}\".\"{tableName}\" ({columnList}){overriding} VALUES ");

                for (int r = 0; r < batchCount; r++)
                {
                    if (r > 0) sql.Append(", ");
                    sql.Append('(');

                    var rowData = data[batchStart + r];
                    for (int c = 0; c < columns.Count; c++)
                    {
                        if (c > 0) sql.Append(", ");
                        var pname = $"@p{r}_{c}";
                        var value = rowData[c] ?? DBNull.Value;

                        sql.Append(pname);
                        cmd.Parameters.Add(new NpgsqlParameter(pname, value));
                    }
                    sql.Append(')');
                }

                cmd.CommandText = sql.ToString();
                rowsAffected += await cmd.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }

        return rowsAffected;
    }

    public async Task<bool> TryEnableBulkLoadModeAsync()
    {
        EnsureConnected();
        try
        {
            using var cmd = _connection!.CreateCommand();
            cmd.CommandText = "SET session_replication_role = 'replica'";
            await cmd.ExecuteNonQueryAsync();
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task DisableBulkLoadModeAsync()
    {
        if (_connection == null || _connection.State != System.Data.ConnectionState.Open) return;
        try
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SET session_replication_role = 'DEFAULT'";
            await cmd.ExecuteNonQueryAsync();
        }
        catch { }
    }

    public async Task AnalyzeTableAsync(string schema, string tableName)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(schema);
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = $"ANALYZE \"{mappedSchema}\".\"{tableName}\"";
        cmd.CommandTimeout = 0;
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task ResetSequenceAsync(string schema, string tableName, string identityColumn)
    {
        EnsureConnected();
        var mappedSchema = MapSchema(schema);
        using var command = _connection!.CreateCommand();
        var safeSchema = mappedSchema.Replace("\"", "\"\"");
        var safeTable = tableName.Replace("\"", "\"\"");
        var safeColumn = identityColumn.Replace("\"", "\"\"").Replace("'", "''");

        command.CommandText = $"""
            SELECT setval(
                pg_get_serial_sequence('"{safeSchema}"."{safeTable}"', '{safeColumn}'),
                COALESCE((SELECT MAX("{safeColumn}") FROM "{safeSchema}"."{safeTable}"), 0)
            )
            """;

        try
        {
            await command.ExecuteScalarAsync();
        }
        catch (PostgresException ex)
        {
            throw new InvalidOperationException($"Could not reset sequence for {mappedSchema}.{tableName}.{identityColumn}: {ex.Message}", ex);
        }
    }

    public async Task<string> GetDatabaseNameAsync()
    {
        EnsureConnected();
        using var command = _connection!.CreateCommand();
        command.CommandText = "SELECT current_database()";
        var result = await command.ExecuteScalarAsync();
        return result?.ToString() ?? "Unknown";
    }

    public async Task<List<string>> GetExistingTablesAsync()
    {
        EnsureConnected();
        var tables = new List<string>();
        using var command = _connection!.CreateCommand();
        command.CommandText = """
            SELECT table_schema || '.' || table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
            """;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            tables.Add(reader.GetString(0));
        return tables;
    }

    public async Task<string> GetServerVersionAsync()
    {
        EnsureConnected();
        using var command = _connection!.CreateCommand();
        command.CommandText = "SELECT version()";
        var result = await command.ExecuteScalarAsync();
        var full = result?.ToString() ?? "";
        var idx = full.IndexOf(',');
        return idx > 0 ? full[..idx].Trim() : full;
    }

    private async Task CreateSchemaIfNotExistsAsync(string schema)
    {
        if (schema == "public") return;

        EnsureConnected();

        using var command = _connection!.CreateCommand();
        command.CommandText = $"CREATE SCHEMA IF NOT EXISTS \"{schema}\"";
        await command.ExecuteNonQueryAsync();
    }

    private static string ConvertCheckExpression(string expression)
    {
        var result = expression.Trim();
        while (result.StartsWith('(') && result.EndsWith(')'))
        {
            var inner = result[1..^1];
            int depth = 0;
            bool balanced = true;
            foreach (char c in inner)
            {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                if (depth < 0) { balanced = false; break; }
            }
            if (balanced && depth == 0) result = inner;
            else break;
        }

        result = ReplaceBracketIdentifiers(result);

        result = result
            .Replace("GETDATE()", "CURRENT_TIMESTAMP")
            .Replace("getdate()", "CURRENT_TIMESTAMP");

        return result;
    }

    private string ConvertDefaultValue(string defaultValue, string dataType)
    {
        if (string.IsNullOrEmpty(defaultValue)) return string.Empty;

        var normalized = defaultValue.Trim();

        // Remove wrapping parentheses - handle multiple levels like ((newid()))
        bool changed;
        do
        {
            changed = false;
            if (normalized.StartsWith('(') && normalized.EndsWith(')'))
            {
                var inner = normalized[1..^1];
                int depth = 0;
                bool balanced = true;
                foreach (char c in inner)
                {
                    if (c == '(') depth++;
                    else if (c == ')') depth--;
                    if (depth < 0) { balanced = false; break; }
                }
                if (balanced && depth == 0)
                {
                    normalized = inner.Trim();
                    changed = true;
                }
            }
        } while (changed);

        if (dataType.Equals("bit", StringComparison.OrdinalIgnoreCase))
        {
            return normalized == "1" ? "TRUE" : "FALSE";
        }

        if (normalized.StartsWith("N'") || normalized.StartsWith("'"))
        {
            if (normalized.StartsWith("N'"))
                normalized = normalized[1..];
            return normalized;
        }

        if (double.TryParse(normalized, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out _))
        {
            return normalized;
        }

        // Use ToUpperInvariant to avoid Turkish locale issues (I -> İ problem)
        return normalized.ToUpperInvariant() switch
        {
            "GETDATE()" or "CURRENT_TIMESTAMP" => "CURRENT_TIMESTAMP",
            "NEWID()" => "gen_random_uuid()",
            "SYSUTCDATETIME()" => "CURRENT_TIMESTAMP",
            "GETUTCDATE()" => "CURRENT_TIMESTAMP",
            "SYSDATETIME()" => "CURRENT_TIMESTAMP",
            "SYSDATETIMEOFFSET()" => "CURRENT_TIMESTAMP",
            _ => normalized
        };
    }

    private string ConvertViewDefinition(string sqlServerDefinition)
    {
        var result = sqlServerDefinition
            .Replace("GETDATE()", "CURRENT_TIMESTAMP")
            .Replace("GETUTCDATE()", "CURRENT_TIMESTAMP")
            .Replace("NEWID()", "gen_random_uuid()")
            .Replace("ISNULL(", "COALESCE(")
            .Replace("LEN(", "LENGTH(");

        result = ReplaceBracketIdentifiers(result);
        result = Regex.Replace(result, @"\bTOP\s+(\d+)\b", "/* TOP $1 - add LIMIT $1 at end */", RegexOptions.IgnoreCase);

        return result;
    }

    private static string ReplaceBracketIdentifiers(string sql)
    {
        var sb = new StringBuilder(sql.Length);
        bool inString = false;

        for (int i = 0; i < sql.Length; i++)
        {
            char c = sql[i];

            if (c == '\'' && !inString)
            {
                inString = true;
                sb.Append(c);
            }
            else if (c == '\'' && inString)
            {
                if (i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    sb.Append("''");
                    i++;
                }
                else
                {
                    inString = false;
                    sb.Append(c);
                }
            }
            else if (c == '[' && !inString)
            {
                sb.Append('"');
            }
            else if (c == ']' && !inString)
            {
                sb.Append('"');
            }
            else
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }

    public async Task CreateDatabaseAsync(string databaseName)
    {
        // Connect to postgres database to create new database
        var builder = new NpgsqlConnectionStringBuilder(_connectionString);
        var originalDatabase = builder.Database;
        builder.Database = "postgres";

        using var conn = new NpgsqlConnection(builder.ToString());
        await conn.OpenAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE \"{databaseName}\"";
        try
        {
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"   [OK] Database '{databaseName}' created successfully.");
        }
        catch (PostgresException ex) when (ex.SqlState == "42P04") // database already exists
        {
            Console.WriteLine($"   [INFO] Database '{databaseName}' already exists.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
    }

    public void Dispose()
    {
        DisconnectAsync().GetAwaiter().GetResult();
    }
}
