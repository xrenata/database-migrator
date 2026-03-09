using System.Text;

namespace DbMigrator.CLI;

sealed class MigrationLogger : IAsyncDisposable
{
    private readonly StreamWriter? _writer;
    private readonly StringBuilder _buffer = new();
    private readonly DateTime _startTime = DateTime.UtcNow;

    public MigrationLogger(string? logFilePath)
    {
        if (!string.IsNullOrWhiteSpace(logFilePath))
        {
            var dir = Path.GetDirectoryName(logFilePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);
            _writer = new StreamWriter(logFilePath, append: false, Encoding.UTF8);
            _writer.AutoFlush = true;
        }
    }

    public async Task LogAsync(string level, string message)
    {
        var line = $"[{DateTime.UtcNow:HH:mm:ss}] [{level}] {message}";
        if (_writer != null)
            await _writer.WriteLineAsync(line);
        _buffer.AppendLine(line);
    }

    public Task InfoAsync(string message) => LogAsync("INFO", message);
    public Task WarnAsync(string message) => LogAsync("WARN", message);
    public Task ErrorAsync(string message) => LogAsync("ERROR", message);
    public Task SuccessAsync(string message) => LogAsync("OK", message);

    public async Task WriteTableResultAsync(string tableName, long sourceRows, long targetRows, bool success, string? error = null)
    {
        var status = success ? "OK" : "FAILED";
        var match = sourceRows == targetRows ? "MATCH" : "MISMATCH";
        var line = $"[{DateTime.UtcNow:HH:mm:ss}] [{status}] {tableName}: source={sourceRows:N0} target={targetRows:N0} ({match})";
        if (error != null) line += $" error={error}";
        if (_writer != null) await _writer.WriteLineAsync(line);
        _buffer.AppendLine(line);
    }

    public async Task WriteSummaryAsync(int tablesOk, int tablesFailed, long totalRows, int schemaErrors)
    {
        var elapsed = DateTime.UtcNow - _startTime;
        var lines = new[]
        {
            "",
            "═══════════════════════════════════════",
            "MIGRATION SUMMARY",
            "═══════════════════════════════════════",
            $"Duration:        {elapsed:hh\\:mm\\:ss}",
            $"Tables OK:       {tablesOk}",
            $"Tables Failed:   {tablesFailed}",
            $"Schema Errors:   {schemaErrors}",
            $"Total Rows:      {totalRows:N0}",
            $"Rows/sec:        {(elapsed.TotalSeconds > 0 ? (long)(totalRows / elapsed.TotalSeconds) : 0):N0}",
            "═══════════════════════════════════════",
        };
        foreach (var line in lines)
        {
            if (_writer != null) await _writer.WriteLineAsync(line);
            _buffer.AppendLine(line);
        }
    }

    public string GetFullLog() => _buffer.ToString();

    public async ValueTask DisposeAsync()
    {
        if (_writer != null)
            await _writer.DisposeAsync();
    }
}
