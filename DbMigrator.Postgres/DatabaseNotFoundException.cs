namespace DbMigrator.Postgres;

public class DatabaseNotFoundException : Exception
{
    public string DatabaseName { get; set; } = string.Empty;

    public DatabaseNotFoundException(string message) : base(message)
    {
    }

    public DatabaseNotFoundException(string databaseName, string message) : base(message)
    {
        DatabaseName = databaseName;
    }
}
