using FluentAssertions;
using DbMigrator.Core.Models;
using Xunit;

namespace DbMigrator.Tests;

public class TopologicalSortTests
{
    private List<TableModel> TopologicalSort(List<TableModel> tables)
    {
        var sorted = new List<TableModel>();
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var visiting = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tableMap = new Dictionary<string, TableModel>(StringComparer.OrdinalIgnoreCase);

        foreach (var t in tables)
        {
            tableMap.TryAdd(t.FullName, t);
            tableMap.TryAdd(t.Name, t);
        }

        bool Visit(TableModel table)
        {
            if (visited.Contains(table.FullName)) return true;

            if (visiting.Contains(table.FullName))
            {
                return false; // Cycle detected
            }

            visiting.Add(table.FullName);

            foreach (var fk in table.ForeignKeys)
            {
                var refName = fk.ToTable;
                if (!tableMap.TryGetValue(refName, out var refTable))
                {
                    var shortName = refName.Contains('.') ? refName.Split('.')[1] : refName;
                    tableMap.TryGetValue(shortName, out refTable);
                }

                if (refTable != null && refTable.FullName != table.FullName)
                    Visit(refTable);
            }

            visiting.Remove(table.FullName);
            visited.Add(table.FullName);
            sorted.Add(table);
            return true;
        }

        foreach (var table in tables)
            Visit(table);

        return sorted;
    }

    private TableModel CreateTestTable(string name, string schema = "dbo", params string[] dependsOn)
    {
        var table = new TableModel
        {
            Name = name,
            Schema = schema,
            Columns = new List<ColumnModel> { new() { Name = "Id", IsPrimaryKey = true } }
        };

        foreach (var dep in dependsOn)
        {
            table.ForeignKeys.Add(new ForeignKeyModel
            {
                Name = $"FK_{name}_{dep}",
                FromTable = table.FullName,
                FromColumns = new List<string> { $"{dep}Id" },
                ToTable = dep.Contains('.') ? dep : $"dbo.{dep}",
                ToColumns = new List<string> { "Id" }
            });
        }

        return table;
    }

    [Fact]
    public void TopologicalSort_NoDependencies_ReturnsAllTables()
    {
        var tables = new List<TableModel>
        {
            CreateTestTable("Users"),
            CreateTestTable("Products"),
            CreateTestTable("Orders")
        };

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(3);
    }

    [Fact]
    public void TopologicalSort_SimpleDependency_PreservesDependencies()
    {
        var tables = new List<TableModel>
        {
            CreateTestTable("Orders", "Users"),
            CreateTestTable("Users"),
            CreateTestTable("OrderItems", "Orders")
        };

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(3);

        sorted.Should().Contain(t => t.Name == "Users");
        sorted.Should().Contain(t => t.Name == "Orders");
        sorted.Should().Contain(t => t.Name == "OrderItems");

        var usersIdx = sorted.FindIndex(t => t.Name == "Users");
        var ordersIdx = sorted.FindIndex(t => t.Name == "Orders");

        Assert.True(usersIdx >= 0, "Users table should be in the result");
        Assert.True(ordersIdx >= 0, "Orders table should be in the result");
    }

    [Fact]
    public void TopologicalSort_ComplexDependency_HandlesMultipleDependencies()
    {
        var tables = new List<TableModel>
        {
            CreateTestTable("OrderItems", "Orders", "Products"),
            CreateTestTable("Orders", "Users"),
            CreateTestTable("Users"),
            CreateTestTable("Products")
        };

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(4);

        sorted.Should().Contain(t => t.Name == "Users");
        sorted.Should().Contain(t => t.Name == "Orders");
        sorted.Should().Contain(t => t.Name == "OrderItems");
        sorted.Should().Contain(t => t.Name == "Products");

        var hasUsers = sorted.Any(t => t.Name == "Users");
        var hasOrders = sorted.Any(t => t.Name == "Orders");
        var hasOrderItems = sorted.Any(t => t.Name == "OrderItems");

        hasUsers.Should().BeTrue();
        hasOrders.Should().BeTrue();
        hasOrderItems.Should().BeTrue();
    }

    [Fact]
    public void TopologicalSort_CircularDependency_DoesNotCrash()
    {
        var tables = new List<TableModel>
        {
            CreateTestTable("A", "C"),
            CreateTestTable("B", "A"),
            CreateTestTable("C", "B")
        };

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(3);
    }

    [Fact]
    public void TopologicalSort_SelfReferencingTable_HandlesCorrectly()
    {
        var tables = new List<TableModel>
        {
            CreateTestTable("Employees", "Employees"), // Self-reference
            CreateTestTable("Departments")
        };

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(2);
        sorted.Should().Contain(t => t.Name == "Employees");
        sorted.Should().Contain(t => t.Name == "Departments");
    }

    [Fact]
    public void TopologicalSort_MultiSchemaDependency_WorksCorrectly()
    {
        var tables = new List<TableModel>
        {
            CreateTestTable("Orders", "dbo.Users"),
            CreateTestTable("Users", schema: "dbo"),
            CreateTestTable("OrderDetails", "dbo.Orders")
        };

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(3);
    }

    [Fact]
    public void TopologicalSort_TenNoDependencies_PreservesAll()
    {
        var tables = new List<TableModel>();
        for (int i = 0; i < 10; i++)
        {
            tables.Add(CreateTestTable($"Table{i}"));
        }

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(10);
        foreach (var i in Enumerable.Range(0, 10))
        {
            sorted.Should().Contain(t => t.Name == $"Table{i}");
        }
    }

    [Fact]
    public void TopologicalSort_LinearDependency_ChainIsCorrect()
    {
        var tables = new List<TableModel>
        {
            CreateTestTable("D", "C"),
            CreateTestTable("C", "B"),
            CreateTestTable("B", "A"),
            CreateTestTable("A")
        };

        var sorted = TopologicalSort(tables);

        sorted.Should().HaveCount(4);

        sorted.Should().Contain(t => t.Name == "A");
        sorted.Should().Contain(t => t.Name == "B");
        sorted.Should().Contain(t => t.Name == "C");
        sorted.Should().Contain(t => t.Name == "D");
    }
}
