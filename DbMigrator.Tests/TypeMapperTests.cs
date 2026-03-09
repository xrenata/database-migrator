using FluentAssertions;
using DbMigrator.Core.TypeMappings;
using Xunit;

namespace DbMigrator.Tests;

public class TypeMapperTests
{
    private readonly MsSqlToPostgresTypeMapper _mapper = new();

    [Fact]
    public void ConvertType_IntegerTypes_MapsCorrectly()
    {
        _mapper.ConvertType("tinyint").Should().Be("SMALLINT");
        _mapper.ConvertType("smallint").Should().Be("SMALLINT");
        _mapper.ConvertType("int").Should().Be("INTEGER");
        _mapper.ConvertType("bigint").Should().Be("BIGINT");
    }

    [Fact]
    public void ConvertType_StringTypes_MapsCorrectly()
    {
        _mapper.ConvertType("varchar", 50).Should().Be("VARCHAR(50)");
        _mapper.ConvertType("varchar", -1).Should().Be("TEXT");
        _mapper.ConvertType("nvarchar", 100).Should().Be("VARCHAR(100)");
        _mapper.ConvertType("nvarchar", -1).Should().Be("TEXT");
        _mapper.ConvertType("char", 10).Should().Be("CHAR(10)");
        _mapper.ConvertType("text").Should().Be("TEXT");
        _mapper.ConvertType("ntext").Should().Be("TEXT");
    }

    [Fact]
    public void ConvertType_DateTimeTypes_MapsCorrectly()
    {
        _mapper.ConvertType("date").Should().Be("DATE");
        _mapper.ConvertType("datetime").Should().Be("TIMESTAMP");
        _mapper.ConvertType("datetime2").Should().Be("TIMESTAMP");
        _mapper.ConvertType("datetime2", null, 3).Should().Be("TIMESTAMP(3)");
        _mapper.ConvertType("datetimeoffset").Should().Be("TIMESTAMPTZ");
        _mapper.ConvertType("time").Should().Be("TIME");
    }

    [Fact]
    public void ConvertType_DecimalTypes_MapsCorrectly()
    {
        _mapper.ConvertType("decimal", null, 18, 2).Should().Be("DECIMAL(18,2)");
        _mapper.ConvertType("numeric", null, 10, 0).Should().Be("DECIMAL(10,0)");
        _mapper.ConvertType("decimal").Should().Be("DECIMAL");
    }

    [Fact]
    public void ConvertType_BinaryTypes_MapsCorrectly()
    {
        _mapper.ConvertType("binary").Should().Be("BYTEA");
        _mapper.ConvertType("varbinary").Should().Be("BYTEA");
        _mapper.ConvertType("image").Should().Be("BYTEA");
        _mapper.ConvertType("timestamp").Should().Be("BYTEA");
        _mapper.ConvertType("rowversion").Should().Be("BYTEA");
    }

    [Fact]
    public void ConvertType_OtherTypes_MapsCorrectly()
    {
        _mapper.ConvertType("bit").Should().Be("BOOLEAN");
        _mapper.ConvertType("uniqueidentifier").Should().Be("UUID");
        _mapper.ConvertType("xml").Should().Be("XML");
        _mapper.ConvertType("json").Should().Be("JSONB");
        _mapper.ConvertType("money").Should().Be("NUMERIC(19,4)");
        _mapper.ConvertType("smallmoney").Should().Be("NUMERIC(10,4)");
    }

    [Fact]
    public void ConvertType_UnknownType_ReturnsText()
    {
        _mapper.ConvertType("unknown_type").Should().Be("TEXT");
    }

    [Fact]
    public void ConvertSequenceType_IntegerTypes_MapsCorrectly()
    {
        MsSqlToPostgresTypeMapper.ConvertSequenceType("tinyint").Should().Be("SMALLINT");
        MsSqlToPostgresTypeMapper.ConvertSequenceType("smallint").Should().Be("SMALLINT");
        MsSqlToPostgresTypeMapper.ConvertSequenceType("int").Should().Be("INTEGER");
        MsSqlToPostgresTypeMapper.ConvertSequenceType("bigint").Should().Be("BIGINT");
        MsSqlToPostgresTypeMapper.ConvertSequenceType("decimal").Should().Be("BIGINT");
        MsSqlToPostgresTypeMapper.ConvertSequenceType("numeric").Should().Be("BIGINT");
    }

    [Fact]
    public void ConvertSequenceType_UnknownType_ReturnsBigInt()
    {
        MsSqlToPostgresTypeMapper.ConvertSequenceType("varchar").Should().Be("BIGINT");
        MsSqlToPostgresTypeMapper.ConvertSequenceType("unknown").Should().Be("BIGINT");
    }
}
