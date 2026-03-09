using FluentAssertions;
using System.Text.RegularExpressions;
using Xunit;

namespace DbMigrator.Tests;

public class WildcardMatcherTests
{
    private bool WildcardMatch(string text, string pattern)
    {
        var regexPattern = "^" + Regex.Escape(pattern)
            .Replace("\\*", ".*")
            .Replace("\\?", ".") + "$";
        return Regex.IsMatch(text, regexPattern, RegexOptions.IgnoreCase);
    }

    [Fact]
    public void WildcardMatch_ExactMatch_ReturnsTrue()
    {
        WildcardMatch("Users", "Users").Should().BeTrue();
        WildcardMatch("dbo.Users", "dbo.Users").Should().BeTrue();
    }

    [Fact]
    public void WildcardMatch_AsteriskWildcard_MatchesAny()
    {
        WildcardMatch("Users", "U*").Should().BeTrue();
        WildcardMatch("Users", "*s").Should().BeTrue();
        WildcardMatch("Users", "*").Should().BeTrue();
        WildcardMatch("dbo.Users", "dbo.*").Should().BeTrue();
        WildcardMatch("dbo.Users", "*.Users").Should().BeTrue();
    }

    [Fact]
    public void WildcardMatch_QuestionMarkWildcard_MatchesSingleChar()
    {
        WildcardMatch("User", "U??r").Should().BeTrue();  // "User" matches "U??r" (U+s+e+r)
        WildcardMatch("User", "U?er").Should().BeTrue();
        WildcardMatch("User", "?ser").Should().BeTrue();
        WildcardMatch("Users", "U?s").Should().BeFalse(); // "Users" has 5 chars, "U?s" has 3
        WildcardMatch("Ur", "U??r").Should().BeFalse();   // "Ur" has 2 chars, "U??r" needs 4
    }

    [Fact]
    public void WildcardCaseInsensitive_MatchesRegardlessOfCase()
    {
        WildcardMatch("Users", "users").Should().BeTrue();
        WildcardMatch("Users", "USERS").Should().BeTrue();
        WildcardMatch("Users", "UsErS").Should().BeTrue();
        WildcardMatch("dbo.Users", "dbo.users").Should().BeTrue();
        WildcardMatch("dbo.Users", "DBO.USERS").Should().BeTrue();
    }

    [Fact]
    public void WildcardMatch_CommonPatterns_MatchesCorrectly()
    {
        WildcardMatch("sys_tables", "sys_*").Should().BeTrue();
        WildcardMatch("sys_columns", "sys_*").Should().BeTrue();
        WildcardMatch("sys_tables", "sys_*").Should().BeTrue();
        WildcardMatch("my_sys_table", "sys_*").Should().BeFalse();

        WildcardMatch("AspNetUsers", "AspNet*").Should().BeTrue();
        WildcardMatch("AspNetRoles", "AspNet*").Should().BeTrue();
        WildcardMatch("MyAspNetTable", "AspNet*").Should().BeFalse();

        WildcardMatch("test_table_123", "test_*").Should().BeTrue();
        WildcardMatch("prod_table_456", "prod_*").Should().BeTrue();
        WildcardMatch("test_table_123", "prod_*").Should().BeFalse();
    }

    [Fact]
    public void WildcardMatch_DottedNames_MatchesCorrectly()
    {
        WildcardMatch("dbo.Users", "dbo.Users").Should().BeTrue();
        WildcardMatch("dbo.Users", "dbo.*").Should().BeTrue();
        WildcardMatch("dbo.Users", "*.Users").Should().BeTrue();
        WildcardMatch("schema.TableName", "schema.*").Should().BeTrue();
        WildcardMatch("schema.TableName", "*.TableName").Should().BeTrue();
        WildcardMatch("schema.TableName", "*.*").Should().BeTrue();
    }

    [Fact]
    public void WildcardMatch_NoMatch_ReturnsFalse()
    {
        WildcardMatch("Users", "Orders").Should().BeFalse();
        WildcardMatch("Users", "U*erss").Should().BeFalse();
        WildcardMatch("dbo.Users", "schema.Users").Should().BeFalse();
        WildcardMatch("AspNetUsers", "Identity*").Should().BeFalse();
    }
}
