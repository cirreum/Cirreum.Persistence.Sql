namespace Cirreum.Persistence.Internal;

/// <summary>
/// Provides database-agnostic helpers for SQL dialect differences.
/// Uses reflection to inspect connection types without requiring references to specific database libraries.
/// </summary>
/// <remarks>
/// Supported databases:
/// <list type="bullet">
/// <item><description>SQL Server (Microsoft.Data.SqlClient) - OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY</description></item>
/// <item><description>SQLite (Microsoft.Data.Sqlite) - LIMIT @PageSize OFFSET @Offset</description></item>
/// <item><description>PostgreSQL (Npgsql) - LIMIT @PageSize OFFSET @Offset</description></item>
/// <item><description>MySQL (MySql.Data / MySqlConnector) - LIMIT @PageSize OFFSET @Offset</description></item>
/// </list>
/// </remarks>
internal static class SqlDialectHelper {

	/// <summary>
	/// Appends the appropriate OFFSET/LIMIT clause for pagination if not already present.
	/// The syntax is determined by inspecting the connection type name.
	/// </summary>
	/// <param name="sql">The SQL query to append pagination to.</param>
	/// <param name="connection">The connection to determine the database provider from.</param>
	/// <returns>The SQL with pagination clause appended, or the original SQL if already present.</returns>
	public static string AppendOffsetIfNeeded(string sql, ISqlConnection connection) {
		// Check if SQL already contains OFFSET or LIMIT clause (case-insensitive)
		if (sql.Contains("OFFSET", StringComparison.OrdinalIgnoreCase) ||
			sql.Contains("LIMIT", StringComparison.OrdinalIgnoreCase)) {
			return sql;
		}

		var offsetClause = GetOffsetClause(connection);
		return sql.TrimEnd() + offsetClause;
	}

	/// <summary>
	/// Gets the appropriate OFFSET clause for the given connection's database provider.
	/// </summary>
	private static string GetOffsetClause(ISqlConnection connection) {
		var typeName = connection.GetType().Name;

		// SQL Server uses OFFSET/FETCH syntax
		if (typeName.Contains("SqlServerConnection", StringComparison.OrdinalIgnoreCase) ||
			typeName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase) ||
			typeName.Contains("SqlConnection", StringComparison.OrdinalIgnoreCase) &&
			!typeName.Contains("Sqlite", StringComparison.OrdinalIgnoreCase)) {
			return " OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY";
		}

		// SQLite, PostgreSQL, MySQL all use LIMIT/OFFSET syntax
		// This is the more common syntax across databases
		return " LIMIT @PageSize OFFSET @Offset";
	}

}
