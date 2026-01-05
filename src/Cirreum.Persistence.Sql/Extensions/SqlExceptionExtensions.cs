namespace Cirreum.Persistence;

using Cirreum;
using Cirreum.Exceptions;
using System.Reflection;

/// <summary>
/// Database-agnostic extension methods for detecting constraint violations.
/// Uses reflection to inspect exception properties without requiring references to specific database libraries.
/// </summary>
/// <remarks>
/// Supported databases:
/// <list type="bullet">
/// <item><description>SQL Server (Microsoft.Data.SqlClient) - Error numbers 2627, 2601 (unique), 547 (FK)</description></item>
/// <item><description>SQLite (Microsoft.Data.Sqlite) - Extended error codes 1555, 2067 (unique), 787 (FK)</description></item>
/// <item><description>PostgreSQL (Npgsql) - SQL states 23505 (unique), 23503 (FK)</description></item>
/// <item><description>MySQL (MySql.Data / MySqlConnector) - Error numbers 1062 (unique), 1452 (FK)</description></item>
/// </list>
/// </remarks>
public static class SqlExceptionExtensions {

	#region Unique Constraint Detection

	/// <summary>
	/// Determines whether the exception represents a unique constraint or unique index violation.
	/// </summary>
	/// <param name="ex">The exception to inspect.</param>
	/// <returns>True if the exception indicates a duplicate key violation; otherwise, false.</returns>
	public static bool IsUniqueConstraintViolation(this Exception ex) {
		var typeName = ex.GetType().Name;

		return typeName switch {
			// SQL Server: error numbers 2627 (PK violation), 2601 (unique index violation)
			"SqlException" => GetInt32Property(ex, "Number") is 2627 or 2601,

			// SQLite: extended error codes 1555 (SQLITE_CONSTRAINT_PRIMARYKEY), 2067 (SQLITE_CONSTRAINT_UNIQUE)
			"SqliteException" => GetInt32Property(ex, "SqliteExtendedErrorCode") is 1555 or 2067,

			// PostgreSQL: SQL state 23505 (unique_violation)
			"PostgresException" => GetStringProperty(ex, "SqlState") == "23505",

			// MySQL: error number 1062 (ER_DUP_ENTRY)
			"MySqlException" => GetInt32Property(ex, "Number") == 1062,

			_ => false
		};
	}

	/// <summary>
	/// Determines whether the exception represents a foreign key constraint violation.
	/// </summary>
	/// <param name="ex">The exception to inspect.</param>
	/// <returns>True if the exception indicates a foreign key violation; otherwise, false.</returns>
	public static bool IsForeignKeyViolation(this Exception ex) {
		var typeName = ex.GetType().Name;

		return typeName switch {
			// SQL Server: error number 547 (FK violation)
			"SqlException" => GetInt32Property(ex, "Number") == 547,

			// SQLite: extended error code 787 (SQLITE_CONSTRAINT_FOREIGNKEY)
			"SqliteException" => GetInt32Property(ex, "SqliteExtendedErrorCode") == 787,

			// PostgreSQL: SQL state 23503 (foreign_key_violation)
			"PostgresException" => GetStringProperty(ex, "SqlState") == "23503",

			// MySQL: error number 1452 (ER_NO_REFERENCED_ROW_2)
			"MySqlException" => GetInt32Property(ex, "Number") == 1452,

			_ => false
		};
	}

	/// <summary>
	/// Determines whether the exception represents any constraint violation (unique, FK, etc).
	/// </summary>
	/// <param name="ex">The exception to inspect.</param>
	/// <returns>True if the exception indicates any constraint violation; otherwise, false.</returns>
	public static bool IsConstraintViolation(this Exception ex) =>
		ex.IsUniqueConstraintViolation() || ex.IsForeignKeyViolation();

	#endregion

	#region Result Conversion

	/// <summary>
	/// Attempts to convert a database exception to an appropriate <see cref="Result"/> based on the error type.
	/// </summary>
	/// <param name="ex">The exception to convert.</param>
	/// <param name="uniqueConstraintMessage">The message to use for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">The message to use for foreign key violations, or null to not handle FK violations.</param>
	/// <param name="result">The resulting <see cref="Result"/> if a known constraint violation was detected.</param>
	/// <returns>True if the exception was a recognized constraint violation and was converted; otherwise, false.</returns>
	public static bool TryToResult(
		this Exception ex,
		string uniqueConstraintMessage,
		string? foreignKeyMessage,
		out Result result) {

		if (ex.IsUniqueConstraintViolation()) {
			result = Result.Fail(new AlreadyExistsException(uniqueConstraintMessage));
			return true;
		}

		if (foreignKeyMessage is not null && ex.IsForeignKeyViolation()) {
			result = Result.Fail(new BadRequestException(foreignKeyMessage));
			return true;
		}

		result = default;
		return false;
	}

	/// <summary>
	/// Attempts to convert a database exception to an appropriate <see cref="Result{T}"/> based on the error type.
	/// </summary>
	/// <typeparam name="T">The type of the value that would have been returned on success.</typeparam>
	/// <param name="ex">The exception to convert.</param>
	/// <param name="uniqueConstraintMessage">The message to use for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">The message to use for foreign key violations, or null to not handle FK violations.</param>
	/// <param name="result">The resulting <see cref="Result{T}"/> if a known constraint violation was detected.</param>
	/// <returns>True if the exception was a recognized constraint violation and was converted; otherwise, false.</returns>
	public static bool TryToResult<T>(
		this Exception ex,
		string uniqueConstraintMessage,
		string? foreignKeyMessage,
		out Result<T> result) {

		if (ex.IsUniqueConstraintViolation()) {
			result = Result.AlreadyExist<T>(uniqueConstraintMessage);
			return true;
		}

		if (foreignKeyMessage is not null && ex.IsForeignKeyViolation()) {
			result = Result.BadRequest<T>(foreignKeyMessage);
			return true;
		}

		result = default;
		return false;
	}

	/// <summary>
	/// Attempts to convert a foreign key violation to a <see cref="ConflictException"/> result.
	/// Used for DELETE operations where FK violations mean the record is still in use.
	/// </summary>
	/// <param name="ex">The exception to convert.</param>
	/// <param name="foreignKeyMessage">The message to use for foreign key violations.</param>
	/// <param name="result">The resulting <see cref="Result"/> if a foreign key violation was detected.</param>
	/// <returns>True if the exception was a foreign key violation and was converted; otherwise, false.</returns>
	public static bool TryToDeleteResult(
		this Exception ex,
		string foreignKeyMessage,
		out Result result) {

		if (ex.IsForeignKeyViolation()) {
			result = Result.Fail(new ConflictException(foreignKeyMessage));
			return true;
		}

		result = default;
		return false;
	}

	/// <summary>
	/// Converts a database exception to an appropriate <see cref="Result"/> based on the error type.
	/// Uses the exception's message for constraint violations.
	/// </summary>
	/// <param name="ex">The exception to convert.</param>
	/// <returns>
	/// A failed <see cref="Result"/> with an appropriate exception type:
	/// <see cref="AlreadyExistsException"/> for unique constraint violations,
	/// <see cref="ConflictException"/> for foreign key violations,
	/// or the original exception for all other cases.
	/// </returns>
	public static Result ToResult(this Exception ex) {
		if (ex.IsUniqueConstraintViolation()) {
			return Result.Fail(new AlreadyExistsException(ex.Message));
		}
		if (ex.IsForeignKeyViolation()) {
			return Result.Fail(new ConflictException(ex.Message));
		}
		return Result.Fail(ex);
	}

	/// <summary>
	/// Converts a database exception to an appropriate <see cref="Result{T}"/> based on the error type.
	/// Uses the exception's message for constraint violations.
	/// </summary>
	/// <typeparam name="T">The type of the value that would have been returned on success.</typeparam>
	/// <param name="ex">The exception to convert.</param>
	/// <returns>
	/// A failed <see cref="Result{T}"/> with an appropriate exception type:
	/// <see cref="AlreadyExistsException"/> for unique constraint violations,
	/// <see cref="BadRequestException"/> for foreign key violations,
	/// or the original exception for all other cases.
	/// </returns>
	public static Result<T> ToResult<T>(this Exception ex) {
		if (ex.IsUniqueConstraintViolation()) {
			return Result.AlreadyExist<T>(ex.Message);
		}
		if (ex.IsForeignKeyViolation()) {
			return Result.BadRequest<T>(ex.Message);
		}
		return Result.Fail<T>(ex);
	}

	#endregion

	#region Reflection Helpers

	private static int? GetInt32Property(Exception ex, string propertyName) {
		var property = ex.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
		return property?.GetValue(ex) as int?;
	}

	private static string? GetStringProperty(Exception ex, string propertyName) {
		var property = ex.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
		return property?.GetValue(ex) as string;
	}

	#endregion

}
