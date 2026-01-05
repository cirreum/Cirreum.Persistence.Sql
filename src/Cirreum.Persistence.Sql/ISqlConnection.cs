namespace Cirreum.Persistence;

using System.Data;

/// <summary>
/// Abstraction over a SQL connection that can execute queries and commands.
/// Implementations wrap the underlying ADO.NET connection and query executor.
/// </summary>
public interface ISqlConnection : IAsyncDisposable {

	/// <summary>
	/// Executes a query that returns a single row, or null if no rows are found.
	/// Throws if more than one row is returned.
	/// </summary>
	/// <typeparam name="T">The type to map the result to.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters to pass to the query, or null if none.</param>
	/// <param name="transaction">The transaction to use, or null for no transaction.</param>
	/// <param name="cancellationToken">A token to cancel the operation.</param>
	/// <returns>The single result mapped to <typeparamref name="T"/>, or null if no rows are found.</returns>
	Task<T?> QuerySingleOrDefaultAsync<T>(string sql, object? parameters,
		IDbTransaction? transaction, CancellationToken cancellationToken);

	/// <summary>
	/// Executes a query and returns the first row, or null if no rows are found.
	/// Additional rows are ignored.
	/// </summary>
	/// <typeparam name="T">The type to map the result to.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters to pass to the query, or null if none.</param>
	/// <param name="transaction">The transaction to use, or null for no transaction.</param>
	/// <param name="cancellationToken">A token to cancel the operation.</param>
	/// <returns>The first result mapped to <typeparamref name="T"/>, or null if no rows are found.</returns>
	Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object? parameters,
		IDbTransaction? transaction, CancellationToken cancellationToken);

	/// <summary>
	/// Executes a query and returns all matching rows.
	/// </summary>
	/// <typeparam name="T">The type to map each row to.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters to pass to the query, or null if none.</param>
	/// <param name="transaction">The transaction to use, or null for no transaction.</param>
	/// <param name="cancellationToken">A token to cancel the operation.</param>
	/// <returns>An enumerable of results mapped to <typeparamref name="T"/>.</returns>
	Task<IEnumerable<T>> QueryAsync<T>(string sql, object? parameters,
		IDbTransaction? transaction, CancellationToken cancellationToken);

	/// <summary>
	/// Executes a query and returns the first column of the first row as a scalar value.
	/// </summary>
	/// <typeparam name="T">The type to cast the scalar result to.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters to pass to the query, or null if none.</param>
	/// <param name="transaction">The transaction to use, or null for no transaction.</param>
	/// <param name="cancellationToken">A token to cancel the operation.</param>
	/// <returns>The scalar result cast to <typeparamref name="T"/>, or null if no result.</returns>
	Task<T?> ExecuteScalarAsync<T>(string sql, object? parameters,
		IDbTransaction? transaction, CancellationToken cancellationToken);

	/// <summary>
	/// Executes a non-query command such as INSERT, UPDATE, or DELETE.
	/// </summary>
	/// <param name="sql">The SQL command to execute.</param>
	/// <param name="parameters">The parameters to pass to the command, or null if none.</param>
	/// <param name="transaction">The transaction to use, or null for no transaction.</param>
	/// <param name="cancellationToken">A token to cancel the operation.</param>
	/// <returns>The number of rows affected.</returns>
	Task<int> ExecuteAsync(string sql, object? parameters,
		IDbTransaction? transaction, CancellationToken cancellationToken);

	/// <summary>
	/// Executes a query that returns multiple result sets.
	/// </summary>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters to pass to the query, or null if none.</param>
	/// <param name="transaction">The transaction to use, or null for no transaction.</param>
	/// <param name="cancellationToken">A token to cancel the operation.</param>
	/// <returns>A reader that can iterate through multiple result sets.</returns>
	Task<IMultipleResult> QueryMultipleAsync(string sql, object? parameters,
		IDbTransaction? transaction, CancellationToken cancellationToken);

	/// <summary>
	/// Begins a new database transaction.
	/// </summary>
	/// <returns>The newly created transaction.</returns>
	IDbTransaction BeginTransaction();

}