namespace Cirreum.Persistence.Sql.Test;

using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Data;

/// <summary>
/// An <see cref="ISqlConnection"/> implementation that wraps a <see cref="SqliteConnection"/>
/// and uses Dapper for query execution. Used for testing purposes.
/// </summary>
internal sealed class SqliteSqlConnection(
	SqliteConnection connection,
	int commandTimeoutSeconds = 30
) : ISqlConnection {

	public async Task<T?> QuerySingleOrDefaultAsync<T>(
		string sql,
		object? parameters,
		IDbTransaction? transaction,
		CancellationToken cancellationToken) {

		var command = new CommandDefinition(
			sql,
			parameters,
			transaction: transaction,
			commandTimeout: commandTimeoutSeconds,
			cancellationToken: cancellationToken);

		return await connection.QuerySingleOrDefaultAsync<T>(command);
	}

	public async Task<T?> QueryFirstOrDefaultAsync<T>(
		string sql,
		object? parameters,
		IDbTransaction? transaction,
		CancellationToken cancellationToken) {

		var command = new CommandDefinition(
			sql,
			parameters,
			transaction: transaction,
			commandTimeout: commandTimeoutSeconds,
			cancellationToken: cancellationToken);

		return await connection.QueryFirstOrDefaultAsync<T>(command);
	}

	public async Task<IEnumerable<T>> QueryAsync<T>(
		string sql,
		object? parameters,
		IDbTransaction? transaction,
		CancellationToken cancellationToken) {

		var command = new CommandDefinition(
			sql,
			parameters,
			transaction: transaction,
			commandTimeout: commandTimeoutSeconds,
			cancellationToken: cancellationToken);

		return await connection.QueryAsync<T>(command);
	}

	public async Task<T?> ExecuteScalarAsync<T>(
		string sql,
		object? parameters,
		IDbTransaction? transaction,
		CancellationToken cancellationToken) {

		var command = new CommandDefinition(
			sql,
			parameters,
			transaction: transaction,
			commandTimeout: commandTimeoutSeconds,
			cancellationToken: cancellationToken);

		return await connection.ExecuteScalarAsync<T>(command);
	}

	public async Task<int> ExecuteAsync(
		string sql,
		object? parameters,
		IDbTransaction? transaction,
		CancellationToken cancellationToken) {

		var command = new CommandDefinition(
			sql,
			parameters,
			transaction: transaction,
			commandTimeout: commandTimeoutSeconds,
			cancellationToken: cancellationToken);

		return await connection.ExecuteAsync(command);
	}

	public async Task<IMultipleResult> QueryMultipleAsync(
		string sql,
		object? parameters,
		IDbTransaction? transaction,
		CancellationToken cancellationToken) {

		var command = new CommandDefinition(
			sql,
			parameters,
			transaction: transaction,
			commandTimeout: commandTimeoutSeconds,
			cancellationToken: cancellationToken);

		var gridReader = await connection.QueryMultipleAsync(command);
		return new SqliteMultipleResult(gridReader);
	}

	public IDbTransaction BeginTransaction() => connection.BeginTransaction();

	public ValueTask DisposeAsync() => connection.DisposeAsync();
}

/// <summary>
/// Wraps Dapper's GridReader to implement <see cref="IMultipleResult"/>.
/// </summary>
internal sealed class SqliteMultipleResult(SqlMapper.GridReader gridReader) : IMultipleResult {

	public bool IsConsumed => gridReader.IsConsumed;

	public async Task<T?> ReadSingleOrDefaultAsync<T>()
		=> await gridReader.ReadSingleOrDefaultAsync<T>();

	public async Task<T?> ReadFirstOrDefaultAsync<T>()
		=> await gridReader.ReadFirstOrDefaultAsync<T>();

	public async Task<IEnumerable<T>> ReadAsync<T>(bool buffered = true)
		=> await gridReader.ReadAsync<T>(buffered);

	public ValueTask DisposeAsync() {
		gridReader.Dispose();
		return ValueTask.CompletedTask;
	}
}
