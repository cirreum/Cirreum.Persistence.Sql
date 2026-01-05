namespace Cirreum.Persistence;

/// <summary>
/// Factory for creating database connections.
/// </summary>
/// <remarks>
/// <para>
/// Implementations should return open, ready-to-use connections from a connection pool.
/// Callers are responsible for disposing the returned connection.
/// </para>
/// <para>
/// <strong>Direct usage:</strong>
/// </para>
/// <code>
/// await using var conn = await factory.CreateConnectionAsync(cancellationToken);
/// return await conn.GetAsync&lt;Order&gt;(sql, parameters, key, cancellationToken: cancellationToken);
/// </code>
/// <para>
/// <strong>Using extension methods:</strong>
/// </para>
/// <code>
/// // Single operation (connection lifecycle managed automatically)
/// return await factory.GetAsync&lt;Order&gt;(
///		orderSql, 
///		query, 
///		query.OrderId, 
///		cancellationToken);
/// 
/// // Multi-read operations (connection lifecycle managed automatically)
/// return await factory.ExecuteAsync(ctx => ctx
///     .GetAsync&lt;Customer&gt;(customerSql, parameters, key)
///     .AndGetAsync&lt;Order&gt;(orderSql, parameters, key)
///     .Map&lt;Dashboard&gt;((c, o) => new(c, o))
/// , cancellationToken);
///
/// // Transaction (connection and transaction lifecycle managed automatically)
/// return await factory.ExecuteTransactionAsync(ctx => ctx
///     .InsertAsync(orderSql, orderParam)
///     .ThenInsertAsync(lineItemSql, lineItemParam)
/// , cancellationToken);
/// </code>
/// </remarks>
public interface ISqlConnectionFactory {

	/// <summary>
	/// Creates and opens a new database connection.
	/// </summary>
	/// <param name="cancellationToken">Cancellation token.</param>
	/// <returns>An open database connection.</returns>
	Task<ISqlConnection> CreateConnectionAsync(CancellationToken cancellationToken = default);

	/// <summary>
	/// The command timeout in seconds.
	/// </summary>
	int CommandTimeoutSeconds { get; }

}