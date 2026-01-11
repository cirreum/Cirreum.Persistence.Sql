namespace Cirreum.Persistence;

using System.Data;

/// <summary>
/// Provides a fluent interface for executing database operations within a given context,
/// automatically flowing the current connection, optional transaction, and cancellation
/// token to each operation.
/// </summary>
/// <remarks>
/// <para>
/// Use this context within <see cref="SqlConnectionFactoryExtensions.ExecuteAsync"/> or
/// <see cref="SqlConnectionFactoryExtensions.ExecuteAsync{T}"/> or 
/// <see cref="SqlConnectionExtensions.ExecuteAsync"/> or 
/// <see cref="SqlConnectionExtensions.ExecuteAsync{T}"/> or 
/// to chain multiple database conection operations.
/// </para>
/// <para>
/// <strong>Usage with ISqlConnection:</strong>
/// </para>
/// <code>
/// await using var conn = await factory.CreateConnectionAsync(cancellationToken);
/// return await conn.Execute{Transaction}Async(ctx => ctx
///     .InsertAsync(orderSql, orderParam)
///     .ThenInsertAsync(lineItemSql, lineItemParam)
///     .ThenUpdateAsync(inventorySql, inventoryParam, inventoryId)
/// , cancellationToken);
/// </code>
/// <para>
/// <strong>Usage with ISqlConnectionFactory:</strong>
/// </para>
/// <code>
/// return await factory.Execute{Transaction}Async(ctx => ctx
///     .InsertAsync(orderSql, orderParam)
///     .ThenInsertAsync(lineItemSql, lineItemParam)
///     .ThenUpdateAsync(inventorySql, inventoryParam, inventoryId)
/// , cancellationToken);
/// </code>
/// </remarks>
public readonly struct DbContext(
	ISqlConnection connection,
	IDbTransaction? transaction,
	CancellationToken cancellationToken) {

	#region Insert

	/// <summary>
	/// Executes an INSERT command and returns a chainable result.
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult InsertAsync(
		string sql,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAsync(sql, null, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an INSERT command and returns a chainable result.
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult InsertAsync(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Insert-If

	/// <summary>
	/// Conditionally executes an INSERT command and returns a chainable result.
	/// If <paramref name="when"/> is false, the insert is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="when">A boolean that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult InsertIfAsync(
		string sql,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? new(this, connection.InsertAsync(sql, null, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult(Result.Success));

	/// <summary>
	/// Conditionally executes an INSERT command and returns a chainable result.
	/// If <paramref name="when"/> returns false, the insert is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="when">A predicate that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult InsertIfAsync(
		string sql,
		Func<bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, this.InsertIfAsyncCore(sql, () => null, when, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Conditionally executes an INSERT command and returns a chainable result.
	/// If <paramref name="when"/> is false, the insert is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the INSERT statement.</param>
	/// <param name="when">A boolean that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult InsertIfAsync(
		string sql,
		Func<object?> parametersFactory,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? new(this, connection.InsertAsync(sql, parametersFactory(), uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult(Result.Success));

	/// <summary>
	/// Conditionally executes an INSERT command and returns a chainable result.
	/// If <paramref name="when"/> returns false, the insert is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the INSERT statement.</param>
	/// <param name="when">A predicate that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult InsertIfAsync(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, this.InsertIfAsyncCore(sql, parametersFactory, when, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result> InsertIfAsyncCore(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		if (!when()) {
			return Result.Success;
		}
		return await connection.InsertAsync(sql, parametersFactory(), uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken).ConfigureAwait(false);
	}

	#endregion

	#region Insert and Return

	/// <summary>
	/// Executes an INSERT command and returns the specified value if at least one row was affected.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="resultSelector">A function that returns the value to include in the successful result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertAndReturnAsync<T>(
		string sql,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAndReturnAsync(sql, null, resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an INSERT command and returns the specified value if at least one row was affected.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="resultSelector">A function that returns the value to include in the successful result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertAndReturnAsync<T>(
		string sql,
		object? parameters,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAndReturnAsync(sql, parameters, resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Insert and Return (entity shortcut)

	/// <summary>
	/// Executes an INSERT command using the entity as both parameters and return value.
	/// </summary>
	/// <typeparam name="TEntity">The type of the entity to insert and return.</typeparam>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="entity">The entity used for parameters and returned on success.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing the entity on success.</returns>
	public DbResult<TEntity> InsertAndReturnAsync<TEntity>(
		string sql,
		TEntity entity,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist") where TEntity : class
		=> this.InsertAndReturnAsync(sql, entity, () => entity, uniqueConstraintMessage, foreignKeyMessage);

	#endregion

	#region Insert-If and Return

	/// <summary>
	/// Conditionally executes an INSERT command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> is false, the insert is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="when">A boolean that determines whether to execute the insert.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertIfAndReturnAsync<T>(
		string sql,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? new(this, connection.InsertAndReturnAsync(sql, null, resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult<Result<T>>(resultSelector()));

	/// <summary>
	/// Conditionally executes an INSERT command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> returns false, the insert is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="when">A predicate that determines whether to execute the insert.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertIfAndReturnAsync<T>(
		string sql,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, this.InsertIfAndReturnAsyncCore(sql, () => null, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Conditionally executes an INSERT command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> is false, the insert is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the INSERT statement.</param>
	/// <param name="when">A boolean that determines whether to execute the insert.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? new(this, connection.InsertAndReturnAsync(sql, parametersFactory(), resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult<Result<T>>(resultSelector()));

	/// <summary>
	/// Conditionally executes an INSERT command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> returns false, the insert is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the INSERT statement.</param>
	/// <param name="when">A predicate that determines whether to execute the insert.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, this.InsertIfAndReturnAsyncCore(sql, parametersFactory, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<T>> InsertIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		if (!when()) {
			return resultSelector();
		}
		return await connection.InsertAndReturnAsync(sql, parametersFactory(), resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken).ConfigureAwait(false);
	}

	#endregion

	#region Update

	/// <summary>
	/// Executes an UPDATE command and returns a chainable result if at least one row was affected.
	/// </summary>
	/// <param name="sql">The SQL UPDATE statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being updated.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult UpdateAsync(
		string sql,
		object? parameters,
		object key,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.UpdateAsync(sql, parameters, key, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Update and Return

	/// <summary>
	/// Executes an UPDATE command and returns the specified value if at least one row was affected.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL UPDATE statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being updated.</param>
	/// <param name="resultSelector">A function that returns the value to include in the successful result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> UpdateAndReturnAsync<T>(
		string sql,
		object? parameters,
		object key,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.UpdateAndReturnAsync(sql, parameters, key, resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Update-If

	/// <summary>
	/// Conditionally executes an UPDATE command and returns a chainable result.
	/// If <paramref name="when"/> is false, the update is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL UPDATE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the UPDATE statement.</param>
	/// <param name="key">The key of the entity being updated.</param>
	/// <param name="when">A boolean that determines whether to execute the update.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult UpdateIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? new(this, connection.UpdateAsync(sql, parametersFactory(), key, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult(Result.Success));

	/// <summary>
	/// Conditionally executes an UPDATE command and returns a chainable result.
	/// If <paramref name="when"/> returns false, the update is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL UPDATE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the UPDATE statement.</param>
	/// <param name="key">The key of the entity being updated.</param>
	/// <param name="when">A predicate that determines whether to execute the update.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult UpdateIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, this.UpdateIfAsyncCore(sql, parametersFactory, key, when, uniqueConstraintMessage, foreignKeyMessage));

	#endregion

	#region Update-If and Return

	/// <summary>
	/// Conditionally executes an UPDATE command and returns the specified value on success.
	/// If <paramref name="when"/> is false, the update is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL UPDATE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the UPDATE statement.</param>
	/// <param name="key">The key of the entity being updated.</param>
	/// <param name="when">A boolean that determines whether to execute the update.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> UpdateIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? new(this, connection.UpdateAndReturnAsync(sql, parametersFactory(), key, resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult<Result<T>>(resultSelector()));

	/// <summary>
	/// Conditionally executes an UPDATE command and returns the specified value on success.
	/// If <paramref name="when"/> returns false, the update is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL UPDATE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the UPDATE statement.</param>
	/// <param name="key">The key of the entity being updated.</param>
	/// <param name="when">A predicate that determines whether to execute the update.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> UpdateIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, this.UpdateIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	#endregion

	#region UpdateCore

	private async Task<Result> UpdateIfAsyncCore(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		if (!when()) {
			return Result.Success;
		}
		return await connection.UpdateAsync(sql, parametersFactory(), key, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken).ConfigureAwait(false);
	}

	private async Task<Result<T>> UpdateIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		if (!when()) {
			return resultSelector();
		}
		return await connection.UpdateAndReturnAsync(sql, parametersFactory(), key, resultSelector, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken).ConfigureAwait(false);
	}

	#endregion

	#region Delete

	/// <summary>
	/// Executes a DELETE command and returns a chainable result if at least one row was affected.
	/// </summary>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult DeleteAsync(
		string sql,
		object? parameters,
		object key,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, connection.DeleteAsync(sql, parameters, key, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Delete and Return

	/// <summary>
	/// Executes a DELETE command and returns the specified value if at least one row was affected.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="resultSelector">A function that returns the value to include in the successful result.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> DeleteAndReturnAsync<T>(
		string sql,
		object? parameters,
		object key,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, connection.DeleteAndReturnAsync(sql, parameters, key, resultSelector, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Delete-If

	/// <summary>
	/// Conditionally executes a DELETE command and returns a chainable result.
	/// If <paramref name="when"/> is false, the delete is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="when">A boolean that determines whether to execute the delete.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult DeleteIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> when
			? new(this, connection.DeleteAsync(sql, parametersFactory(), key, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult(Result.Success));

	/// <summary>
	/// Conditionally executes a DELETE command and returns a chainable result.
	/// If <paramref name="when"/> returns false, the delete is skipped and a successful result is returned.
	/// </summary>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="when">A predicate that determines whether to execute the delete.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult"/> that can be chained with other database operations.</returns>
	public DbResult DeleteIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, this.DeleteIfAsyncCore(sql, parametersFactory, key, when, foreignKeyMessage));

	private async Task<Result> DeleteIfAsyncCore(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string foreignKeyMessage) {
		if (!when()) {
			return Result.Success;
		}
		return await connection.DeleteAsync(sql, parametersFactory(), key, foreignKeyMessage, transaction, cancellationToken).ConfigureAwait(false);
	}

	#endregion

	#region Delete-If And Return

	/// <summary>
	/// Conditionally executes a DELETE command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> is false, the delete is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="when">A boolean that determines whether to execute the delete.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> DeleteIfAndReturnAsync<T>(
		string sql,
		object key,
		bool when,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> when
			? new(this, connection.DeleteAndReturnAsync(sql, null, key, resultSelector, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult<Result<T>>(resultSelector()));

	/// <summary>
	/// Conditionally executes a DELETE command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> returns false, the delete is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="when">A predicate that determines whether to execute the delete.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> DeleteIfAndReturnAsync<T>(
		string sql,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, this.DeleteIfAndReturnAsyncCore(sql, () => null, key, when, resultSelector, foreignKeyMessage));

	/// <summary>
	/// Conditionally executes a DELETE command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> is false, the delete is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="when">A boolean that determines whether to execute the delete.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> DeleteIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> when
			? new(this, connection.DeleteAndReturnAsync(sql, parametersFactory(), key, resultSelector, foreignKeyMessage, transaction, cancellationToken))
			: new(this, Task.FromResult<Result<T>>(resultSelector()));

	/// <summary>
	/// Conditionally executes a DELETE command and returns the specified value if at least one row was affected.
	/// If <paramref name="when"/> returns false, the delete is skipped and the result from <paramref name="resultSelector"/> is returned.
	/// </summary>
	/// <typeparam name="T">The type of the value to return on success.</typeparam>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted.</param>
	/// <param name="when">A predicate that determines whether to execute the delete.</param>
	/// <param name="resultSelector">A function that returns the value to include in the result.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> DeleteIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, this.DeleteIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, foreignKeyMessage));

	private async Task<Result<T>> DeleteIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string foreignKeyMessage) {
		if (!when()) {
			return resultSelector();
		}
		return await connection.DeleteAndReturnAsync(sql, parametersFactory(), key, resultSelector, foreignKeyMessage, transaction, cancellationToken).ConfigureAwait(false);
	}

	#endregion

	#region Insert and Get

	/// <summary>
	/// Executes an INSERT command followed by a SELECT in a single batch and returns the selected row.
	/// </summary>
	/// <typeparam name="T">The type of the row to return.</typeparam>
	/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertAndGetAsync<T>(
		string sql,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAndGetAsync<T>(sql, null, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an INSERT command followed by a SELECT in a single batch and returns the selected row.
	/// </summary>
	/// <typeparam name="T">The type of the row to return.</typeparam>
	/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> InsertAndGetAsync<T>(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAndGetAsync<T>(sql, parameters, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an INSERT command followed by a SELECT in a single batch and returns the selected row,
	/// applying a mapping function to transform the result.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="mapper">A function to transform the data row to the domain model.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> InsertAndGetAsync<TData, TModel>(
		string sql,
		object? parameters,
		Func<TData, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAndGetAsync(sql, parameters, mapper, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an INSERT command followed by a SELECT in a single batch and returns an <see cref="Optional{T}"/>
	/// containing the selected row if present, allowing the caller to handle the empty case via a mapper.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> InsertAndGetOptionalAsync<TData, TModel>(
		string sql,
		object? parameters,
		Func<Optional<TData>, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertAndGetOptionalAsync(sql, parameters, mapper, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Update and Get

	/// <summary>
	/// Executes an UPDATE command followed by a SELECT in a single batch and returns the selected row.
	/// </summary>
	/// <typeparam name="T">The type of the row to return.</typeparam>
	/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being updated, used in the NotFoundException if no row is returned.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> UpdateAndGetAsync<T>(
		string sql,
		object? parameters,
		object key,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.UpdateAndGetAsync<T>(sql, parameters, key, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an UPDATE command followed by a SELECT in a single batch and returns the selected row,
	/// applying a mapping function to transform the result.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being updated, used in the NotFoundException if no row is returned.</param>
	/// <param name="mapper">A function to transform the data row to the domain model.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> UpdateAndGetAsync<TData, TModel>(
		string sql,
		object? parameters,
		object key,
		Func<TData, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.UpdateAndGetAsync(sql, parameters, key, mapper, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an UPDATE command followed by a SELECT in a single batch and returns an <see cref="Optional{T}"/>
	/// containing the selected row if present, allowing the caller to handle the empty case via a mapper.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> UpdateAndGetOptionalAsync<TData, TModel>(
		string sql,
		object? parameters,
		Func<Optional<TData>, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.UpdateAndGetOptionalAsync(sql, parameters, mapper, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Delete and Get

	/// <summary>
	/// Executes a DELETE command and returns the deleted row using an OUTPUT clause or similar mechanism.
	/// </summary>
	/// <typeparam name="T">The type of the row to return.</typeparam>
	/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being deleted, used in the NotFoundException if no row is returned.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> DeleteAndGetAsync<T>(
		string sql,
		object? parameters,
		object key,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, connection.DeleteAndGetAsync<T>(sql, parameters, key, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes a DELETE command and returns the deleted row using an OUTPUT clause or similar mechanism,
	/// applying a mapping function to transform the result.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="key">The key of the entity being deleted, used in the NotFoundException if no row is returned.</param>
	/// <param name="mapper">A function to transform the data row to the domain model.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> DeleteAndGetAsync<TData, TModel>(
		string sql,
		object? parameters,
		object key,
		Func<TData, TModel> mapper,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, connection.DeleteAndGetAsync(sql, parameters, key, mapper, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes a DELETE command and returns an <see cref="Optional{T}"/> containing the deleted row if present,
	/// allowing the caller to handle the empty case via a mapper.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> DeleteAndGetOptionalAsync<TData, TModel>(
		string sql,
		object? parameters,
		Func<Optional<TData>, TModel> mapper,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, connection.DeleteAndGetOptionalAsync(sql, parameters, mapper, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region InsertWithCount

	/// <summary>
	/// Executes an INSERT command and returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., conditional inserts).
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing the row count that can be chained with other database operations.</returns>
	public DbResult<int> InsertWithCountAsync(
		string sql,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertWithCountAsync(sql, null, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	/// <summary>
	/// Executes an INSERT command and returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., conditional inserts).
	/// </summary>
	/// <param name="sql">The SQL INSERT statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing the row count that can be chained with other database operations.</returns>
	public DbResult<int> InsertWithCountAsync(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.InsertWithCountAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region UpdateWithCount

	/// <summary>
	/// Executes an UPDATE command and returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., "update if exists" patterns).
	/// </summary>
	/// <param name="sql">The SQL UPDATE statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing the row count that can be chained with other database operations.</returns>
	public DbResult<int> UpdateWithCountAsync(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(this, connection.UpdateWithCountAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region DeleteWithCount

	/// <summary>
	/// Executes a DELETE command and returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., "delete if exists" patterns).
	/// </summary>
	/// <param name="sql">The SQL DELETE statement to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL command.</param>
	/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing the row count that can be chained with other database operations.</returns>
	public DbResult<int> DeleteWithCountAsync(
		string sql,
		object? parameters,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(this, connection.DeleteWithCountAsync(sql, parameters, foreignKeyMessage, transaction, cancellationToken));

	#endregion

	#region Get

	/// <summary>
	/// Retrieves a single entity by executing the specified SQL query.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="key">A key associated with the query result.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> GetAsync<T>(
		string sql,
		object key)
		=> new(this, connection.GetAsync<T>(sql, null, key, transaction, cancellationToken));

	/// <summary>
	/// Retrieves a single entity by executing the specified SQL query.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="key">A key associated with the query result.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> GetAsync<T>(
		string sql,
		object? parameters,
		object key)
		=> new(this, connection.GetAsync<T>(sql, parameters, key, transaction, cancellationToken));

	/// <summary>
	/// Retrieves a single entity by executing the specified SQL query, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the object returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="key">A key associated with the query result.</param>
	/// <param name="mapper">A function to transform the data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> GetAsync<TData, TModel>(
		string sql,
		object key,
		Func<TData, TModel> mapper)
		=> new(this, connection.GetAsync(sql, null, key, mapper, transaction, cancellationToken));

	/// <summary>
	/// Retrieves a single entity by executing the specified SQL query, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the object returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="key">A key associated with the query result.</param>
	/// <param name="mapper">A function to transform the data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> GetAsync<TData, TModel>(
		string sql,
		object? parameters,
		object key,
		Func<TData, TModel> mapper)
		=> new(this, connection.GetAsync(sql, parameters, key, mapper, transaction, cancellationToken));

	#endregion

	#region GetOptional

	/// <summary>
	/// Retrieves zero or one entity by executing the specified SQL query.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the value if one row was found.</returns>
	public DbResult<Optional<T>> GetOptionalAsync<T>(string sql)
		=> new(this, connection.GetOptionalAsync<T>(sql, null, transaction, cancellationToken));

	/// <summary>
	/// Retrieves zero or one entity by executing the specified SQL query.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the value if one row was found.</returns>
	public DbResult<Optional<T>> GetOptionalAsync<T>(string sql, object? parameters)
		=> new(this, connection.GetOptionalAsync<T>(sql, parameters, transaction, cancellationToken));

	/// <summary>
	/// Retrieves zero or one entity by executing the specified SQL query,
	/// applying a mapping function to transform the item if present.
	/// </summary>
	/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
	/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
	/// <param name="mapper">A function to transform the data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the mapped value if one row was found.</returns>
	public DbResult<Optional<TModel>> GetOptionalAsync<TData, TModel>(
		string sql,
		Func<TData, TModel> mapper)
		=> new(this, connection.GetOptionalAsync(sql, null, mapper, transaction, cancellationToken));

	/// <summary>
	/// Retrieves zero or one entity by executing the specified SQL query,
	/// applying a mapping function to transform the item if present.
	/// </summary>
	/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
	/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="mapper">A function to transform the data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the mapped value if one row was found.</returns>
	public DbResult<Optional<TModel>> GetOptionalAsync<TData, TModel>(
		string sql,
		object? parameters,
		Func<TData, TModel> mapper)
		=> new(this, connection.GetOptionalAsync(sql, parameters, mapper, transaction, cancellationToken));

	#endregion

	#region GetScalar

	/// <summary>
	/// Executes the specified SQL query and returns the first column of the first row.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the scalar value to return.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> GetScalarAsync<T>(string sql)
		=> new(this, connection.GetScalarAsync<T>(sql, null, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the first column of the first row.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the scalar value to return.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> GetScalarAsync<T>(string sql, object? parameters)
		=> new(this, connection.GetScalarAsync<T>(sql, parameters, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the first column of the first row, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the scalar value returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the value in the final result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="mapper">A function to transform the data value to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> GetScalarAsync<TData, TModel>(string sql, Func<TData?, TModel> mapper)
		=> new(this, connection.GetScalarAsync(sql, null, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the first column of the first row, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the scalar value returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the value in the final result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="mapper">A function to transform the data value to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<TModel> GetScalarAsync<TData, TModel>(string sql, object? parameters, Func<TData?, TModel> mapper)
		=> new(this, connection.GetScalarAsync(sql, parameters, mapper, transaction, cancellationToken));

	#endregion

	#region QueryOptional

	/// <summary>
	/// Retrieves the first row (if any) by executing the specified SQL query.
	/// </summary>
	/// <remarks>
	/// Use this method when you want the first matching row and don't care if multiple rows exist.
	/// For strict single-row semantics where multiple rows is an error, use <see cref="GetOptionalAsync{T}(string)"/>.
	/// </remarks>
	/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the first row's value.</returns>
	public DbResult<Optional<T>> QueryOptionalAsync<T>(string sql)
		=> new(this, connection.QueryOptionalAsync<T>(sql, null, transaction, cancellationToken));

	/// <summary>
	/// Retrieves the first row (if any) by executing the specified SQL query.
	/// </summary>
	/// <remarks>
	/// Use this method when you want the first matching row and don't care if multiple rows exist.
	/// For strict single-row semantics where multiple rows is an error, use <see cref="GetOptionalAsync{T}(string, object?)"/>.
	/// </remarks>
	/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the first row's value.</returns>
	public DbResult<Optional<T>> QueryOptionalAsync<T>(string sql, object? parameters)
		=> new(this, connection.QueryOptionalAsync<T>(sql, parameters, transaction, cancellationToken));

	/// <summary>
	/// Retrieves the first row (if any) by executing the specified SQL query,
	/// applying a mapping function to transform the item if present.
	/// </summary>
	/// <remarks>
	/// Use this method when you want the first matching row and don't care if multiple rows exist.
	/// For strict single-row semantics where multiple rows is an error, use <see cref="GetOptionalAsync{TData, TModel}(string, Func{TData, TModel})"/>.
	/// </remarks>
	/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="mapper">A function to transform the data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the mapped first row's value.</returns>
	public DbResult<Optional<TModel>> QueryOptionalAsync<TData, TModel>(
		string sql,
		Func<TData, TModel> mapper)
		=> new(this, connection.QueryOptionalAsync(sql, null, mapper, transaction, cancellationToken));

	/// <summary>
	/// Retrieves the first row (if any) by executing the specified SQL query,
	/// applying a mapping function to transform the item if present.
	/// </summary>
	/// <remarks>
	/// Use this method when you want the first matching row and don't care if multiple rows exist.
	/// For strict single-row semantics where multiple rows is an error, use <see cref="GetOptionalAsync{TData, TModel}(string, object?, Func{TData, TModel})"/>.
	/// </remarks>
	/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="mapper">A function to transform the data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> containing an <see cref="Optional{T}"/> 
	/// that is empty if no row was found, or contains the mapped first row's value.</returns>
	public DbResult<Optional<TModel>> QueryOptionalAsync<TData, TModel>(
		string sql,
		object? parameters,
		Func<TData, TModel> mapper)
		=> new(this, connection.QueryOptionalAsync(sql, parameters, mapper, transaction, cancellationToken));

	#endregion

	#region QueryAny

	/// <summary>
	/// Executes the specified SQL query and returns zero or more results as a read-only list.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<IReadOnlyList<T>> QueryAnyAsync<T>(string sql)
		=> new(this, connection.QueryAnyAsync<T>(sql, null, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns zero or more results as a read-only list.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<IReadOnlyList<T>> QueryAnyAsync<T>(string sql, object? parameters)
		=> new(this, connection.QueryAnyAsync<T>(sql, parameters, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns zero or more results, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final result list.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<IReadOnlyList<TModel>> QueryAnyAsync<TData, TModel>(string sql, Func<TData, TModel> mapper)
		=> new(this, connection.QueryAnyAsync(sql, null, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns zero or more results, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final result list.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<IReadOnlyList<TModel>> QueryAnyAsync<TData, TModel>(string sql, object? parameters, Func<TData, TModel> mapper)
		=> new(this, connection.QueryAnyAsync(sql, parameters, mapper, transaction, cancellationToken));

	#endregion

	#region QueryPaged

	/// <summary>
	/// Executes the specified SQL query and returns the results as a paginated result.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="totalCount">The total number of records.</param>
	/// <param name="pageSize">The number of items per page.</param>
	/// <param name="page">The current page number (1-based).</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<T>> QueryPagedAsync<T>(
		string sql,
		int totalCount,
		int pageSize,
		int page)
		=> new(this, connection.QueryPagedAsync<T>(sql, null, totalCount, pageSize, page, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the results as a paginated result.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="totalCount">The total number of records.</param>
	/// <param name="pageSize">The number of items per page.</param>
	/// <param name="page">The current page number (1-based).</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<T>> QueryPagedAsync<T>(
		string sql,
		object? parameters,
		int totalCount,
		int pageSize,
		int page)
		=> new(this, connection.QueryPagedAsync<T>(sql, parameters, totalCount, pageSize, page, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the results as a paginated result, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final paged result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="totalCount">The total number of records.</param>
	/// <param name="pageSize">The number of items per page.</param>
	/// <param name="page">The current page number (1-based).</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<TModel>> QueryPagedAsync<TData, TModel>(
		string sql,
		int totalCount,
		int pageSize,
		int page,
		Func<TData, TModel> mapper)
		=> new(this, connection.QueryPagedAsync(sql, null, totalCount, pageSize, page, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the results as a paginated result, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final paged result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="totalCount">The total number of records.</param>
	/// <param name="pageSize">The number of items per page.</param>
	/// <param name="page">The current page number (1-based).</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<TModel>> QueryPagedAsync<TData, TModel>(
		string sql,
		object? parameters,
		int totalCount,
		int pageSize,
		int page,
		Func<TData, TModel> mapper)
		=> new(this, connection.QueryPagedAsync(sql, parameters, totalCount, pageSize, page, mapper, transaction, cancellationToken));

	#endregion

	#region GetPaged

	/// <summary>
	/// Executes a SQL batch containing a count query and a data query, returning a paginated result.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
	/// <param name="pageSize">The number of items per page.</param>
	/// <param name="page">The current page number (1-based).</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<T>> GetPagedAsync<T>(
		string sql,
		int pageSize,
		int page)
		=> new(this, connection.GetPagedAsync<T>(sql, pageSize, page, transaction, cancellationToken));

	/// <summary>
	/// Executes a SQL batch containing a count query and a data query, returning a paginated result.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// The <paramref name="parameters"/> object must include <c>PageSize</c> and <c>Page</c> properties (both <c>int</c>).
	/// The <c>@Offset</c> parameter is calculated automatically from these values.
	/// </para>
	/// </remarks>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
	/// <param name="parameters">An object containing the query parameters, including <c>PageSize</c> and <c>Page</c> properties.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<T>> GetPagedAsync<T>(
		string sql,
		object parameters)
		=> new(this, connection.GetPagedAsync<T>(sql, parameters, transaction, cancellationToken));

	/// <summary>
	/// Executes a SQL batch containing a count query and a data query, returning a paginated result with mapped items.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters.
	/// </para>
	/// </remarks>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final paged result.</typeparam>
	/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
	/// <param name="pageSize">The number of items per page.</param>
	/// <param name="page">The current page number (1-based).</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<TModel>> GetPagedAsync<TData, TModel>(
		string sql,
		int pageSize,
		int page,
		Func<TData, TModel> mapper)
		=> new(this, connection.GetPagedAsync(sql, pageSize, page, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes a SQL batch containing a count query and a data query, returning a paginated result with mapped items.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// The <paramref name="parameters"/> object must include <c>PageSize</c> and <c>Page</c> properties (both <c>int</c>).
	/// The <c>@Offset</c> parameter is calculated automatically from these values.
	/// </para>
	/// </remarks>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final paged result.</typeparam>
	/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
	/// <param name="parameters">An object containing the query parameters, including <c>PageSize</c> and <c>Page</c> properties.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<PagedResult<TModel>> GetPagedAsync<TData, TModel>(
		string sql,
		object parameters,
		Func<TData, TModel> mapper)
		=> new(this, connection.GetPagedAsync(sql, parameters, mapper, transaction, cancellationToken));

	#endregion

	#region QueryCursor

	/// <summary>
	/// Executes the specified SQL query and returns the results as a cursor-based paginated result.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="pageSize">The maximum number of items to return per page.</param>
	/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<CursorResult<T>> QueryCursorAsync<T, TColumn>(
		string sql,
		int pageSize,
		Func<T, (TColumn Column, Guid Id)> cursorSelector)
		=> new(this, connection.QueryCursorAsync(sql, null, pageSize, cursorSelector, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the results as a cursor-based paginated result.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="pageSize">The maximum number of items to return per page.</param>
	/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<CursorResult<T>> QueryCursorAsync<T, TColumn>(
		string sql,
		object? parameters,
		int pageSize,
		Func<T, (TColumn Column, Guid Id)> cursorSelector)
		=> new(this, connection.QueryCursorAsync(sql, parameters, pageSize, cursorSelector, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the results as a cursor-based paginated result, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final cursor result.</typeparam>
	/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="pageSize">The maximum number of items to return per page.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<CursorResult<TModel>> QueryCursorAsync<TData, TModel, TColumn>(
		string sql,
		int pageSize,
		Func<TData, TModel> mapper,
		Func<TModel, (TColumn Column, Guid Id)> cursorSelector)
		=> new(this, connection.QueryCursorAsync(sql, null, pageSize, mapper, cursorSelector, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns the results as a cursor-based paginated result, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final cursor result.</typeparam>
	/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="pageSize">The maximum number of items to return per page.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<CursorResult<TModel>> QueryCursorAsync<TData, TModel, TColumn>(
		string sql,
		object? parameters,
		int pageSize,
		Func<TData, TModel> mapper,
		Func<TModel, (TColumn Column, Guid Id)> cursorSelector)
		=> new(this, connection.QueryCursorAsync(sql, parameters, pageSize, mapper, cursorSelector, transaction, cancellationToken));

	#endregion

	#region QuerySlice

	/// <summary>
	/// Executes the specified SQL query and returns a slice of results with an indicator for whether more items exist.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="pageSize">The maximum number of items to return.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<SliceResult<T>> QuerySliceAsync<T>(string sql, int pageSize)
		=> new(this, connection.QuerySliceAsync<T>(sql, null, pageSize, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns a slice of results with an indicator for whether more items exist.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="T">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="pageSize">The maximum number of items to return.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<SliceResult<T>> QuerySliceAsync<T>(string sql, object? parameters, int pageSize)
		=> new(this, connection.QuerySliceAsync<T>(sql, parameters, pageSize, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns a slice of results, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final slice result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="pageSize">The maximum number of items to return.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<SliceResult<TModel>> QuerySliceAsync<TData, TModel>(string sql, int pageSize, Func<TData, TModel> mapper)
		=> new(this, connection.QuerySliceAsync(sql, null, pageSize, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes the specified SQL query and returns a slice of results, applying a mapping function.
	/// Returns a <see cref="DbResult{T}"/> for fluent chaining within transactions.
	/// </summary>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final slice result.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="pageSize">The maximum number of items to return.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<SliceResult<TModel>> QuerySliceAsync<TData, TModel>(string sql, object? parameters, int pageSize, Func<TData, TModel> mapper)
		=> new(this, connection.QuerySliceAsync(sql, parameters, pageSize, mapper, transaction, cancellationToken));

	#endregion

	#region MultipleGet

	/// <summary>
	/// Executes a query returning multiple result sets and processes them using the provided mapper.
	/// Returns a failure with <see cref="Exceptions.NotFoundException"/> if the mapper returns null.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
	/// <param name="keys">The keys used to identify the resource for the <see cref="Exceptions.NotFoundException"/>.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> MultipleGetAsync<T>(
		string sql,
		object[] keys,
		Func<IMultipleResult, Task<T?>> mapper)
		=> new(this, connection.MultipleGetAsync(sql, null, keys, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes a query returning multiple result sets and processes them using the provided mapper.
	/// Returns a failure with <see cref="Exceptions.NotFoundException"/> if the mapper returns null.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="keys">The keys used to identify the resource for the <see cref="Exceptions.NotFoundException"/>.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<T> MultipleGetAsync<T>(
		string sql,
		object? parameters,
		object[] keys,
		Func<IMultipleResult, Task<T?>> mapper)
		=> new(this, connection.MultipleGetAsync(sql, parameters, keys, mapper, transaction, cancellationToken));

	#endregion

	#region MultipleGetOptional

	/// <summary>
	/// Executes a query returning multiple result sets and processes them using the provided mapper.
	/// Returns an <see cref="Optional{T}"/> that is empty if the mapper returns null.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<Optional<T>> MultipleGetOptionalAsync<T>(
		string sql,
		Func<IMultipleResult, Task<T?>> mapper)
		=> new(this, connection.MultipleGetOptionalAsync(sql, null, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes a query returning multiple result sets and processes them using the provided mapper.
	/// Returns an <see cref="Optional{T}"/> that is empty if the mapper returns null.
	/// </summary>
	/// <typeparam name="T">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<Optional<T>> MultipleGetOptionalAsync<T>(
		string sql,
		object? parameters,
		Func<IMultipleResult, Task<T?>> mapper)
		=> new(this, connection.MultipleGetOptionalAsync(sql, parameters, mapper, transaction, cancellationToken));

	#endregion

	#region MultipleQueryAny

	/// <summary>
	/// Executes a query returning multiple result sets and processes them using the provided mapper.
	/// Returns the list from the mapper; an empty list is a valid result.
	/// </summary>
	/// <typeparam name="T">The type of the elements in the returned list.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the list.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<IReadOnlyList<T>> MultipleQueryAnyAsync<T>(
		string sql,
		Func<IMultipleResult, Task<IReadOnlyList<T>?>> mapper)
		=> new(this, connection.MultipleQueryAnyAsync(sql, null, mapper, transaction, cancellationToken));

	/// <summary>
	/// Executes a query returning multiple result sets and processes them using the provided mapper.
	/// Returns the list from the mapper; an empty list is a valid result.
	/// </summary>
	/// <typeparam name="T">The type of the elements in the returned list.</typeparam>
	/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
	/// <param name="parameters">An object containing the parameters to be passed to the SQL query.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the list.</param>
	/// <returns>A <see cref="DbResult{T}"/> that can be chained with other database operations.</returns>
	public DbResult<IReadOnlyList<T>> MultipleQueryAnyAsync<T>(
		string sql,
		object? parameters,
		Func<IMultipleResult, Task<IReadOnlyList<T>?>> mapper)
		=> new(this, connection.MultipleQueryAnyAsync(sql, parameters, mapper, transaction, cancellationToken));

	#endregion

}