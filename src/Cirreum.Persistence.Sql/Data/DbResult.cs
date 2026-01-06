namespace Cirreum.Persistence;

using Cirreum.Exceptions;
using System.Runtime.CompilerServices;

/// <summary>
/// Represents a non-generic database operation result that carries a <see cref="DbContext"/> context
/// for fluent chaining of database operations within a transaction.
/// </summary>
/// <remarks>
/// <para>
/// This type enables fluent chaining without passing the builder to each method:
/// </para>
/// <para>
/// <strong>In a Transaction:</strong>
/// </para>
/// <code>
/// return await conn.ExecuteInTransactionAsync(builder =&gt; builder
///     .GetAsync&lt;Data&gt;(sql, key)
///     .EnsureAsync(d =&gt; d.IsActive, new BadRequestException("Not active"))
///     .ThenGetAsync&lt;int&gt;(countSql, parameters, countKey)
///     .ThenDeleteAsync(deleteSql, parameters, deleteKey)
///     .ToResult()
/// ), cancellationToken);
/// </code>
/// <para>
/// <strong>Without a transaction (read-only):</strong>
/// </para>
/// <code>
/// return await conn.ExecuteAsync(ctx =&gt; ctx
///     .GetAsync&lt;Data&gt;(sql, key)
///     .EnsureAsync(d =&gt; d.IsActive, new BadRequestException("Not active"))
///     .ThenGetAsync&lt;Data&gt;(nextSql, nextKey)
/// ), cancellationToken);
/// </code>
/// </remarks>
public readonly struct DbResult(DbContext context, Task<Result> resultTask) {

	/// <summary>
	/// Gets the underlying result task.
	/// </summary>
	public Task<Result> Result => resultTask;

	/// <summary>
	/// Enables direct awaiting of the DbResultNonGeneric.
	/// </summary>
	public TaskAwaiter<Result> GetAwaiter() => resultTask.GetAwaiter();

	/// <summary>
	/// Implicitly converts to Task for compatibility with existing Result extensions.
	/// </summary>
	public static implicit operator Task<Result>(DbResult dbResult) => dbResult.Result;

	#region Then

	/// <summary>
	/// Chains an arbitrary async operation that returns a non-generic Result.
	/// Use this as an escape hatch to integrate external async operations into the fluent chain.
	/// </summary>
	/// <param name="next">The async operation to execute.</param>
	public DbResult ThenAsync(Func<Task<Result>> next)
		=> new(context, this.ThenAsyncCore(next));

	/// <summary>
	/// Chains an arbitrary async operation that returns a Result&lt;T&gt;.
	/// Use this as an escape hatch to integrate external async operations into the fluent chain.
	/// </summary>
	/// <param name="next">The async operation to execute.</param>
	public DbResult<T> ThenAsync<T>(Func<Task<Result<T>>> next)
		=> new(context, this.ThenAsyncCore(next));

	private async Task<Result> ThenAsyncCore(Func<Task<Result>> next) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		return await next().ConfigureAwait(false);
	}

	private async Task<Result<T>> ThenAsyncCore<T>(Func<Task<Result<T>>> next) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await next().ConfigureAwait(false);
	}

	#endregion


	//========================= THEN INSERT =========================
	//===============================================================

	#region Then Insert

	/// <summary>
	/// Chains an INSERT operation after a successful result.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenInsertAsync(
		string sql,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAsyncCore(sql, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation after a successful result.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parameters">The parameters for the INSERT statement.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenInsertAsync(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAsyncCore(sql, parameters, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation after a successful result.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenInsertAsync(
		string sql,
		Func<object?> parametersFactory,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAsyncCore(sql, parametersFactory, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result> ThenInsertAsyncCore(
			string sql,
			string uniqueConstraintMessage,
			string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		return await context
			.InsertAsync(sql, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result> ThenInsertAsyncCore(
		string sql,
		object? parameters,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		return await context
			.InsertAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result> ThenInsertAsyncCore(
		string sql,
		Func<object?> parametersFactory,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		return await context
			.InsertAsync(sql, parametersFactory(), uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Insert-If (bool overloads)

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="when">A condition that determines whether to execute the insert; if false, the insert is skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenInsertIfAsync(
		string sql,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? this.ThenInsertAsync(sql, uniqueConstraintMessage, foreignKeyMessage)
			: this;

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the INSERT statement.</param>
	/// <param name="when">A condition that determines whether to execute the insert; if false, the insert is skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenInsertIfAsync(
		string sql,
		Func<object?> parametersFactory,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? this.ThenInsertAsync(sql, parametersFactory, uniqueConstraintMessage, foreignKeyMessage)
			: this;
	#endregion

	#region Then Insert-If (Func<bool> overloads)

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="when">Predicate that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenInsertIfAsync(
		string sql,
		Func<bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAsyncCore(sql, when, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the INSERT statement.</param>
	/// <param name="when">Predicate that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	/// <remarks>
	/// <strong>NOTE:</strong> we only allow a parameters factory here to avoid capturing nullable values
	/// </remarks>
	public DbResult ThenInsertIfAsync(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAsyncCore(sql, parametersFactory, when, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result> ThenInsertIfAsyncCore(
		string sql,
		Func<bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when()) {
			return result;
		}
		return await context
			.InsertAsync(sql, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result> ThenInsertIfAsyncCore(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when()) {
			return result;
		}
		return await context
			.InsertAsync(sql, parametersFactory(), uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Insert and Return

	/// <summary>
	/// Chains an INSERT operation that returns a value after a successful result.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="resultSelector">Factory to create the result value after successful insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertAndReturnAsync<T>(
		string sql,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndReturnAsyncCore(sql, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation that returns a value after a successful result.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parameters">The parameters for the INSERT statement.</param>
	/// <param name="resultSelector">Factory to create the result value after successful insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertAndReturnAsync<T>(
		string sql,
		object? parameters,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndReturnAsyncCore(sql, parameters, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation that returns a value after a successful result.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the INSERT statement.</param>
	/// <param name="resultSelector">Factory to create the result value after successful insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndReturnAsyncCore(sql, parametersFactory, resultSelector, uniqueConstraintMessage, foreignKeyMessage));


	private async Task<Result<T>> ThenInsertAndReturnAsyncCore<T>(
		string sql,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context
			.InsertAndReturnAsync(sql, resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result<T>> ThenInsertAndReturnAsyncCore<T>(
		string sql,
		object? parameters,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context
			.InsertAndReturnAsync(sql, parameters, resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result<T>> ThenInsertAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context
			.InsertAndReturnAsync(sql, parametersFactory(), resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Insert-If and Return (bool overloads)

	/// <summary>
	/// Conditionally chains an INSERT operation that returns a value after a successful result.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="when">A condition that determines whether to execute the insert; if false, the insert is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value after successful insert or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAndReturnAsync<T>(
		string sql,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAndReturnAsyncCore(sql, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Conditionally chains an INSERT operation that returns a value after a successful result.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the INSERT statement.</param>
	/// <param name="when">A condition that determines whether to execute the insert; if false, the insert is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value after successful insert or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAndReturnAsyncCore(sql, parametersFactory, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<T>> ThenInsertIfAndReturnAsyncCore<T>(
		string sql,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when) {
			return resultSelector();
		}
		return await context
			.InsertAndReturnAsync(sql, resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result<T>> ThenInsertIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when) {
			return resultSelector();
		}
		return await context
			.InsertAndReturnAsync(sql, parametersFactory(), resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Insert-If and Return (Func<bool> overloads)

	/// <summary>
	/// Conditionally chains an INSERT operation that returns a value after a successful result.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="when">Predicate that determines whether to execute the insert.</param>
	/// <param name="resultSelector">Factory to create the result value after successful insert or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAndReturnAsync<T>(
		string sql,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAndReturnAsyncCore(sql, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Conditionally chains an INSERT operation that returns a value after a successful result.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">The parameters factory to resolve the parameters for the INSERT statement.</param>
	/// <param name="when">Predicate that determines whether to execute the insert.</param>
	/// <param name="resultSelector">Factory to create the result value after successful insert or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAndReturnAsyncCore(sql, parametersFactory, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));


	private async Task<Result<T>> ThenInsertIfAndReturnAsyncCore<T>(
		string sql,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when()) {
			return resultSelector();
		}
		return await context
			.InsertAndReturnAsync(sql, resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result<T>> ThenInsertIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when()) {
			return resultSelector();
		}
		return await context
			.InsertAndReturnAsync(sql, parametersFactory(), resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Insert and Return (entity shortcut)

	/// <summary>
	/// Chains an INSERT operation using the entity as both parameters and return value.
	/// </summary>
	/// <typeparam name="TEntity">The type of the entity to insert and return.</typeparam>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="entity">The entity used for parameters and returned on success.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TEntity> ThenInsertAndReturnAsync<TEntity>(
		string sql,
		TEntity entity,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist") where TEntity : class
		=> this.ThenInsertAndReturnAsync(sql, entity, () => entity, uniqueConstraintMessage, foreignKeyMessage);

	#endregion

	#region Then Insert-If and Return (entity shortcut)

	/// <summary>
	/// Conditionally chains an INSERT operation using the entity as both parameters and return value.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues with the entity.
	/// </summary>
	/// <typeparam name="TEntity">The type of the entity to insert and return.</typeparam>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="entity">The entity used for parameters and returned on success or when skipped.</param>
	/// <param name="when">A condition that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TEntity> ThenInsertIfAndReturnAsync<TEntity>(
		string sql,
		TEntity entity,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist") where TEntity : class
		=> this.ThenInsertIfAndReturnAsync(sql, () => entity, when, () => entity, uniqueConstraintMessage, foreignKeyMessage);

	/// <summary>
	/// Conditionally chains an INSERT operation using the entity as both parameters and return value.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues with the entity.
	/// </summary>
	/// <typeparam name="TEntity">The type of the entity to insert and return.</typeparam>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="entity">The entity used for parameters and returned on success or when skipped.</param>
	/// <param name="when">Predicate that determines whether to execute the insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TEntity> ThenInsertIfAndReturnAsync<TEntity>(
		string sql,
		TEntity entity,
		Func<bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist") where TEntity : class
		=> this.ThenInsertIfAndReturnAsync(sql, () => entity, when, () => entity, uniqueConstraintMessage, foreignKeyMessage);

	#endregion

	//========================= THEN UPDATE =========================
	//===============================================================

	#region Update

	/// <summary>
	/// Chains an UPDATE operation after a successful result.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parameters">The parameters for the UPDATE statement.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenUpdateAsync(
		string sql,
		object? parameters,
		object key,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAsyncCore(sql, parameters, key, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result> ThenUpdateAsyncCore(
		string sql,
		object? parameters,
		object key,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		return await context
			.UpdateAsync(sql, parameters, key, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Update-If (bool and Func<bool> overloads)

	/// <summary>
	/// Conditionally chains an UPDATE operation after a successful result.
	/// If <paramref name="when"/> is false, the update is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the UPDATE statement.</param>
	/// <param name="key">The key of the entity being updated, used in the NotFoundException if no rows are affected.</param>
	/// <param name="when">A condition that determines whether to execute the update; if false, the update is skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenUpdateIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? this.ThenUpdateAsync(sql, parametersFactory, key, uniqueConstraintMessage, foreignKeyMessage)
			: this;

	/// <summary>
	/// Conditionally chains an UPDATE operation after a successful result.
	/// If <paramref name="when"/> returns false, the update is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the UPDATE statement.</param>
	/// <param name="key">The key of the entity being updated, used in the NotFoundException if no rows are affected.</param>
	/// <param name="when">Predicate that determines whether to execute the update; if false, the update is skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenUpdateIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateIfAsyncCore(sql, parametersFactory, key, when, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result> ThenUpdateIfAsyncCore(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when()) {
			return result;
		}
		return await context
			.UpdateAsync(sql, parametersFactory(), key, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Update and Return

	/// <summary>
	/// Chains an UPDATE operation that returns a value after a successful result.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parameters">The parameters for the UPDATE statement.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="resultSelector">Factory to create the result value after successful update.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenUpdateAndReturnAsync<T>(
		string sql,
		object? parameters,
		object key,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAndReturnAsyncCore(sql, parameters, key, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<T>> ThenUpdateAndReturnAsyncCore<T>(
		string sql,
		object? parameters,
		object key,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context
			.UpdateAndReturnAsync(sql, parameters, key, resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Update-If and Return (bool overload)

	/// <summary>
	/// Conditionally chains an UPDATE operation that returns a value after a successful result.
	/// If <paramref name="when"/> is false, the update is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the UPDATE statement.</param>
	/// <param name="key">The key of the entity being updated, used in the NotFoundException if no rows are affected.</param>
	/// <param name="when">A condition that determines whether to execute the update; if false, the update is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value after successful update or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenUpdateIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<T>> ThenUpdateIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when) {
			return resultSelector();
		}
		return await context
			.UpdateAndReturnAsync(sql, parametersFactory(), key, resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Update-If and Return (Func<bool> overloads)

	/// <summary>
	/// Conditionally chains an UPDATE operation that returns a value after a successful result.
	/// If <paramref name="when"/> returns false, the update is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">Predicate that determines whether to execute the update.</param>
	/// <param name="resultSelector">Factory to create the result value after successful update or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenUpdateIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<T>> ThenUpdateIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when()) {
			return resultSelector();
		}
		return await context
			.UpdateAndReturnAsync(sql, parametersFactory(), key, resultSelector, uniqueConstraintMessage, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion


	//========================= THEN DELETE =========================
	//===============================================================

	#region Then Delete

	/// <summary>
	/// Chains a DELETE operation after a successful result.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parameters">The parameters for the DELETE statement.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenDeleteAsync(
		string sql,
		object? parameters,
		object key,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAsyncCore(sql, parameters, key, foreignKeyMessage));


	private async Task<Result> ThenDeleteAsyncCore(
		string sql,
		object? parameters,
		object key,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		return await context
			.DeleteAsync(sql, parameters, key, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then Delete-If (bool overload)

	/// <summary>
	/// Conditionally chains a DELETE operation after a successful result.
	/// If <paramref name="when"/> is false, the delete is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted, used in the NotFoundException if no rows are affected.</param>
	/// <param name="when">A condition that determines whether to execute the delete; if false, the delete is skipped.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenDeleteIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> when
			? this.ThenDeleteAsync(sql, parametersFactory, key, foreignKeyMessage)
			: this;

	#endregion

	#region Then Delete-If (Func<bool> overload)

	/// <summary>
	/// Conditionally chains a DELETE operation after a successful result.
	/// If <paramref name="when"/> returns false, the delete is skipped and the chain continues.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">Predicate that determines whether to execute the delete.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult ThenDeleteIfAsync(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteIfAsyncCore(sql, parametersFactory, key, when, foreignKeyMessage));

	private async Task<Result> ThenDeleteIfAsyncCore(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when()) {
			return result;
		}
		return await context
			.DeleteAsync(sql, parametersFactory(), key, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
	}


	#endregion

	#region Then Delete and Return

	/// <summary>
	/// Chains a DELETE operation that returns a value after a successful result.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parameters">The parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted, used in the NotFoundException if no rows are affected.</param>
	/// <param name="resultSelector">Factory to create the result value after successful delete.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteAndReturnAsync<T>(
		string sql,
		object? parameters,
		object key,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAndReturnAsyncCore(sql, parameters, key, resultSelector, foreignKeyMessage));

	/// <summary>
	/// Chains a DELETE operation that returns a value after a successful result.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted, used in the NotFoundException if no rows are affected.</param>
	/// <param name="resultSelector">Factory to create the result value after successful delete.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAndReturnAsyncCore(sql, parametersFactory, key, resultSelector, foreignKeyMessage));

	private async Task<Result<T>> ThenDeleteAndReturnAsyncCore<T>(
		string sql,
		object? parameters,
		object key,
		Func<T> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var deleteResult = await context
			.DeleteAsync(sql, parameters, key, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector();
	}

	private async Task<Result<T>> ThenDeleteAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<T> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var deleteResult = await context
			.DeleteAsync(sql, parametersFactory(), key, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector();
	}

	#endregion

	#region Then Delete-If and Return

	/// <summary>
	/// Conditionally chains a DELETE operation that returns a value after a successful result.
	/// If <paramref name="when"/> is false, the delete is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted, used in the NotFoundException if no rows are affected.</param>
	/// <param name="when">A condition that determines whether to execute the delete; if false, the delete is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value after successful delete or when skipped.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, foreignKeyMessage));

	/// <summary>
	/// Conditionally chains a DELETE operation that returns a value after a successful result.
	/// If <paramref name="when"/> returns false, the delete is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters for the DELETE statement.</param>
	/// <param name="key">The key of the entity being deleted, used in the NotFoundException if no rows are affected.</param>
	/// <param name="when">Predicate that determines whether to execute the delete; if false, the delete is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value after successful delete or when skipped.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteIfAndReturnAsync<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, foreignKeyMessage));

	private async Task<Result<T>> ThenDeleteIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		bool when,
		Func<T> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when) {
			return resultSelector();
		}
		var deleteResult = await context
			.DeleteAsync(sql, parametersFactory(), key, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector();
	}

	private async Task<Result<T>> ThenDeleteIfAndReturnAsyncCore<T>(
		string sql,
		Func<object?> parametersFactory,
		object key,
		Func<bool> when,
		Func<T> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when()) {
			return resultSelector();
		}
		var deleteResult = await context
			.DeleteAsync(sql, parametersFactory(), key, foreignKeyMessage)
			.Result
			.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector();
	}

	#endregion


	//==================== THEN COMMAND WITH COUNT ==================
	//===============================================================

	#region Then InsertWithCount

	/// <summary>
	/// Chains an INSERT operation that returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., conditional inserts).
	/// </summary>
	/// <param name="sql">The SQL INSERT statement.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<int> ThenInsertWithCountAsync(
		string sql,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertWithCountAsyncCore(sql, null, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation that returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., conditional inserts).
	/// </summary>
	/// <param name="sql">The SQL INSERT statement.</param>
	/// <param name="parameters">The parameters for the INSERT statement.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<int> ThenInsertWithCountAsync(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertWithCountAsyncCore(sql, parameters, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<int>> ThenInsertWithCountAsyncCore(
		string sql,
		object? parameters,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.InsertWithCountAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then UpdateWithCount

	/// <summary>
	/// Chains an UPDATE operation that returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., "update if exists" patterns).
	/// </summary>
	/// <param name="sql">The SQL UPDATE statement.</param>
	/// <param name="parameters">The parameters for the UPDATE statement.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<int> ThenUpdateWithCountAsync(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateWithCountAsyncCore(sql, parameters, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<int>> ThenUpdateWithCountAsyncCore(
		string sql,
		object? parameters,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.UpdateWithCountAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then DeleteWithCount

	/// <summary>
	/// Chains a DELETE operation that returns the number of rows affected.
	/// Use this when 0 rows is a valid outcome (e.g., "delete if exists" patterns).
	/// </summary>
	/// <param name="sql">The SQL DELETE statement.</param>
	/// <param name="parameters">The parameters for the DELETE statement.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<int> ThenDeleteWithCountAsync(
		string sql,
		object? parameters,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteWithCountAsyncCore(sql, parameters, foreignKeyMessage));

	private async Task<Result<int>> ThenDeleteWithCountAsyncCore(
		string sql,
		object? parameters,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.DeleteWithCountAsync(sql, parameters, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	#endregion


	//========================= THEN GET/QUERY ======================
	//===============================================================

	#region Then Get

	/// <summary>
	/// Chains a GET operation after a successful result.
	/// </summary>
	public DbResult<TResult> ThenGetAsync<TResult>(string sql, object key)
		=> new(context, this.ThenGetAsyncCore<TResult>(sql, null, key));

	/// <summary>
	/// Chains a GET operation with parameters after a successful result.
	/// </summary>
	public DbResult<TResult> ThenGetAsync<TResult>(string sql, object? parameters, object key)
		=> new(context, this.ThenGetAsyncCore<TResult>(sql, parameters, key));

	/// <summary>
	/// Chains a GET operation with mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetAsync<TData, TModel>(string sql, object key, Func<TData, TModel> mapper)
		=> new(context, this.ThenGetAsyncCore(sql, null, key, mapper));

	/// <summary>
	/// Chains a GET operation with parameters and mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetAsync<TData, TModel>(string sql, object? parameters, object key, Func<TData, TModel> mapper)
		=> new(context, this.ThenGetAsyncCore(sql, parameters, key, mapper));

	private async Task<Result<TResult>> ThenGetAsyncCore<TResult>(string sql, object? parameters, object key) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context
			.GetAsync<TResult>(sql, parameters, key)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenGetAsyncCore<TData, TModel>(string sql, object? parameters, object key, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context
			.GetAsync(sql, parameters, key, mapper)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then GetScalar

	/// <summary>
	/// Chains a GET scalar operation after a successful result.
	/// </summary>
	public DbResult<TResult> ThenGetScalarAsync<TResult>(string sql)
		=> new(context, this.ThenGetScalarAsyncCore<TResult>(sql, null));

	/// <summary>
	/// Chains a GET scalar operation with parameters after a successful result.
	/// </summary>
	public DbResult<TResult> ThenGetScalarAsync<TResult>(string sql, object? parameters)
		=> new(context, this.ThenGetScalarAsyncCore<TResult>(sql, parameters));

	/// <summary>
	/// Chains a GET scalar operation with mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetScalarAsync<TData, TModel>(string sql, Func<TData?, TModel> mapper)
		=> new(context, this.ThenGetScalarAsyncCore(sql, null, mapper));

	/// <summary>
	/// Chains a GET scalar operation with parameters and mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetScalarAsync<TData, TModel>(string sql, object? parameters, Func<TData?, TModel> mapper)
		=> new(context, this.ThenGetScalarAsyncCore(sql, parameters, mapper));

	private async Task<Result<TResult>> ThenGetScalarAsyncCore<TResult>(string sql, object? parameters) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context
			.GetScalarAsync<TResult>(sql, parameters)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenGetScalarAsyncCore<TData, TModel>(string sql, object? parameters, Func<TData?, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context
			.GetScalarAsync(sql, parameters, mapper)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	#region Then QueryAny

	/// <summary>
	/// Chains a QueryAny operation after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TResult>> ThenQueryAnyAsync<TResult>(string sql)
		=> new(context, this.ThenQueryAnyAsyncCore<TResult>(sql, null));

	/// <summary>
	/// Chains a QueryAny operation with parameters after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TResult>> ThenQueryAnyAsync<TResult>(string sql, object? parameters)
		=> new(context, this.ThenQueryAnyAsyncCore<TResult>(sql, parameters));

	/// <summary>
	/// Chains a QueryAny operation with mapping after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TModel>> ThenQueryAnyAsync<TData, TModel>(string sql, Func<TData, TModel> mapper)
		=> new(context, this.ThenQueryAnyAsyncCore(sql, null, mapper));

	/// <summary>
	/// Chains a QueryAny operation with parameters and mapping after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TModel>> ThenQueryAnyAsync<TData, TModel>(string sql, object? parameters, Func<TData, TModel> mapper)
		=> new(context, this.ThenQueryAnyAsyncCore(sql, parameters, mapper));

	private async Task<Result<IReadOnlyList<TResult>>> ThenQueryAnyAsyncCore<TResult>(string sql, object? parameters) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context
			.QueryAnyAsync<TResult>(sql, parameters)
			.Result
			.ConfigureAwait(false);
	}

	private async Task<Result<IReadOnlyList<TModel>>> ThenQueryAnyAsyncCore<TData, TModel>(string sql, object? parameters, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context
			.QueryAnyAsync(sql, parameters, mapper)
			.Result
			.ConfigureAwait(false);
	}

	#endregion

	//==================== THEN GET/QUERY MULTIPLE ==================
	//===============================================================

	#region Then MultipleGet

	/// <summary>
	/// Chains a multiple-result GET operation after a successful result.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="keys">The keys used to identify the resource for the <see cref="Cirreum.Exceptions.NotFoundException"/>.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	public DbResult<TResult> ThenMultipleGetAsync<TResult>(
		string sql,
		object[] keys,
		Func<IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetAsyncCore(sql, null, keys, mapper));

	/// <summary>
	/// Chains a multiple-result GET operation with parameters after a successful result.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters for the query.</param>
	/// <param name="keys">The keys used to identify the resource for the <see cref="Cirreum.Exceptions.NotFoundException"/>.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	public DbResult<TResult> ThenMultipleGetAsync<TResult>(
		string sql,
		object? parameters,
		object[] keys,
		Func<IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetAsyncCore(sql, parameters, keys, mapper));

	private async Task<Result<TResult>> ThenMultipleGetAsyncCore<TResult>(
		string sql,
		object? parameters,
		object[] keys,
		Func<IMultipleResult, Task<TResult?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleGetAsync(sql, parameters, keys, mapper).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then MultipleGetOptional

	/// <summary>
	/// Chains a multiple-result optional GET operation after a successful result.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	public DbResult<Optional<TResult>> ThenMultipleGetOptionalAsync<TResult>(
		string sql,
		Func<IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetOptionalAsyncCore<TResult>(sql, null, mapper));

	/// <summary>
	/// Chains a multiple-result optional GET operation with parameters after a successful result.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters for the query.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
	public DbResult<Optional<TResult>> ThenMultipleGetOptionalAsync<TResult>(
		string sql,
		object? parameters,
		Func<IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetOptionalAsyncCore<TResult>(sql, parameters, mapper));

	private async Task<Result<Optional<TResult>>> ThenMultipleGetOptionalAsyncCore<TResult>(
		string sql,
		object? parameters,
		Func<IMultipleResult, Task<TResult?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleGetOptionalAsync(sql, parameters, mapper).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then MultipleQueryAny

	/// <summary>
	/// Chains a multiple-result query operation after a successful result.
	/// </summary>
	/// <typeparam name="TResult">The type of the elements in the returned list.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the list.</param>
	public DbResult<IReadOnlyList<TResult>> ThenMultipleQueryAnyAsync<TResult>(
		string sql,
		Func<IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper)
		=> new(context, this.ThenMultipleQueryAnyAsyncCore(sql, null, mapper));

	/// <summary>
	/// Chains a multiple-result query operation with parameters after a successful result.
	/// </summary>
	/// <typeparam name="TResult">The type of the elements in the returned list.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters for the query.</param>
	/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the list.</param>
	public DbResult<IReadOnlyList<TResult>> ThenMultipleQueryAnyAsync<TResult>(
		string sql,
		object? parameters,
		Func<IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper)
		=> new(context, this.ThenMultipleQueryAnyAsyncCore(sql, parameters, mapper));

	private async Task<Result<IReadOnlyList<TResult>>> ThenMultipleQueryAnyAsyncCore<TResult>(
		string sql,
		object? parameters,
		Func<IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleQueryAnyAsync(sql, parameters, mapper).Result.ConfigureAwait(false);
	}

	#endregion

}