namespace Cirreum.Persistence;

using System.Runtime.CompilerServices;

/// <summary>
/// Represents an in-progress database operation that carries the current <see cref="DbContext"/>
/// and a pending <see cref="Result{T}"/> for fluent composition.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="DbResult{T}"/> is designed to build a chain of dependent database operations without repeatedly
/// passing a context parameter. Each chaining method short-circuits on failure: if the current result is a failure,
/// the remainder of the chain is skipped and the failure is propagated.
/// </para>
/// <para>
/// This type is awaitable; awaiting it yields the underlying <see cref="Result{T}"/>.
/// </para>
/// <para>
/// <strong>In a Transaction:</strong>
/// </para>
/// <code>
/// return await conn.ExecuteTransactionAsync(ctx =&gt; ctx
///     .GetAsync&lt;Data&gt;(sql, key)
///     .EnsureAsync(d =&gt; d.IsActive, new BadRequestException("Not active"))
///     .ThenUpdateAsync(updateSql, p, key)
///     .ThenGetAsync&lt;Data&gt;(nextSql, nextKey)
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
/// <typeparam name="T">The value type carried by the current result.</typeparam>
public readonly struct DbResult<T>(DbContext context, Task<Result<T>> resultTask) {

	/// <summary>
	/// Gets the underlying task that produces the current <see cref="Result{T}"/>.
	/// </summary>
	public Task<Result<T>> Result => resultTask;

	/// <summary>
	/// Enables <c>await</c> directly on <see cref="DbResult{T}"/> by returning the awaiter
	/// for the underlying result task.
	/// </summary>
	public TaskAwaiter<Result<T>> GetAwaiter() => resultTask.GetAwaiter();

	/// <summary>
	/// Implicitly converts this instance to its underlying <see cref="Task{TResult}"/> to enable
	/// reuse of existing extensions targeting <c>Task&lt;Result&lt;T&gt;&gt;</c>.
	/// </summary>
	public static implicit operator Task<Result<T>>(DbResult<T> dbResult) => dbResult.Result;

	/// <summary>
	/// The current context associated with this result.
	/// </summary>
	internal DbContext Context => context;

	#region ToResult

	/// <summary>
	/// Converts this instance to a non-generic <see cref="DbResult"/>, discarding the value while
	/// preserving the success/failure state.
	/// </summary>
	/// <remarks>
	/// This is commonly used at the end of a chain when the transaction should return only success/failure.
	/// </remarks>
	public DbResult ToResult()
		=> new(context, this.ToResultCore());

	private async Task<Result> ToResultCore() {
		var result = await resultTask.ConfigureAwait(false);
		return result.ToResult();
	}

	#endregion

	#region Ensure

	/// <summary>
	/// Ensures the current value satisfies a predicate; otherwise returns a failure.
	/// </summary>
	/// <remarks>
	/// If the current result is a failure, the predicate is not evaluated and the failure is propagated.
	/// </remarks>
	public DbResult<T> EnsureAsync(Func<T, bool> predicate, string errorMessage)
		=> new(context, resultTask.EnsureAsyncTask(predicate, errorMessage));

	/// <summary>
	/// Ensures the current value satisfies a predicate; otherwise returns a failure.
	/// </summary>
	/// <remarks>
	/// If the current result is a failure, the predicate is not evaluated and the failure is propagated.
	/// </remarks>
	public DbResult<T> EnsureAsync(Func<T, bool> predicate, Exception error)
		=> new(context, resultTask.EnsureAsyncTask(predicate, error));

	/// <summary>
	/// Ensures the current value satisfies a predicate; otherwise returns a failure.
	/// </summary>
	/// <remarks>
	/// If the current result is a failure, the predicate is not evaluated and the failure is propagated.
	/// </remarks>
	public DbResult<T> EnsureAsync(Func<T, bool> predicate, Func<T, Exception> error)
		=> new(context, resultTask.EnsureAsyncTask(predicate, error));

	/// <summary>
	/// Ensures the current value satisfies a predicate; otherwise returns a failure.
	/// </summary>
	/// <remarks>
	/// If the current result is a failure, the predicate is not evaluated and the failure is propagated.
	/// </remarks>
	public DbResult<T> EnsureAsync(Func<T, Task<bool>> predicate, string errorMessage)
		=> new(context, resultTask.EnsureAsyncTask(predicate, errorMessage));

	/// <summary>
	/// Ensures the current value satisfies a predicate; otherwise returns a failure.
	/// </summary>
	/// <remarks>
	/// If the current result is a failure, the predicate is not evaluated and the failure is propagated.
	/// </remarks>
	public DbResult<T> EnsureAsync(Func<T, Task<bool>> predicate, Exception error)
		=> new(context, resultTask.EnsureAsyncTask(predicate, error));

	/// <summary>
	/// Ensures the current value satisfies a predicate; otherwise returns a failure.
	/// </summary>
	/// <remarks>
	/// If the current result is a failure, the predicate is not evaluated and the failure is propagated.
	/// </remarks>
	public DbResult<T> EnsureAsync(Func<T, Task<bool>> predicate, Func<T, Exception> error)
		=> new(context, resultTask.EnsureAsyncTask(predicate, error));

	#endregion

	#region Map

	/// <summary>
	/// Transforms the current successful value into a new value.
	/// </summary>
	/// <remarks>
	/// If the current result represents a failure, the mapping function is not invoked and the
	/// failure is propagated unchanged.
	/// </remarks>
	/// <typeparam name="TResult">
	/// The type of the value produced by the mapping function.
	/// </typeparam>
	/// <param name="mapper">
	/// A function that transforms the successful value of the current result into a new value.
	/// </param>
	/// <returns>
	/// A <see cref="DbResult{TResult}"/> containing the transformed value if the original result
	/// is successful; otherwise, a result containing the original failure.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// Thrown when <paramref name="mapper"/> is <see langword="null"/>.
	/// </exception>
	public DbResult<TResult> MapAsync<TResult>(Func<T, TResult> mapper)
		=> new(context, resultTask.MapAsyncTask(mapper));

	/// <summary>
	/// Transforms the current successful value into a new value asynchronously.
	/// </summary>
	/// <remarks>
	/// If the current result represents a failure, the mapping function is not invoked and the
	/// failure is propagated unchanged. Any exception thrown while executing the mapping function
	/// is captured and returned as a failed result.
	/// </remarks>
	/// <typeparam name="TResult">
	/// The type of the value produced by the mapping function.
	/// </typeparam>
	/// <param name="mapper">
	/// An asynchronous function that transforms the successful value of the current result into
	/// a new value.
	/// </param>
	/// <returns>
	/// A <see cref="DbResult{TResult}"/> containing the transformed value if the original result
	/// is successful; otherwise, a result containing the original failure or any exception thrown
	/// by the mapping function.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// Thrown when <paramref name="mapper"/> is <see langword="null"/>.
	/// </exception>
	public DbResult<TResult> MapAsync<TResult>(Func<T, Task<TResult>> mapper)
		=> new(context, resultTask.MapAsyncTask(mapper));

	/// <summary>
	/// Transforms the current successful value into a new value asynchronously using a
	/// <see cref="ValueTask{TResult}"/>-based mapping function.
	/// </summary>
	/// <remarks>
	/// If the current result represents a failure, the mapping function is not invoked and the
	/// failure is propagated unchanged. Any exception thrown by the mapping function is captured
	/// and returned as a failed result. This overload is optimized for mapping functions that
	/// may complete synchronously.
	/// </remarks>
	/// <typeparam name="TResult">
	/// The type of the value produced by the mapping function.
	/// </typeparam>
	/// <param name="mapper">
	/// An asynchronous mapping function that transforms the successful value of the current result
	/// into a new value.
	/// </param>
	/// <returns>
	/// A <see cref="DbResult{TResult}"/> containing the transformed value if the original result
	/// is successful; otherwise, a result containing the original failure or any exception thrown
	/// by the mapping function.
	/// </returns>
	/// <exception cref="ArgumentNullException">
	/// Thrown when <paramref name="mapper"/> is <see langword="null"/>.
	/// </exception>
	public DbResult<TResult> MapAsync<TResult>(Func<T, ValueTask<TResult>> mapper) {
		ArgumentNullException.ThrowIfNull(mapper);

		static async Task<Result<TResult>> Impl(Task<Result<T>> task, Func<T, ValueTask<TResult>> mapper) {
			var result = await task.ConfigureAwait(false);

			if (!result.IsSuccess) {
				return Result<TResult>.Fail(result.Error!);
			}

			try {
				var mapped = await mapper(result.Value!).ConfigureAwait(false);
				return Result<TResult>.Success(mapped);
			} catch (Exception ex) {
				return Result<TResult>.Fail(ex);
			}
		}
		return new(context, Impl(resultTask, mapper));
	}

	#endregion

	#region Then

	/// <summary>
	/// Chains an arbitrary async operation that returns a <see cref="Result"/>.
	/// Use this as an escape hatch to integrate external async operations into the fluent chain.
	/// </summary>
	/// <remarks>
	/// If the current result is a failure, <paramref name="next"/> is not invoked and the failure is propagated.
	/// </remarks>
	/// <param name="next">The async operation to execute, receiving the current value.</param>
	public DbResult ThenAsync(Func<T, Task<Result>> next)
		=> new(context, resultTask.ThenAsyncTask(next));

	/// <summary>
	/// Chains an arbitrary async operation that returns a <see cref="Result{T}"/>.
	/// Use this as an escape hatch to integrate external async operations into the fluent chain.
	/// <remarks>
	/// If the current result is a failure, <paramref name="next"/> is not invoked and the failure is propagated.
	/// </remarks>
	/// </summary>
	/// <param name="next">The async operation to execute, receiving the current value.</param>
	public DbResult<TResult> ThenAsync<TResult>(Func<T, Task<Result<TResult>>> next)
		=> new(context, this.ThenAsyncCore(next));

	private async Task<Result<TResult>> ThenAsyncCore<TResult>(Func<T, Task<Result<TResult>>> next) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await next(result.Value).ConfigureAwait(false);
	}

	#endregion


	//========================= THEN INSERT =========================
	//===============================================================

	#region Then Insert

	/// <summary>
	/// Chains an INSERT operation after a successful result. The current value passes through unchanged.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertAsync(
		string sql,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAsyncCorePassThrough(sql, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation after a successful result. The current value passes through unchanged.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parameters">The parameters for the INSERT statement.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertAsync(
		string sql,
		object? parameters,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAsyncCorePassThroughWithParams(sql, parameters, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation after a successful result, using the previous value to build parameters.
	/// The current value passes through unchanged.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertAsync(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAsyncCorePassThroughWithFactory(sql, parametersFactory, uniqueConstraintMessage, foreignKeyMessage));


	private async Task<Result<T>> ThenInsertAsyncCorePassThrough(
		string sql,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		var insertResult = await context.InsertAsync(sql, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return result;
	}

	private async Task<Result<T>> ThenInsertAsyncCorePassThroughWithParams(
		string sql,
		object? parameters,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		var insertResult = await context.InsertAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return result;
	}

	private async Task<Result<T>> ThenInsertAsyncCorePassThroughWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		var insertResult = await context.InsertAsync(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return result;
	}

	#endregion

	#region Then Insert and Return

	/// <summary>
	/// Chains an INSERT operation that transforms the result after a successful insert.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value after successful insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenInsertAndReturnAsync<TResult>(
		string sql,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndReturnAsyncCore(sql, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation that transforms the result after a successful insert.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parameters">The parameters for the INSERT statement.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value after successful insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenInsertAndReturnAsync<TResult>(
		string sql,
		object? parameters,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndReturnAsyncCoreWithParams(sql, parameters, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT operation that transforms the result after a successful insert, using the previous value to build parameters.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value after successful insert.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenInsertAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndReturnAsyncCoreWithFactory(sql, parametersFactory, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<TResult>> ThenInsertAndReturnAsyncCore<TResult>(
		string sql,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var insertResult = await context.InsertAsync(sql, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return resultSelector(result.Value);
	}

	private async Task<Result<TResult>> ThenInsertAndReturnAsyncCoreWithParams<TResult>(
		string sql,
		object? parameters,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var insertResult = await context.InsertAsync(sql, parameters, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return resultSelector(result.Value);
	}

	private async Task<Result<TResult>> ThenInsertAndReturnAsyncCoreWithFactory<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var insertResult = await context.InsertAsync(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Insert-If (bool overloads)

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="when">A condition that determines whether to execute the insert; if false, the insert is skipped and the current value passes through.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAsync(
		string sql,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? this.ThenInsertAsync(sql, uniqueConstraintMessage, foreignKeyMessage)
			: this;

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result, using the previous value to build parameters.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="when">A condition that determines whether to execute the insert; if false, the insert is skipped and the current value passes through.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAsync(
		string sql,
		Func<T, object?> parametersFactory,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? this.ThenInsertAsync(sql, parametersFactory, uniqueConstraintMessage, foreignKeyMessage)
			: this;

	#endregion

	#region Then Insert-If (Func<T, bool> overload)

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="when">Predicate that determines whether to execute the insert; if false, the insert is skipped and the current value passes through.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAsync(
		string sql,
		Func<T, bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAsyncCoreNoParams(sql, when, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Conditionally chains an INSERT operation after a successful result, using the previous value to build parameters.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="when">Predicate that determines whether to execute the insert; if false, the insert is skipped and the current value passes through.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenInsertIfAsync(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAsyncCoreWithFactory(sql, parametersFactory, when, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<T>> ThenInsertIfAsyncCoreNoParams(
		string sql,
		Func<T, bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when(result.Value)) {
			return result;
		}
		var insertResult = await context.InsertAsync(sql, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return result;
	}

	private async Task<Result<T>> ThenInsertIfAsyncCoreWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when(result.Value)) {
			return result;
		}
		var insertResult = await context.InsertAsync(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return result;
	}

	#endregion

	#region Then Insert-If and Return (Func<T, bool> overload)

	#region Then Insert-If and Return (bool overload)

	/// <summary>
	/// Conditionally chains an INSERT operation that transforms the result after a successful insert, using the previous value to build parameters.
	/// If <paramref name="when"/> is false, the insert is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="when">A condition that determines whether to execute the insert; if false, the insert is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value, called after successful insert or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenInsertIfAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		bool when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAndReturnAsyncCore(sql, parametersFactory, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<TResult>> ThenInsertIfAndReturnAsyncCore<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		bool when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when) {
			return resultSelector(result.Value);
		}
		var insertResult = await context.InsertAsync(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	/// <summary>
	/// Conditionally chains an INSERT operation that transforms the result after a successful insert, using the previous value to build parameters.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="when">Predicate that determines whether to execute the insert; if false, the insert is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value, called after successful insert or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenInsertIfAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, bool> when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertIfAndReturnAsyncCore(sql, parametersFactory, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<TResult>> ThenInsertIfAndReturnAsyncCore<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, bool> when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when(result.Value)) {
			return resultSelector(result.Value);
		}
		var insertResult = await context.InsertAsync(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (insertResult.IsFailure) {
			return insertResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Insert and Return (entity shortcut)

	/// <summary>
	/// Chains an INSERT operation using the entity as both parameters and return value.
	/// The entity replaces the current value in the chain.
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
		=> this.ThenInsertAndReturnAsync(sql, entity, _ => entity, uniqueConstraintMessage, foreignKeyMessage);

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
		=> this.ThenInsertIfAndReturnAsync(sql, _ => entity, when, _ => entity, uniqueConstraintMessage, foreignKeyMessage);

	/// <summary>
	/// Conditionally chains an INSERT operation using the entity as both parameters and return value.
	/// If <paramref name="when"/> returns false, the insert is skipped and the chain continues with the entity.
	/// </summary>
	/// <typeparam name="TEntity">The type of the entity to insert and return.</typeparam>
	/// <param name="sql">The INSERT SQL statement.</param>
	/// <param name="entity">The entity used for parameters and returned on success or when skipped.</param>
	/// <param name="when">Predicate that determines whether to execute the insert; receives the current value.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TEntity> ThenInsertIfAndReturnAsync<TEntity>(
		string sql,
		TEntity entity,
		Func<T, bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist") where TEntity : class
		=> this.ThenInsertIfAndReturnAsync(sql, _ => entity, when, _ => entity, uniqueConstraintMessage, foreignKeyMessage);

	#endregion

	#region Then Insert and Get

	/// <summary>
	/// Chains an INSERT + SELECT operation that returns the selected row.
	/// </summary>
	/// <typeparam name="TNext">The type of the row returned by the SELECT.</typeparam>
	/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TNext> ThenInsertAndGetAsync<TNext>(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndGetAsyncCore<TNext>(sql, parametersFactory, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT + SELECT operation that returns the selected row, applying a mapping function.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="mapper">A function to transform the data row to the domain model.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TModel> ThenInsertAndGetAsync<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<TData, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndGetAsyncCore(sql, parametersFactory, mapper, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an INSERT + SELECT operation that returns an Optional, allowing the caller to handle the empty case.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="mapper">A function that receives an Optional and returns the final result.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TModel> ThenInsertAndGetOptionalAsync<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<Optional<TData>, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertAndGetOptionalAsyncCore(sql, parametersFactory, mapper, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<TNext>> ThenInsertAndGetAsyncCore<TNext>(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.InsertAndGetAsync<TNext>(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenInsertAndGetAsyncCore<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<TData, TModel> mapper,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.InsertAndGetAsync(sql, parametersFactory(result.Value), mapper, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenInsertAndGetOptionalAsyncCore<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<Optional<TData>, TModel> mapper,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.InsertAndGetOptionalAsync(sql, parametersFactory(result.Value), mapper, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	#endregion

	//========================= THEN UPDATE =========================
	//===============================================================

	#region Then Update

	/// <summary>
	/// Chains an UPDATE operation after a successful result. The current value passes through unchanged.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parameters">The parameters for the UPDATE statement.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenUpdateAsync(
		string sql,
		object? parameters,
		object key,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAsyncCorePassThrough(sql, parameters, key, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an UPDATE operation after a successful result, using the previous value to build parameters.
	/// The current value passes through unchanged.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenUpdateAsync(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAsyncCorePassThroughWithFactory(sql, parametersFactory, key, uniqueConstraintMessage, foreignKeyMessage));


	private async Task<Result<T>> ThenUpdateAsyncCorePassThrough(
		string sql,
		object? parameters,
		object key,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		var updateResult = await context.UpdateAsync(sql, parameters, key, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (updateResult.IsFailure) {
			return updateResult.Error;
		}
		return result;
	}

	private async Task<Result<T>> ThenUpdateAsyncCorePassThroughWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		var updateResult = await context.UpdateAsync(sql, parametersFactory(result.Value), key, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (updateResult.IsFailure) {
			return updateResult.Error;
		}
		return result;
	}

	#endregion

	#region Then Update and Return

	/// <summary>
	/// Chains an UPDATE operation that transforms the result after a successful update.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parameters">The parameters for the UPDATE statement.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value after successful update.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenUpdateAndReturnAsync<TResult>(
		string sql,
		object? parameters,
		object key,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAndReturnAsyncCore(sql, parameters, key, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an UPDATE operation that transforms the result after a successful update, using the previous value to build parameters.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value after successful update.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenUpdateAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAndReturnAsyncCoreWithFactory(sql, parametersFactory, key, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<TResult>> ThenUpdateAndReturnAsyncCore<TResult>(
		string sql,
		object? parameters,
		object key,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var updateResult = await context.UpdateAsync(sql, parameters, key, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (updateResult.IsFailure) {
			return updateResult.Error;
		}
		return resultSelector(result.Value);
	}

	private async Task<Result<TResult>> ThenUpdateAndReturnAsyncCoreWithFactory<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var updateResult = await context.UpdateAsync(sql, parametersFactory(result.Value), key, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (updateResult.IsFailure) {
			return updateResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Update-If (bool overload)

	/// <summary>
	/// Conditionally chains an UPDATE operation after a successful result, using the previous value to build parameters.
	/// If <paramref name="when"/> is false, the update is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">A condition that determines whether to execute the update; if false, the update is skipped and the current value passes through.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenUpdateIfAsync(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		bool when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> when
			? this.ThenUpdateAsync(sql, parametersFactory, key, uniqueConstraintMessage, foreignKeyMessage)
			: this;

	#endregion

	#region Then Update-If (Func<T, bool> overload)

	/// <summary>
	/// Conditionally chains an UPDATE operation after a successful result, using the previous value to build parameters.
	/// If <paramref name="when"/> returns false, the update is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">Predicate that determines whether to execute the update; if false, the update is skipped and the current value passes through.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenUpdateIfAsync(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateIfAsyncCoreWithFactory(sql, parametersFactory, key, when, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<T>> ThenUpdateIfAsyncCoreWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when(result.Value)) {
			return result;
		}
		var updateResult = await context.UpdateAsync(sql, parametersFactory(result.Value), key, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (updateResult.IsFailure) {
			return updateResult.Error;
		}
		return result;
	}

	#endregion

	#region Then Update-If and Return (bool overload)

	/// <summary>
	/// Conditionally chains an UPDATE operation that transforms the result after a successful update, using the previous value to build parameters.
	/// If <paramref name="when"/> is false, the update is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">A condition that determines whether to execute the update; if false, the update is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value, called after successful update or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenUpdateIfAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		bool when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<TResult>> ThenUpdateIfAndReturnAsyncCore<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		bool when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when) {
			return resultSelector(result.Value);
		}
		var updateResult = await context.UpdateAsync(sql, parametersFactory(result.Value), key, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (updateResult.IsFailure) {
			return updateResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Update-If and Return (Func<T, bool> overload)

	/// <summary>
	/// Conditionally chains an UPDATE operation that transforms the result after a successful update, using the previous value to build parameters.
	/// If <paramref name="when"/> returns false, the update is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The UPDATE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">Predicate that determines whether to execute the update; if false, the update is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value, called after successful update or when skipped.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenUpdateIfAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, uniqueConstraintMessage, foreignKeyMessage));


	private async Task<Result<TResult>> ThenUpdateIfAndReturnAsyncCore<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		Func<T, TResult> resultSelector,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when(result.Value)) {
			return resultSelector(result.Value);
		}
		var updateResult = await context.UpdateAsync(sql, parametersFactory(result.Value), key, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
		if (updateResult.IsFailure) {
			return updateResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Update and Get

	/// <summary>
	/// Chains an UPDATE + SELECT operation that returns the selected row.
	/// </summary>
	/// <typeparam name="TNext">The type of the row returned by the SELECT.</typeparam>
	/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="keyFactory">Factory to create the key for NotFoundException from the current value.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TNext> ThenUpdateAndGetAsync<TNext>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAndGetAsyncCore<TNext>(sql, parametersFactory, keyFactory, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an UPDATE + SELECT operation that returns the selected row, applying a mapping function.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="keyFactory">Factory to create the key for NotFoundException from the current value.</param>
	/// <param name="mapper">A function to transform the data row to the domain model.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TModel> ThenUpdateAndGetAsync<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		Func<TData, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAndGetAsyncCore(sql, parametersFactory, keyFactory, mapper, uniqueConstraintMessage, foreignKeyMessage));

	/// <summary>
	/// Chains an UPDATE + SELECT operation that returns an Optional, allowing the caller to handle the empty case.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="mapper">A function that receives an Optional and returns the final result.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TModel> ThenUpdateAndGetOptionalAsync<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<Optional<TData>, TModel> mapper,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateAndGetOptionalAsyncCore(sql, parametersFactory, mapper, uniqueConstraintMessage, foreignKeyMessage));

	private async Task<Result<TNext>> ThenUpdateAndGetAsyncCore<TNext>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.UpdateAndGetAsync<TNext>(sql, parametersFactory(result.Value), keyFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenUpdateAndGetAsyncCore<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		Func<TData, TModel> mapper,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.UpdateAndGetAsync(sql, parametersFactory(result.Value), keyFactory(result.Value), mapper, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenUpdateAndGetOptionalAsyncCore<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<Optional<TData>, TModel> mapper,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.UpdateAndGetOptionalAsync(sql, parametersFactory(result.Value), mapper, uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	#endregion


	//========================= THEN DELETE =========================
	//===============================================================

	#region Then Delete

	/// <summary>
	/// Chains a DELETE operation after a successful result. The current value passes through unchanged.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parameters">The parameters for the DELETE statement.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteAsync(
		string sql,
		object? parameters,
		object key,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAsyncCorePassThrough(sql, parameters, key, foreignKeyMessage));

	/// <summary>
	/// Chains a DELETE operation after a successful result, using the previous value to build parameters.
	/// The current value passes through unchanged.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteAsync(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAsyncCorePassThroughWithFactory(sql, parametersFactory, key, foreignKeyMessage));

	private async Task<Result<T>> ThenDeleteAsyncCorePassThrough(
		string sql,
		object? parameters,
		object key,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		var deleteResult = await context.DeleteAsync(sql, parameters, key, foreignKeyMessage).Result.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return result;
	}

	private async Task<Result<T>> ThenDeleteAsyncCorePassThroughWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		var deleteResult = await context.DeleteAsync(sql, parametersFactory(result.Value), key, foreignKeyMessage).Result.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return result;
	}

	#endregion

	#region Then Delete and Return

	/// <summary>
	/// Chains a DELETE operation that transforms the result after a successful delete.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parameters">The parameters for the DELETE statement.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value after successful delete.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenDeleteAndReturnAsync<TResult>(
		string sql,
		object? parameters,
		object key,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAndReturnAsyncCore(sql, parameters, key, resultSelector, foreignKeyMessage));

	/// <summary>
	/// Chains a DELETE operation that transforms the result after a successful delete, using the previous value to build parameters.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value after successful delete.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenDeleteAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAndReturnAsyncCoreWithFactory(sql, parametersFactory, key, resultSelector, foreignKeyMessage));

	private async Task<Result<TResult>> ThenDeleteAndReturnAsyncCore<TResult>(
		string sql,
		object? parameters,
		object key,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var deleteResult = await context.DeleteAsync(sql, parameters, key, foreignKeyMessage).Result.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector(result.Value);
	}

	private async Task<Result<TResult>> ThenDeleteAndReturnAsyncCoreWithFactory<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		var deleteResult = await context.DeleteAsync(sql, parametersFactory(result.Value), key, foreignKeyMessage).Result.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Delete-If (bool overload)

	/// <summary>
	/// Conditionally chains a DELETE operation after a successful result, using the previous value to build parameters.
	/// If <paramref name="when"/> is false, the delete is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">A condition that determines whether to execute the delete; if false, the delete is skipped and the current value passes through.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteIfAsync(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		bool when,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> when
			? this.ThenDeleteAsync(sql, parametersFactory, key, foreignKeyMessage)
			: this;

	#endregion

	#region Then Delete-If (Func<T, bool> overloads)

	/// <summary>
	/// Conditionally chains a DELETE operation after a successful result, using the previous value to build parameters.
	/// If <paramref name="when"/> returns false, the delete is skipped and the chain continues with the current value.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">Predicate that determines whether to execute the delete; if false, the delete is skipped and the current value passes through.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<T> ThenDeleteIfAsync(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteIfAsyncCoreWithFactory(sql, parametersFactory, key, when, foreignKeyMessage));

	private async Task<Result<T>> ThenDeleteIfAsyncCoreWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result;
		}
		if (!when(result.Value)) {
			return result;
		}
		var deleteResult = await context.DeleteAsync(sql, parametersFactory(result.Value), key, foreignKeyMessage).Result.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return result;
	}

	#endregion

	#region Then Delete-If and Return (bool overload)

	/// <summary>
	/// Conditionally chains a DELETE operation that transforms the result after a successful delete, using the previous value to build parameters.
	/// If <paramref name="when"/> is false, the delete is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">A condition that determines whether to execute the delete; if false, the delete is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value, called after successful delete or when skipped.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenDeleteIfAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		bool when,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, foreignKeyMessage));

	private async Task<Result<TResult>> ThenDeleteIfAndReturnAsyncCore<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		bool when,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when) {
			return resultSelector(result.Value);
		}
		var deleteResult = await context.DeleteAsync(sql, parametersFactory(result.Value), key, foreignKeyMessage).Result.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Delete-If and Return (Func<T, bool> overloads)

	/// <summary>
	/// Conditionally chains a DELETE operation that transforms the result after a successful delete, using the previous value to build parameters.
	/// If <paramref name="when"/> returns false, the delete is skipped and the chain continues with the result from <paramref name="resultSelector"/>.
	/// </summary>
	/// <param name="sql">The DELETE SQL statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="key">The key for not-found error messages.</param>
	/// <param name="when">Predicate that determines whether to execute the delete; if false, the delete is skipped.</param>
	/// <param name="resultSelector">Factory to create the result value from the current value, called after successful delete or when skipped.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TResult> ThenDeleteIfAndReturnAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteIfAndReturnAsyncCore(sql, parametersFactory, key, when, resultSelector, foreignKeyMessage));

	private async Task<Result<TResult>> ThenDeleteIfAndReturnAsyncCore<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object key,
		Func<T, bool> when,
		Func<T, TResult> resultSelector,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		if (!when(result.Value)) {
			return resultSelector(result.Value);
		}
		var deleteResult = await context.DeleteAsync(sql, parametersFactory(result.Value), key, foreignKeyMessage).Result.ConfigureAwait(false);
		if (deleteResult.IsFailure) {
			return deleteResult.Error;
		}
		return resultSelector(result.Value);
	}

	#endregion

	#region Then Delete and Get

	/// <summary>
	/// Chains a DELETE + OUTPUT operation that returns the deleted row.
	/// </summary>
	/// <typeparam name="TNext">The type of the row returned by the OUTPUT clause.</typeparam>
	/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="keyFactory">Factory to create the key for NotFoundException from the current value.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TNext> ThenDeleteAndGetAsync<TNext>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAndGetAsyncCore<TNext>(sql, parametersFactory, keyFactory, foreignKeyMessage));

	/// <summary>
	/// Chains a DELETE + OUTPUT operation that returns the deleted row, applying a mapping function.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="keyFactory">Factory to create the key for NotFoundException from the current value.</param>
	/// <param name="mapper">A function to transform the data row to the domain model.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TModel> ThenDeleteAndGetAsync<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		Func<TData, TModel> mapper,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAndGetAsyncCore(sql, parametersFactory, keyFactory, mapper, foreignKeyMessage));

	/// <summary>
	/// Chains a DELETE + OUTPUT operation that returns an Optional, allowing the caller to handle the empty case.
	/// </summary>
	/// <typeparam name="TData">The type of the row returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the object in the final result.</typeparam>
	/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="mapper">A function that receives an Optional and returns the final result.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<TModel> ThenDeleteAndGetOptionalAsync<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<Optional<TData>, TModel> mapper,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteAndGetOptionalAsyncCore(sql, parametersFactory, mapper, foreignKeyMessage));

	private async Task<Result<TNext>> ThenDeleteAndGetAsyncCore<TNext>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.DeleteAndGetAsync<TNext>(sql, parametersFactory(result.Value), keyFactory(result.Value), foreignKeyMessage).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenDeleteAndGetAsyncCore<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, object> keyFactory,
		Func<TData, TModel> mapper,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.DeleteAndGetAsync(sql, parametersFactory(result.Value), keyFactory(result.Value), mapper, foreignKeyMessage).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenDeleteAndGetOptionalAsyncCore<TData, TModel>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<Optional<TData>, TModel> mapper,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.DeleteAndGetOptionalAsync(sql, parametersFactory(result.Value), mapper, foreignKeyMessage).Result.ConfigureAwait(false);
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

	/// <summary>
	/// Chains an INSERT operation that returns the number of rows affected, using the previous value to build parameters.
	/// Use this when 0 rows is a valid outcome (e.g., conditional inserts).
	/// </summary>
	/// <param name="sql">The SQL INSERT statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<int> ThenInsertWithCountAsync(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenInsertWithCountAsyncCoreWithFactory(sql, parametersFactory, uniqueConstraintMessage, foreignKeyMessage));

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

	private async Task<Result<int>> ThenInsertWithCountAsyncCoreWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.InsertWithCountAsync(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
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

	/// <summary>
	/// Chains an UPDATE operation that returns the number of rows affected, using the previous value to build parameters.
	/// Use this when 0 rows is a valid outcome (e.g., "update if exists" patterns).
	/// </summary>
	/// <param name="sql">The SQL UPDATE statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="uniqueConstraintMessage">Error message for unique constraint violations.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<int> ThenUpdateWithCountAsync(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage = "Record already exists",
		string? foreignKeyMessage = "Referenced record does not exist")
		=> new(context, this.ThenUpdateWithCountAsyncCoreWithFactory(sql, parametersFactory, uniqueConstraintMessage, foreignKeyMessage));

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

	private async Task<Result<int>> ThenUpdateWithCountAsyncCoreWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		string uniqueConstraintMessage,
		string? foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.UpdateWithCountAsync(sql, parametersFactory(result.Value), uniqueConstraintMessage, foreignKeyMessage).Result.ConfigureAwait(false);
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

	/// <summary>
	/// Chains a DELETE operation that returns the number of rows affected, using the previous value to build parameters.
	/// Use this when 0 rows is a valid outcome (e.g., "delete if exists" patterns).
	/// </summary>
	/// <param name="sql">The SQL DELETE statement.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="foreignKeyMessage">Error message for foreign key violations.</param>
	public DbResult<int> ThenDeleteWithCountAsync(
		string sql,
		Func<T, object?> parametersFactory,
		string foreignKeyMessage = "Cannot delete, record is in use")
		=> new(context, this.ThenDeleteWithCountAsyncCoreWithFactory(sql, parametersFactory, foreignKeyMessage));

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

	private async Task<Result<int>> ThenDeleteWithCountAsyncCoreWithFactory(
		string sql,
		Func<T, object?> parametersFactory,
		string foreignKeyMessage) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.DeleteWithCountAsync(sql, parametersFactory(result.Value), foreignKeyMessage).Result.ConfigureAwait(false);
	}

	#endregion


	//========================= THEN GET/QUERY =========================
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
	/// Chains a GET operation after a successful result, using the previous value to build parameters.
	/// </summary>
	public DbResult<TResult> ThenGetAsync<TResult>(string sql, Func<T, object?> parametersFactory, object key)
		=> new(context, this.ThenGetAsyncCoreWithFactory<TResult>(sql, parametersFactory, key));

	/// <summary>
	/// Chains a GET operation with mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetAsync<TData, TModel>(string sql, object key, Func<TData, TModel> mapper)
		=> new(context, this.ThenGetAsyncCoreWithMapper(sql, null, key, mapper));

	/// <summary>
	/// Chains a GET operation with parameters and mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetAsync<TData, TModel>(string sql, object? parameters, object key, Func<TData, TModel> mapper)
		=> new(context, this.ThenGetAsyncCoreWithMapper(sql, parameters, key, mapper));

	/// <summary>
	/// Chains a GET operation with mapping after a successful result, using the previous value to build parameters.
	/// </summary>
	public DbResult<TModel> ThenGetAsync<TData, TModel>(string sql, Func<T, object?> parametersFactory, object key, Func<TData, TModel> mapper)
		=> new(context, this.ThenGetAsyncCoreWithMapperAndFactory(sql, parametersFactory, key, mapper));

	private async Task<Result<TResult>> ThenGetAsyncCore<TResult>(string sql, object? parameters, object key) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.GetAsync<TResult>(sql, parameters, key).Result.ConfigureAwait(false);
	}

	private async Task<Result<TResult>> ThenGetAsyncCoreWithFactory<TResult>(string sql, Func<T, object?> parametersFactory, object key) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetAsync<TResult>(sql, parametersFactory(result.Value), key).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenGetAsyncCoreWithMapper<TData, TModel>(string sql, object? parameters, object key, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetAsync(sql, parameters, key, mapper).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenGetAsyncCoreWithMapperAndFactory<TData, TModel>(string sql, Func<T, object?> parametersFactory, object key, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetAsync(sql, parametersFactory(result.Value), key, mapper).Result.ConfigureAwait(false);
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
	/// Chains a GET scalar operation after a successful result, using the previous value to build parameters.
	/// </summary>
	public DbResult<TResult> ThenGetScalarAsync<TResult>(string sql, Func<T, object?> parametersFactory)
		=> new(context, this.ThenGetScalarAsyncCoreWithFactory<TResult>(sql, parametersFactory));

	/// <summary>
	/// Chains a GET scalar operation with mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetScalarAsync<TData, TModel>(string sql, Func<TData?, TModel> mapper)
		=> new(context, this.ThenGetScalarAsyncCoreWithMapper(sql, null, mapper));

	/// <summary>
	/// Chains a GET scalar operation with parameters and mapping after a successful result.
	/// </summary>
	public DbResult<TModel> ThenGetScalarAsync<TData, TModel>(string sql, object? parameters, Func<TData?, TModel> mapper)
		=> new(context, this.ThenGetScalarAsyncCoreWithMapper(sql, parameters, mapper));

	/// <summary>
	/// Chains a GET scalar operation with mapping after a successful result, using the previous value to build parameters.
	/// </summary>
	public DbResult<TModel> ThenGetScalarAsync<TData, TModel>(string sql, Func<T, object?> parametersFactory, Func<TData?, TModel> mapper)
		=> new(context, this.ThenGetScalarAsyncCoreWithMapperAndFactory(sql, parametersFactory, mapper));

	private async Task<Result<TResult>> ThenGetScalarAsyncCore<TResult>(string sql, object? parameters) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetScalarAsync<TResult>(sql, parameters).Result.ConfigureAwait(false);
	}

	private async Task<Result<TResult>> ThenGetScalarAsyncCoreWithFactory<TResult>(string sql, Func<T, object?> parametersFactory) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetScalarAsync<TResult>(sql, parametersFactory(result.Value)).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenGetScalarAsyncCoreWithMapper<TData, TModel>(string sql, object? parameters, Func<TData?, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetScalarAsync(sql, parameters, mapper).Result.ConfigureAwait(false);
	}

	private async Task<Result<TModel>> ThenGetScalarAsyncCoreWithMapperAndFactory<TData, TModel>(string sql, Func<T, object?> parametersFactory, Func<TData?, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetScalarAsync(sql, parametersFactory(result.Value), mapper).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then QueryAny

	/// <summary>
	/// Chains a QueryAny operation after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TResult>> ThenQueryAnyAsync<TResult>(string sql)
		=> new(context, this.ThenQueryAnyAsyncCore<TResult>(sql, null));

	/// <summary>
	/// Chains a QueryAny operation after a successful result, using the previous value to build parameters.
	/// </summary>
	public DbResult<IReadOnlyList<TResult>> ThenQueryAnyAsync<TResult>(string sql, Func<T, object?> parametersFactory)
		=> new(context, this.ThenQueryAnyAsyncCoreWithFactory<TResult>(sql, parametersFactory));

	/// <summary>
	/// Chains a QueryAny operation with parameters after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TResult>> ThenQueryAnyAsync<TResult>(string sql, object? parameters)
		=> new(context, this.ThenQueryAnyAsyncCore<TResult>(sql, parameters));

	/// <summary>
	/// Chains a QueryAny operation with mapping after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TModel>> ThenQueryAnyAsync<TData, TModel>(string sql, Func<TData, TModel> mapper)
		=> new(context, this.ThenQueryAnyAsyncCoreWithMapper(sql, null, mapper));

	/// <summary>
	/// Chains a QueryAny operation with parameters and mapping after a successful result.
	/// </summary>
	public DbResult<IReadOnlyList<TModel>> ThenQueryAnyAsync<TData, TModel>(string sql, object? parameters, Func<TData, TModel> mapper)
		=> new(context, this.ThenQueryAnyAsyncCoreWithMapper(sql, parameters, mapper));

	/// <summary>
	/// Chains a QueryAny operation with mapping after a successful result, using the previous value to build parameters.
	/// </summary>
	public DbResult<IReadOnlyList<TModel>> ThenQueryAnyAsync<TData, TModel>(string sql, Func<T, object?> parametersFactory, Func<TData, TModel> mapper)
		=> new(context, this.ThenQueryAnyAsyncCoreWithMapperAndFactory(sql, parametersFactory, mapper));

	private async Task<Result<IReadOnlyList<TResult>>> ThenQueryAnyAsyncCore<TResult>(string sql, object? parameters) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.QueryAnyAsync<TResult>(sql, parameters).Result.ConfigureAwait(false);
	}

	private async Task<Result<IReadOnlyList<TResult>>> ThenQueryAnyAsyncCoreWithFactory<TResult>(string sql, Func<T, object?> parametersFactory) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.QueryAnyAsync<TResult>(sql, parametersFactory(result.Value)).Result.ConfigureAwait(false);
	}

	private async Task<Result<IReadOnlyList<TModel>>> ThenQueryAnyAsyncCoreWithMapper<TData, TModel>(string sql, object? parameters, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.QueryAnyAsync(sql, parameters, mapper).Result.ConfigureAwait(false);
	}

	private async Task<Result<IReadOnlyList<TModel>>> ThenQueryAnyAsyncCoreWithMapperAndFactory<TData, TModel>(string sql, Func<T, object?> parametersFactory, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.QueryAnyAsync(sql, parametersFactory(result.Value), mapper).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then GetPaged

	/// <summary>
	/// Chains a GetPaged operation after a successful result.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters.
	/// The parameters object must include <c>PageSize</c> and <c>Page</c> properties.
	/// </para>
	/// </remarks>
	/// <typeparam name="TResult">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL batch to execute.</param>
	/// <param name="parameters">An object containing the parameters including PageSize and Page properties.</param>
	public DbResult<PagedResult<TResult>> ThenGetPagedAsync<TResult>(string sql, object parameters)
		=> new(context, this.ThenGetPagedAsyncCore<TResult>(sql, parameters));

	/// <summary>
	/// Chains a GetPaged operation after a successful result, using a factory to create the parameters object.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters.
	/// The parameters object must include <c>PageSize</c> and <c>Page</c> properties.
	/// </para>
	/// </remarks>
	/// <typeparam name="TResult">The type of the elements to be returned.</typeparam>
	/// <param name="sql">The SQL batch to execute.</param>
	/// <param name="parametersFactory">Factory to create parameters (including PageSize and Page) from the current value.</param>
	public DbResult<PagedResult<TResult>> ThenGetPagedAsync<TResult>(string sql, Func<T, object> parametersFactory)
		=> new(context, this.ThenGetPagedAsyncCoreWithFactory<TResult>(sql, parametersFactory));

	/// <summary>
	/// Chains a GetPaged operation with mapping after a successful result.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters.
	/// The parameters object must include <c>PageSize</c> and <c>Page</c> properties.
	/// </para>
	/// </remarks>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final paged result.</typeparam>
	/// <param name="sql">The SQL batch to execute.</param>
	/// <param name="parameters">An object containing the parameters including PageSize and Page properties.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	public DbResult<PagedResult<TModel>> ThenGetPagedAsync<TData, TModel>(string sql, object parameters, Func<TData, TModel> mapper)
		=> new(context, this.ThenGetPagedAsyncCore<TData, TModel>(sql, parameters, mapper));

	/// <summary>
	/// Chains a GetPaged operation with mapping after a successful result, using a factory to create the parameters object.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
	/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
	/// appended automatically.
	/// </para>
	/// <para>
	/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters.
	/// The parameters object must include <c>PageSize</c> and <c>Page</c> properties.
	/// </para>
	/// </remarks>
	/// <typeparam name="TData">The type of the elements returned by the SQL query.</typeparam>
	/// <typeparam name="TModel">The type of the elements in the final paged result.</typeparam>
	/// <param name="sql">The SQL batch to execute.</param>
	/// <param name="parametersFactory">Factory to create parameters (including PageSize and Page) from the current value.</param>
	/// <param name="mapper">A function to transform each data item to the domain model.</param>
	public DbResult<PagedResult<TModel>> ThenGetPagedAsync<TData, TModel>(string sql, Func<T, object> parametersFactory, Func<TData, TModel> mapper)
		=> new(context, this.ThenGetPagedAsyncCoreWithFactory<TData, TModel>(sql, parametersFactory, mapper));

	private async Task<Result<PagedResult<TResult>>> ThenGetPagedAsyncCore<TResult>(string sql, object parameters) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetPagedAsync<TResult>(sql, parameters).Result.ConfigureAwait(false);
	}

	private async Task<Result<PagedResult<TResult>>> ThenGetPagedAsyncCoreWithFactory<TResult>(string sql, Func<T, object> parametersFactory) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		var parameters = parametersFactory(result.Value);
		return await context.GetPagedAsync<TResult>(sql, parameters).Result.ConfigureAwait(false);
	}

	private async Task<Result<PagedResult<TModel>>> ThenGetPagedAsyncCore<TData, TModel>(string sql, object parameters, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		return await context.GetPagedAsync<TData, TModel>(sql, parameters, mapper).Result.ConfigureAwait(false);
	}

	private async Task<Result<PagedResult<TModel>>> ThenGetPagedAsyncCoreWithFactory<TData, TModel>(string sql, Func<T, object> parametersFactory, Func<TData, TModel> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}

		var parameters = parametersFactory(result.Value);
		return await context.GetPagedAsync<TData, TModel>(sql, parameters, mapper).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then MultipleGet

	/// <summary>
	/// Chains a multiple-result GET operation after a successful result.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="keys">The keys used to identify the resource for the <see cref="Exceptions.NotFoundException"/>.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the mapped value.</param>
	public DbResult<TResult> ThenMultipleGetAsync<TResult>(
		string sql,
		object[] keys,
		Func<T, IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetAsyncCore(sql, null, keys, mapper));

	/// <summary>
	/// Chains a multiple-result GET operation with parameters after a successful result.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters for the query.</param>
	/// <param name="keys">The keys used to identify the resource for the <see cref="Exceptions.NotFoundException"/>.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the mapped value.</param>
	public DbResult<TResult> ThenMultipleGetAsync<TResult>(
		string sql,
		object? parameters,
		object[] keys,
		Func<T, IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetAsyncCore(sql, parameters, keys, mapper));

	/// <summary>
	/// Chains a multiple-result GET operation after a successful result, using the previous value to build parameters.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="keys">The keys used to identify the resource for the <see cref="Exceptions.NotFoundException"/>.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the mapped value.</param>
	public DbResult<TResult> ThenMultipleGetAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object[] keys,
		Func<T, IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetAsyncCoreWithFactory(sql, parametersFactory, keys, mapper));

	private async Task<Result<TResult>> ThenMultipleGetAsyncCore<TResult>(
		string sql,
		object? parameters,
		object[] keys,
		Func<T, IMultipleResult, Task<TResult?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleGetAsync(sql, parameters, keys, reader => mapper(result.Value, reader)).Result.ConfigureAwait(false);
	}

	private async Task<Result<TResult>> ThenMultipleGetAsyncCoreWithFactory<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		object[] keys,
		Func<T, IMultipleResult, Task<TResult?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleGetAsync(sql, parametersFactory(result.Value), keys, reader => mapper(result.Value, reader)).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then MultipleGetOptional

	/// <summary>
	/// Chains a multiple-result optional GET operation after a successful result.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the mapped value.</param>
	public DbResult<Optional<TResult>> ThenMultipleGetOptionalAsync<TResult>(
		string sql,
		Func<T, IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetOptionalAsyncCore(sql, null, mapper));

	/// <summary>
	/// Chains a multiple-result optional GET operation with parameters after a successful result.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters for the query.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the mapped value.</param>
	public DbResult<Optional<TResult>> ThenMultipleGetOptionalAsync<TResult>(
		string sql,
		object? parameters,
		Func<T, IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetOptionalAsyncCore(sql, parameters, mapper));

	/// <summary>
	/// Chains a multiple-result optional GET operation after a successful result, using the previous value to build parameters.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the object to be returned.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the mapped value.</param>
	public DbResult<Optional<TResult>> ThenMultipleGetOptionalAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, IMultipleResult, Task<TResult?>> mapper)
		=> new(context, this.ThenMultipleGetOptionalAsyncCoreWithFactory(sql, parametersFactory, mapper));

	private async Task<Result<Optional<TResult>>> ThenMultipleGetOptionalAsyncCore<TResult>(
		string sql,
		object? parameters,
		Func<T, IMultipleResult, Task<TResult?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleGetOptionalAsync(sql, parameters, reader => mapper(result.Value, reader)).Result.ConfigureAwait(false);
	}

	private async Task<Result<Optional<TResult>>> ThenMultipleGetOptionalAsyncCoreWithFactory<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, IMultipleResult, Task<TResult?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleGetOptionalAsync(sql, parametersFactory(result.Value), reader => mapper(result.Value, reader)).Result.ConfigureAwait(false);
	}

	#endregion

	#region Then MultipleQueryAny

	/// <summary>
	/// Chains a multiple-result query operation after a successful result.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the elements in the returned list.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the list.</param>
	public DbResult<IReadOnlyList<TResult>> ThenMultipleQueryAnyAsync<TResult>(
		string sql,
		Func<T, IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper)
		=> new(context, this.ThenMultipleQueryAnyAsyncCore(sql, null, mapper));

	/// <summary>
	/// Chains a multiple-result query operation with parameters after a successful result.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the elements in the returned list.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parameters">The parameters for the query.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the list.</param>
	public DbResult<IReadOnlyList<TResult>> ThenMultipleQueryAnyAsync<TResult>(
		string sql,
		object? parameters,
		Func<T, IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper)
		=> new(context, this.ThenMultipleQueryAnyAsyncCore(sql, parameters, mapper));

	/// <summary>
	/// Chains a multiple-result query operation after a successful result, using the previous value to build parameters.
	/// The mapper receives both the current value and the multiple result reader.
	/// </summary>
	/// <typeparam name="TResult">The type of the elements in the returned list.</typeparam>
	/// <param name="sql">The SQL query to execute.</param>
	/// <param name="parametersFactory">Factory to create parameters from the current value.</param>
	/// <param name="mapper">An async function that receives the current value and reads from the <see cref="IMultipleResult"/> to return the list.</param>
	public DbResult<IReadOnlyList<TResult>> ThenMultipleQueryAnyAsync<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper)
		=> new(context, this.ThenMultipleQueryAnyAsyncCoreWithFactory(sql, parametersFactory, mapper));

	private async Task<Result<IReadOnlyList<TResult>>> ThenMultipleQueryAnyAsyncCore<TResult>(
		string sql,
		object? parameters,
		Func<T, IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleQueryAnyAsync(sql, parameters, reader => mapper(result.Value, reader)).Result.ConfigureAwait(false);
	}

	private async Task<Result<IReadOnlyList<TResult>>> ThenMultipleQueryAnyAsyncCoreWithFactory<TResult>(
		string sql,
		Func<T, object?> parametersFactory,
		Func<T, IMultipleResult, Task<IReadOnlyList<TResult>?>> mapper) {
		var result = await resultTask.ConfigureAwait(false);
		if (result.IsFailure) {
			return result.Error;
		}
		return await context.MultipleQueryAnyAsync(sql, parametersFactory(result.Value), reader => mapper(result.Value, reader)).Result.ConfigureAwait(false);
	}

	#endregion

}
