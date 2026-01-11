namespace Cirreum.Persistence;

using Cirreum.Exceptions;
using System;

/// <summary>
/// Provides extension methods for executing asynchronous operations and transactions using an <see
/// cref="ISqlConnectionFactory"/> instance.
/// </summary>
/// <remarks>These extension methods simplify the process of managing database connections and transactions by
/// ensuring proper resource handling and encapsulating common patterns for asynchronous execution. All methods ensure
/// that connections are opened, disposed, and, when applicable, that transactions are committed or rolled back
/// appropriately. Callers are responsible for handling the results and any errors encapsulated within the returned <see
/// cref="Result"/> or <see cref="Result{T}"/> objects.</remarks>
public static class SqlConnectionFactoryExtensions {

	extension(ISqlConnectionFactory factory) {

		#region EXECUTE ASYNC

		/// <summary>
		/// Creates a database connection using the configured factory and executes the supplied operation.
		/// </summary>
		/// <param name="operations">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is observed during connection creation and passed to the executed operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result"/> produced by <paramref name="operations"/>.
		/// </returns>
		/// <remarks>
		/// <para>
		/// This method is responsible for creating and disposing the underlying database connection.
		/// </para>
		/// <para>
		/// All execution behavior—including <see cref="DbContext"/> usage, exception handling, and result propagation—
		/// is delegated to the underlying connection implementation.
		/// </para>
		/// </remarks>
		public async Task<Result> ExecuteAsync(
			Func<DbContext, Task<Result>> operations,
			CancellationToken cancellationToken = default) {

			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.ExecuteAsync(operations, cancellationToken);
		}

		/// <summary>
		/// Creates a database connection using the configured factory and executes the supplied operation,
		/// returning a typed result.
		/// </summary>
		/// <typeparam name="T">The value type carried by the returned <see cref="Result{T}"/>.</typeparam>
		/// <param name="operations">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result{T}"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is observed during connection creation and passed to the executed operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result{T}"/> produced by <paramref name="operations"/>.
		/// </returns>
		/// <remarks>
		/// <para>
		/// This method is responsible for creating and disposing the underlying database connection.
		/// </para>
		/// <para>
		/// All execution behavior—including <see cref="DbContext"/> usage, exception handling, and result propagation—
		/// is delegated to the underlying connection implementation.
		/// </para>
		/// </remarks>
		public async Task<Result<T>> ExecuteAsync<T>(
			Func<DbContext, Task<Result<T>>> operations,
			CancellationToken cancellationToken = default) {

			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.ExecuteAsync(operations, cancellationToken);
		}

		#endregion

		#region EXECUTE TRANSACTION ASYNC

		/// <summary>
		/// Creates a database connection using the configured factory and executes the supplied operation
		/// within a transaction.
		/// </summary>
		/// <param name="operations">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is observed during connection creation and passed to the executed operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result"/> produced by <paramref name="operations"/>.
		/// </returns>
		/// <remarks>
		/// <para>
		/// This method is responsible for creating and disposing the underlying database connection.
		/// </para>
		/// <para>
		/// Transaction behavior—including begin, commit, rollback, and exception handling—is delegated to the
		/// underlying connection implementation.
		/// </para>
		/// </remarks>
		public async Task<Result> ExecuteTransactionAsync(
			Func<DbContext, Task<Result>> operations,
			CancellationToken cancellationToken = default) {

			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.ExecuteTransactionAsync(operations, cancellationToken);
		}

		/// <summary>
		/// Creates a database connection using the configured factory and executes the supplied operation
		/// within a transaction, returning a typed result.
		/// </summary>
		/// <typeparam name="T">The value type carried by the returned <see cref="Result{T}"/>.</typeparam>
		/// <param name="operations">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result{T}"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is observed during connection creation and passed to the executed operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result{T}"/> produced by <paramref name="operations"/>.
		/// </returns>
		/// <remarks>
		/// <para>
		/// This method is responsible for creating and disposing the underlying database connection.
		/// </para>
		/// <para>
		/// Transaction behavior—including begin, commit, rollback, and exception handling—is delegated to the
		/// underlying connection implementation.
		/// </para>
		/// </remarks>
		public async Task<Result<T>> ExecuteTransactionAsync<T>(
			Func<DbContext, Task<Result<T>>> operations,
			CancellationToken cancellationToken = default) {

			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.ExecuteTransactionAsync(operations, cancellationToken);
		}

		#endregion

		#region GET

		/// <summary>
		/// Retrieves a single entity by executing the specified SQL query asynchronously and returns the result wrapped in a <see cref="Result{T}"/>
		/// object.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should be a statement that returns a single row.</param>
		/// <param name="key">A key associated with the query result, used to identify or correlate the returned value.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// queried value, or a NotFound result if no row is found.</returns>
		public async Task<Result<T>> GetAsync<T>(
			string sql,
			object key,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetAsync<T>(sql, null, key, null, cancellationToken);
		}

		/// <summary>
		/// Retrieves a single entity by executing the specified SQL query asynchronously and returns the result wrapped in a <see cref="Result{T}"/>
		/// object.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should be a statement that returns a single row.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are
		/// required.</param>
		/// <param name="key">A key associated with the query result, used to identify or correlate the returned value.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// queried value, or a NotFound result if no row is found.</returns>
		public async Task<Result<T>> GetAsync<T>(
			string sql,
			object? parameters,
			object key,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetAsync<T>(
				sql,
				parameters,
				key,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Retrieves a single entity by executing the specified SQL query asynchronously and returns the result wrapped in a <see cref="Result{T}"/>
		/// object, applying a mapping function to transform the item.
		/// </summary>
		/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should be a statement that returns a single row.</param>
		/// <param name="key">A key associated with the query result, used to identify or correlate the returned value.</param>
		/// <param name="mapper">A function to transform the data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if no row is found.</returns>
		public async Task<Result<TModel>> GetAsync<TData, TModel>(
			string sql,
			object key,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetAsync(sql, null, key, mapper, null, cancellationToken);
		}

		/// <summary>
		/// Retrieves a single entity by executing the specified SQL query asynchronously and returns the result wrapped in a <see cref="Result{T}"/>
		/// object, applying a mapping function to transform the item.
		/// </summary>
		/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should be a statement that returns a single row.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">A key associated with the query result, used to identify or correlate the returned value.</param>
		/// <param name="mapper">A function to transform the data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if no row is found.</returns>
		public async Task<Result<TModel>> GetAsync<TData, TModel>(
			string sql,
			object? parameters,
			object key,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetAsync(
				sql,
				parameters,
				key,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}


		#endregion

		#region GET OPTIONAL

		/// <summary>
		/// Retrieves zero or one entity by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the value if one row was found.</returns>
		public async Task<Result<Optional<T>>> GetOptionalAsync<T>(
			string sql,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetOptionalAsync<T>(sql, null, null, cancellationToken);
		}

		/// <summary>
		/// Retrieves zero or one entity by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the value if one row was found.</returns>
		public async Task<Result<Optional<T>>> GetOptionalAsync<T>(
			string sql,
			object? parameters,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetOptionalAsync<T>(
				sql,
				parameters,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Retrieves zero or one entity by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>,
		/// applying a mapping function to transform the item if present.
		/// </summary>
		/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
		/// <param name="mapper">A function to transform the data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped value if one row was found.</returns>
		public async Task<Result<Optional<TModel>>> GetOptionalAsync<TData, TModel>(
			string sql,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetOptionalAsync(sql, null, mapper, null, cancellationToken);
		}

		/// <summary>
		/// Retrieves zero or one entity by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>,
		/// applying a mapping function to transform the item if present.
		/// </summary>
		/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function to transform the data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped value if one row was found.</returns>
		public async Task<Result<Optional<TModel>>> GetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetOptionalAsync(
				sql,
				parameters,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region GET SCALAR

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the first column of the first row
		/// in the result set, wrapped in a <see cref="Result{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method is useful for queries that return a single scalar value, such as COUNT, SUM, MAX,
		/// or selecting a single column value.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.GetScalarAsync&lt;int&gt;(
		///     "SELECT COUNT(*) FROM Orders",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the scalar value to return.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return a single scalar value.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the scalar value.</returns>
		public async Task<Result<T>> GetScalarAsync<T>(
			string sql,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetScalarAsync<T>(sql, null, null, cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the first column of the first row
		/// in the result set, wrapped in a <see cref="Result{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method is useful for queries that return a single scalar value, such as COUNT, SUM, MAX,
		/// or selecting a single column value.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.GetScalarAsync&lt;int&gt;(
		///     "SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId",
		///     new { query.CustomerId },
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the scalar value to return.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return a single scalar value.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the scalar value.</returns>
		public async Task<Result<T>> GetScalarAsync<T>(
			string sql,
			object? parameters,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetScalarAsync<T>(
				sql,
				parameters,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the first column of the first row
		/// in the result set, applying a mapping function to transform the value, wrapped in a <see cref="Result{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method is useful for queries that return a single scalar value that needs transformation,
		/// such as converting database types to domain types.
		/// </para>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.GetScalarAsync&lt;int, OrderCount&gt;(
		///     "SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId",
		///     new { query.CustomerId },
		///     count =&gt; new OrderCount(count),
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="TData">The type of the scalar value returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the value in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should return a single scalar value.</param>
		/// <param name="mapper">A function to transform the data value to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped scalar value.</returns>
		public async Task<Result<TModel>> GetScalarAsync<TData, TModel>(
			string sql,
			Func<TData?, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetScalarAsync(
				sql,
				null,
				mapper,
				null,
				cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the first column of the first row
		/// in the result set, applying a mapping function to transform the value, wrapped in a <see cref="Result{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method is useful for queries that return a single scalar value that needs transformation,
		/// such as converting database types to domain types.
		/// </para>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.GetScalarAsync&lt;int, OrderCount&gt;(
		///     "SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId",
		///     new { query.CustomerId },
		///     count =&gt; new OrderCount(count),
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="TData">The type of the scalar value returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the value in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should return a single scalar value.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function to transform the data value to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped scalar value.</returns>
		public async Task<Result<TModel>> GetScalarAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData?, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetScalarAsync(
				sql,
				parameters,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region QUERY OPTIONAL

		/// <summary>
		/// Retrieves the first row (if any) by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the first row's value.</returns>
		public async Task<Result<Optional<T>>> QueryOptionalAsync<T>(
			string sql,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryOptionalAsync<T>(sql, null, null, cancellationToken);
		}

		/// <summary>
		/// Retrieves the first row (if any) by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the first row's value.</returns>
		public async Task<Result<Optional<T>>> QueryOptionalAsync<T>(
			string sql,
			object? parameters,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryOptionalAsync<T>(
				sql,
				parameters,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Retrieves the first row (if any) by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>,
		/// applying a mapping function to transform the item if present.
		/// </summary>
		/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute.</param>
		/// <param name="mapper">A function to transform the data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped first row's value.</returns>
		public async Task<Result<Optional<TModel>>> QueryOptionalAsync<TData, TModel>(
			string sql,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryOptionalAsync(sql, null, mapper, null, cancellationToken);
		}

		/// <summary>
		/// Retrieves the first row (if any) by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>,
		/// applying a mapping function to transform the item if present.
		/// </summary>
		/// <typeparam name="TData">The type of the object returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function to transform the data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped first row's value.</returns>
		public async Task<Result<Optional<TModel>>> QueryOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryOptionalAsync(
				sql,
				parameters,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region QUERY ANY

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result.
		/// </summary>
		/// <typeparam name="T">The type of the elements to be returned in the result list.</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<T>>> QueryAnyAsync<T>(
			string sql,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryAnyAsync<T>(sql, null, null, cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result.
		/// </summary>
		/// <typeparam name="T">The type of the elements to be returned in the result list.</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or null if no parameters are required.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<T>>> QueryAnyAsync<T>(
			string sql,
			object? parameters,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryAnyAsync<T>(
				sql,
				parameters,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result, applying a mapping function to transform each item.
		/// </summary>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final result list (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of mapped items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<TModel>>> QueryAnyAsync<TData, TModel>(
			string sql,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryAnyAsync(sql, null, mapper, null, cancellationToken);
		}
		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result, applying a mapping function to transform each item.
		/// </summary>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final result list (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or null if no parameters are required.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of mapped items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<TModel>>> QueryAnyAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryAnyAsync(
				sql,
				parameters,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region QUERY PAGED

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a paginated result wrapped in a
		/// <see cref="Result{T}"/> object.
		/// </summary>
		/// <remarks>
		/// This method expects an SQL query that includes OFFSET/FETCH clauses for pagination. The total count must be
		/// obtained separately before calling this method, typically via a COUNT(*) query.
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the paged result.</typeparam>
		/// <param name="sql">The SQL query to execute against the database. Should include OFFSET/FETCH for pagination.</param>
		/// <param name="totalCount">The total number of records matching the query criteria (before pagination).</param>
		/// <param name="pageSize">The number of items per page.</param>
		/// <param name="page">The current page number (1-based).</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the queried items and pagination metadata.</returns>
		public async Task<Result<PagedResult<T>>> QueryPagedAsync<T>(
			string sql,
			int totalCount,
			int pageSize,
			int page,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryPagedAsync<T>(
				sql,
				null,
				totalCount,
				pageSize,
				page,
				null,
				cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a paginated result wrapped in a
		/// <see cref="Result{T}"/> object.
		/// </summary>
		/// <remarks>
		/// This method expects an SQL query that includes OFFSET/FETCH clauses for pagination. The total count must be
		/// obtained separately before calling this method, typically via a COUNT(*) query.
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the paged result.</typeparam>
		/// <param name="sql">The SQL query to execute against the database. Should include OFFSET/FETCH for pagination.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="totalCount">The total number of records matching the query criteria (before pagination).</param>
		/// <param name="pageSize">The number of items per page.</param>
		/// <param name="page">The current page number (1-based).</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the queried items and pagination metadata.</returns>
		public async Task<Result<PagedResult<T>>> QueryPagedAsync<T>(
			string sql,
			object? parameters,
			int totalCount,
			int pageSize,
			int page,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryPagedAsync<T>(
				sql,
				parameters,
				totalCount,
				pageSize,
				page,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a paginated result wrapped in a
		/// <see cref="Result{T}"/> object, applying a mapping function to transform each item.
		/// </summary>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final paged result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute against the database. Should include OFFSET/FETCH for pagination.</param>
		/// <param name="totalCount">The total number of records matching the query criteria (before pagination).</param>
		/// <param name="pageSize">The number of items per page.</param>
		/// <param name="page">The current page number (1-based).</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the mapped items and pagination metadata.</returns>
		public async Task<Result<PagedResult<TModel>>> QueryPagedAsync<TData, TModel>(
			string sql,
			int totalCount,
			int pageSize,
			int page,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryPagedAsync(
				sql,
				null,
				totalCount,
				pageSize,
				page,
				mapper,
				null,
				cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a paginated result wrapped in a
		/// <see cref="Result{T}"/> object, applying a mapping function to transform each item.
		/// </summary>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final paged result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute against the database. Should include OFFSET/FETCH for pagination.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="totalCount">The total number of records matching the query criteria (before pagination).</param>
		/// <param name="pageSize">The number of items per page.</param>
		/// <param name="page">The current page number (1-based).</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the mapped items and pagination metadata.</returns>
		public async Task<Result<PagedResult<TModel>>> QueryPagedAsync<TData, TModel>(
			string sql,
			object? parameters,
			int totalCount,
			int pageSize,
			int page,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryPagedAsync(
				sql,
				parameters,
				totalCount,
				pageSize,
				page,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region GET PAGED

		/// <summary>
		/// Executes a SQL batch containing a count query and a data query, returning a paginated result.
		/// </summary>
		/// <remarks>
		/// <para>
		/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
		/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
		/// appended automatically.
		/// </para>
		/// <para>
		/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters. Your SQL can reference
		/// these directly, or omit the OFFSET clause entirely to have it appended automatically.
		/// </para>
		/// <example>
		/// <code>
		/// // SQL without OFFSET (will be appended automatically):
		/// var sql = @"
		///     SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId;
		///     SELECT * FROM Orders WHERE CustomerId = @CustomerId ORDER BY CreatedAt DESC";
		///
		/// // SQL with explicit OFFSET:
		/// var sql = @"
		///     SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId;
		///     SELECT * FROM Orders WHERE CustomerId = @CustomerId
		///     ORDER BY CreatedAt DESC
		///     OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY";
		/// </code>
		/// </example>
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the paged result.</typeparam>
		/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
		/// <param name="pageSize">The number of items per page.</param>
		/// <param name="page">The current page number (1-based).</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the queried items and pagination metadata.</returns>
		public async Task<Result<PagedResult<T>>> GetPagedAsync<T>(
			string sql,
			int pageSize,
			int page,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetPagedAsync<T>(sql, pageSize, page, null, cancellationToken);
		}

		/// <summary>
		/// Executes a SQL batch containing a count query and a data query, returning a paginated result.
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
		/// <typeparam name="T">The type of the elements to be returned in the paged result.</typeparam>
		/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
		/// <param name="parameters">An object containing the query parameters, including <c>PageSize</c> and <c>Page</c> properties.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the queried items and pagination metadata.</returns>
		public async Task<Result<PagedResult<T>>> GetPagedAsync<T>(
			string sql,
			object parameters,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetPagedAsync<T>(sql, parameters, null, cancellationToken);
		}

		/// <summary>
		/// Executes a SQL batch containing a count query and a data query, returning a paginated result with mapped items.
		/// </summary>
		/// <remarks>
		/// <para>
		/// The SQL should contain two statements: first a <c>SELECT COUNT(*)</c> query, then a data query with an
		/// <c>ORDER BY</c> clause. If the data query does not already contain an <c>OFFSET</c> clause, one will be
		/// appended automatically.
		/// </para>
		/// <para>
		/// This method automatically injects <c>@PageSize</c> and <c>@Offset</c> parameters. Your SQL can reference
		/// these directly, or omit the OFFSET clause entirely to have it appended automatically.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final paged result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
		/// <param name="pageSize">The number of items per page.</param>
		/// <param name="page">The current page number (1-based).</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the mapped items and pagination metadata.</returns>
		public async Task<Result<PagedResult<TModel>>> GetPagedAsync<TData, TModel>(
			string sql,
			int pageSize,
			int page,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetPagedAsync(sql, pageSize, page, mapper, null, cancellationToken);
		}

		/// <summary>
		/// Executes a SQL batch containing a count query and a data query, returning a paginated result with mapped items.
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
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final paged result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch to execute. Should contain a COUNT query followed by a data query with ORDER BY.</param>
		/// <param name="parameters">An object containing the query parameters, including <c>PageSize</c> and <c>Page</c> properties.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the mapped items and pagination metadata.</returns>
		public async Task<Result<PagedResult<TModel>>> GetPagedAsync<TData, TModel>(
			string sql,
			object parameters,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.GetPagedAsync(sql, parameters, mapper, null, cancellationToken);
		}

		#endregion

		#region QUERY CURSOR

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a cursor-based paginated result
		/// wrapped in a <see cref="Result{T}"/> object.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional pages exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the cursor result.</typeparam>
		/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="pageSize">The maximum number of items to return per page.</param>
		/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier from an item for cursor encoding.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the queried items and cursor metadata.</returns>
		public async Task<Result<CursorResult<T>>> QueryCursorAsync<T, TColumn>(
			string sql,
			int pageSize,
			Func<T, (TColumn Column, Guid Id)> cursorSelector,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryCursorAsync(
				sql,
				null,
				pageSize,
				cursorSelector,
				null,
				cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a cursor-based paginated result
		/// wrapped in a <see cref="Result{T}"/> object.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional pages exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// <para>
		/// The query should include a WHERE clause for cursor positioning when a cursor is provided. Use
		/// <see cref="Cursor.Decode{TColumn}"/> to decode the cursor and pass <c>cursor?.Column</c> and
		/// <c>cursor?.Id</c> as parameters.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// -- First page (no cursor)
		/// SELECT TOP (@PageSize) *
		/// FROM Orders
		/// WHERE CustomerId = @CustomerId
		/// ORDER BY CreatedAt DESC, OrderId DESC
		///
		/// -- Subsequent pages (with cursor)
		/// SELECT TOP (@PageSize) *
		/// FROM Orders
		/// WHERE CustomerId = @CustomerId
		///   AND (CreatedAt &lt; @Column
		///        OR (CreatedAt = @Column AND OrderId &lt; @Id))
		/// ORDER BY CreatedAt DESC, OrderId DESC
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// var cursor = Cursor.Decode&lt;DateTime&gt;(query.Cursor);
		///
		/// var sql = cursor is null
		///     ? "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC, Id DESC"
		///     : "SELECT TOP (@PageSize) ... WHERE (CreatedAt &lt; @Column OR (CreatedAt = @Column AND Id &lt; @Id)) ORDER BY CreatedAt DESC, Id DESC";
		///
		/// return await factory.QueryCursorAsync&lt;Order, DateTime&gt;(
		///     sql,
		///     new { query.CustomerId, cursor?.Column, cursor?.Id },
		///     query.PageSize,
		///     o =&gt; (o.CreatedAt, o.Id),
		///     cancellationToken);
		/// </code>
		/// <para>
		/// The returned cursor is URL-safe base64 encoded and can be passed directly in query strings.
		/// </para>
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the cursor result.</typeparam>
		/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="pageSize">The maximum number of items to return per page.</param>
		/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier from an item for cursor encoding.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the queried items and cursor metadata.</returns>
		public async Task<Result<CursorResult<T>>> QueryCursorAsync<T, TColumn>(
			string sql,
			object? parameters,
			int pageSize,
			Func<T, (TColumn Column, Guid Id)> cursorSelector,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryCursorAsync(
				sql,
				parameters,
				pageSize,
				cursorSelector,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a cursor-based paginated result
		/// wrapped in a <see cref="Result{T}"/> object, applying a mapping function to transform each item.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional pages exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final cursor result (domain layer).</typeparam>
		/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="pageSize">The maximum number of items to return per page.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier from a mapped item for cursor encoding.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the mapped items and cursor metadata.</returns>
		public async Task<Result<CursorResult<TModel>>> QueryCursorAsync<TData, TModel, TColumn>(
			string sql,
			int pageSize,
			Func<TData, TModel> mapper,
			Func<TModel, (TColumn Column, Guid Id)> cursorSelector,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryCursorAsync(
				sql,
				pageSize,
				mapper,
				cursorSelector,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns the results as a cursor-based paginated result
		/// wrapped in a <see cref="Result{T}"/> object, applying a mapping function to transform each item.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional pages exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// <para>
		/// The query should include a WHERE clause for cursor positioning when a cursor is provided. Use
		/// <see cref="Cursor.Decode{TColumn}"/> to decode the cursor and pass <c>cursor?.Column</c> and
		/// <c>cursor?.Id</c> as parameters.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// -- First page (no cursor)
		/// SELECT TOP (@PageSize) *
		/// FROM Orders
		/// WHERE CustomerId = @CustomerId
		/// ORDER BY CreatedAt DESC, OrderId DESC
		///
		/// -- Subsequent pages (with cursor)
		/// SELECT TOP (@PageSize) *
		/// FROM Orders
		/// WHERE CustomerId = @CustomerId
		///   AND (CreatedAt &lt; @Column
		///        OR (CreatedAt = @Column AND OrderId &lt; @Id))
		/// ORDER BY CreatedAt DESC, OrderId DESC
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// var cursor = Cursor.Decode&lt;DateTime&gt;(query.Cursor);
		///
		/// var sql = cursor is null
		///     ? "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC, Id DESC"
		///     : "SELECT TOP (@PageSize) ... WHERE (CreatedAt &lt; @Column OR (CreatedAt = @Column AND Id &lt; @Id)) ORDER BY CreatedAt DESC, Id DESC";
		///
		/// return await factory.QueryCursorAsync&lt;OrderData, Order, DateTime&gt;(
		///     sql,
		///     new { query.CustomerId, cursor?.Column, cursor?.Id },
		///     query.PageSize,
		///     data =&gt; new Order(data),
		///     o =&gt; (o.CreatedAt, o.Id),
		///     cancellationToken);
		/// </code>
		/// <para>
		/// The returned cursor is URL-safe base64 encoded and can be passed directly in query strings.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final cursor result (domain layer).</typeparam>
		/// <typeparam name="TColumn">The type of the sort column used for cursor positioning.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="pageSize">The maximum number of items to return per page.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cursorSelector">A function that extracts the sort column value and unique identifier from a mapped item for cursor encoding.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the mapped items and cursor metadata.</returns>
		public async Task<Result<CursorResult<TModel>>> QueryCursorAsync<TData, TModel, TColumn>(
			string sql,
			object? parameters,
			int pageSize,
			Func<TData, TModel> mapper,
			Func<TModel, (TColumn Column, Guid Id)> cursorSelector,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QueryCursorAsync(
				sql,
				parameters,
				pageSize,
				mapper,
				cursorSelector,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region QUERY SLICE

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns a slice of results with an indicator
		/// for whether more items exist.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional items exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// SELECT TOP (@PageSize) *
		/// FROM Orders
		/// WHERE CustomerId = @CustomerId
		/// ORDER BY CreatedAt DESC
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.QuerySliceAsync&lt;Order&gt;(
		///     "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC",
		///     query.PageSize,
		///     cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the slice result.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="pageSize">The maximum number of items to return.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the queried items and a flag indicating if more items exist.</returns>
		public async Task<Result<SliceResult<T>>> QuerySliceAsync<T>(
			string sql,
			int pageSize,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QuerySliceAsync<T>(
				sql,
				pageSize,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns a slice of results with an indicator
		/// for whether more items exist.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional items exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// SELECT TOP (@PageSize) *
		/// FROM Orders
		/// WHERE CustomerId = @CustomerId
		/// ORDER BY CreatedAt DESC
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.QuerySliceAsync&lt;Order&gt;(
		///     "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC",
		///     new { query.CustomerId },
		///     query.PageSize,
		///     cancellationToken);
		/// </code>
		/// <para>
		/// Use this for simple "load more" patterns without full pagination metadata.
		/// For stable cursor-based pagination, use <see cref="QueryCursorAsync{T, TColumn}(ISqlConnectionFactory, string, object?, int, Func{T, ValueTuple{TColumn, Guid}}, CancellationToken)"/> instead.
		/// For full pagination with total counts, use <see cref="QueryPagedAsync{T}(ISqlConnectionFactory, string, object?, int, int, int, CancellationToken)"/> instead.
		/// </para>
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the slice result.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="pageSize">The maximum number of items to return.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the queried items and a flag indicating if more items exist.</returns>
		public async Task<Result<SliceResult<T>>> QuerySliceAsync<T>(
			string sql,
			object? parameters,
			int pageSize,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QuerySliceAsync<T>(
				sql,
				parameters,
				pageSize,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns a slice of results with an indicator
		/// for whether more items exist, applying a mapping function to transform each item.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional items exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final slice result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="pageSize">The maximum number of items to return.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the mapped items and a flag indicating if more items exist.</returns>
		public async Task<Result<SliceResult<TModel>>> QuerySliceAsync<TData, TModel>(
			string sql,
			int pageSize,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QuerySliceAsync(
				sql,
				pageSize,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns a slice of results with an indicator
		/// for whether more items exist, applying a mapping function to transform each item.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method automatically injects a <c>@PageSize</c> parameter set to <paramref name="pageSize"/> + 1 to
		/// determine if additional items exist. Your SQL query should use <c>TOP (@PageSize)</c>.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// SELECT TOP (@PageSize) *
		/// FROM Orders
		/// WHERE CustomerId = @CustomerId
		/// ORDER BY CreatedAt DESC
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.QuerySliceAsync&lt;OrderData, Order&gt;(
		///     "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC",
		///     new { query.CustomerId },
		///     query.PageSize,
		///     data =&gt; new Order(data),
		///     cancellationToken);
		/// </code>
		/// <para>
		/// Use this for simple "load more" patterns without full pagination metadata.
		/// For stable cursor-based pagination, use <see cref="QueryCursorAsync{TData, TModel, TColumn}(ISqlConnectionFactory, string, object?, int, Func{TData, TModel}, Func{TModel, ValueTuple{TColumn, Guid}}, CancellationToken)"/> instead.
		/// For full pagination with total counts, use <see cref="QueryPagedAsync{TData, TModel}(ISqlConnectionFactory, string, object?, int, int, int, Func{TData, TModel}, CancellationToken)"/> instead.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final slice result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="pageSize">The maximum number of items to return.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the mapped items and a flag indicating if more items exist.</returns>
		public async Task<Result<SliceResult<TModel>>> QuerySliceAsync<TData, TModel>(
			string sql,
			object? parameters,
			int pageSize,
			Func<TData, TModel> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.QuerySliceAsync(
				sql,
				parameters,
				pageSize,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region INSERT

		/// <summary>
		/// Executes an INSERT command and returns a successful result if at least one row was affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for INSERT operations where no rows affected indicates a failure.
		/// Returns <see cref="InvalidOperationException"/> (HTTP 500) if no rows were inserted.
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400, referenced record doesn't exist).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// INSERT INTO Orders (OrderId, CustomerId, Amount, CreatedAt)
		/// VALUES (@OrderId, @CustomerId, @Amount, @CreatedAt)
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// var orderId = Guid.CreateVersion7();
		///
		/// return await factory.InsertAsync(
		///     "INSERT INTO Orders (OrderId, CustomerId, Amount) VALUES (@OrderId, @CustomerId, @Amount)",
		///     new { OrderId = orderId, command.CustomerId, command.Amount },
		///     uniqueConstraintMessage: "Order already exists",
		///     foreignKeyMessage: "Customer not found",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <param name="sql">The SQL INSERT statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful <see cref="Result"/>
		/// if at least one row was inserted, or a failure result with an appropriate exception.</returns>
		public async Task<Result> InsertAsync(
			string sql,
			object? parameters,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.InsertAsync(
				sql,
				parameters,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes an INSERT command and returns the specified value if at least one row was affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for INSERT operations that return a client-generated value (e.g., a Guid created before insert).
		/// Returns <see cref="InvalidOperationException"/> (HTTP 500) if no rows were inserted.
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400, referenced record doesn't exist).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// INSERT INTO Orders (OrderId, CustomerId, Amount, CreatedAt)
		/// VALUES (@OrderId, @CustomerId, @Amount, @CreatedAt)
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// var orderId = Guid.CreateVersion7();
		///
		/// return await factory.InsertAndReturnAsync(
		///     "INSERT INTO Orders (OrderId, CustomerId, Amount) VALUES (@OrderId, @CustomerId, @Amount)",
		///     new { OrderId = orderId, command.CustomerId, command.Amount },
		///     () =&gt; orderId,
		///     uniqueConstraintMessage: "Order already exists",
		///     foreignKeyMessage: "Customer not found",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the value to return on success.</typeparam>
		/// <param name="sql">The SQL INSERT statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="resultSelector">A function that returns the value to include in the successful result.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the value from <paramref name="resultSelector"/> if at least one row was inserted, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> InsertAndReturnAsync<T>(
			string sql,
			object? parameters,
			Func<T> resultSelector,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.InsertAndReturnAsync(
				sql,
				parameters,
				resultSelector,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region UPDATE

		/// <summary>
		/// Executes an UPDATE command and returns a successful result if at least one row was affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for UPDATE operations where no rows affected indicates the record was not found.
		/// Returns <see cref="NotFoundException"/> (HTTP 404) if no rows were updated.
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400, referenced record doesn't exist).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// UPDATE Orders
		/// SET Amount = @Amount, UpdatedAt = @UpdatedAt
		/// WHERE OrderId = @OrderId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.UpdateAsync(
		///     "UPDATE Orders SET Amount = @Amount WHERE OrderId = @OrderId",
		///     new { command.OrderId, command.Amount },
		///     key: command.OrderId,
		///     uniqueConstraintMessage: "Order with this reference already exists",
		///     foreignKeyMessage: "Customer not found",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <param name="sql">The SQL UPDATE statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being updated, used in the <see cref="NotFoundException"/> if no rows are affected.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful <see cref="Result"/>
		/// if at least one row was updated, or a failure result with an appropriate exception.</returns>
		public async Task<Result> UpdateAsync(
			string sql,
			object? parameters,
			object key,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.UpdateAsync(
				sql,
				parameters,
				key,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes an UPDATE command and returns the specified value if at least one row was affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for UPDATE operations that return a value on success.
		/// Returns <see cref="NotFoundException"/> (HTTP 404) if no rows were updated.
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400, referenced record doesn't exist).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// UPDATE Orders
		/// SET Amount = @Amount, UpdatedAt = @UpdatedAt
		/// WHERE OrderId = @OrderId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.UpdateAndReturnAsync(
		///     "UPDATE Orders SET Amount = @Amount WHERE OrderId = @OrderId",
		///     new { command.OrderId, command.Amount },
		///     key: command.OrderId,
		///     () =&gt; command.OrderId,
		///     uniqueConstraintMessage: "Order with this reference already exists",
		///     foreignKeyMessage: "Customer not found",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the value to return on success.</typeparam>
		/// <param name="sql">The SQL UPDATE statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being updated, used in the <see cref="NotFoundException"/> if no rows are affected.</param>
		/// <param name="resultSelector">A function that returns the value to include in the successful result.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the value from <paramref name="resultSelector"/> if at least one row was updated, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> UpdateAndReturnAsync<T>(
			string sql,
			object? parameters,
			object key,
			Func<T> resultSelector,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.UpdateAndReturnAsync(
				sql,
				parameters,
				key,
				resultSelector,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region DELETE

		/// <summary>
		/// Executes a DELETE command and returns a successful result if at least one row was affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for DELETE operations where no rows affected indicates the record was not found.
		/// Returns <see cref="NotFoundException"/> (HTTP 404) if no rows were deleted.
		/// Foreign key violations become <see cref="ConflictException"/> (HTTP 409, record is still referenced by other records).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// DELETE FROM Orders
		/// WHERE OrderId = @OrderId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await factory.DeleteAsync(
		///     "DELETE FROM Orders WHERE OrderId = @OrderId",
		///     new { command.OrderId },
		///     key: command.OrderId,
		///     foreignKeyMessage: "Cannot delete order, it has associated line items",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <param name="sql">The SQL DELETE statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being deleted, used in the <see cref="NotFoundException"/> if no rows are affected.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful <see cref="Result"/>
		/// if at least one row was deleted, or a failure result with an appropriate exception.</returns>
		public async Task<Result> DeleteAsync(
			string sql,
			object? parameters,
			object key,
			string foreignKeyMessage = "Cannot delete, record is in use",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.DeleteAsync(
				sql,
				parameters,
				key,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes a DELETE command and returns the specified value if at least one row was affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for DELETE operations where you need to return a value (typically the deleted entity's ID)
		/// while still checking that a row was actually deleted.
		/// Returns <see cref="NotFoundException"/> (HTTP 404) if no rows were deleted.
		/// Foreign key violations become <see cref="ConflictException"/> (HTTP 409, record is still referenced by other records).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// DELETE FROM Orders
		/// WHERE OrderId = @OrderId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// var orderId = command.OrderId;
		///
		/// return await factory.DeleteAndReturnAsync(
		///     "DELETE FROM Orders WHERE OrderId = @OrderId",
		///     new { OrderId = orderId },
		///     key: orderId,
		///     () =&gt; orderId,
		///     foreignKeyMessage: "Cannot delete order, it has associated line items",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the value to return on success.</typeparam>
		/// <param name="sql">The SQL DELETE statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being deleted, used in the <see cref="NotFoundException"/> if no rows are affected.</param>
		/// <param name="resultSelector">A function that returns the value to include in the successful result.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the value from <paramref name="resultSelector"/> if at least one row was deleted, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> DeleteAndReturnAsync<T>(
			string sql,
			object? parameters,
			object key,
			Func<T> resultSelector,
			string foreignKeyMessage = "Cannot delete, record is in use",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.DeleteAndReturnAsync(
				sql,
				parameters,
				key,
				resultSelector,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region INSERT AND GET

		/// <summary>
		/// Executes an INSERT command followed by a SELECT in a single batch and returns the selected row.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you need to INSERT a record and immediately SELECT it back to get server-generated
		/// values (auto-increment IDs, default values, computed columns, etc.) in a single database roundtrip.
		/// Returns <see cref="InvalidOperationException"/> if no row is returned by the SELECT.
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// INSERT INTO Orders (CustomerId, Amount) VALUES (@CustomerId, @Amount);
		/// SELECT * FROM Orders WHERE Id = SCOPE_IDENTITY();
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the row to return.</typeparam>
		/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the selected row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> InsertAndGetAsync<T>(
			string sql,
			object? parameters = null,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.InsertAndGetAsync<T>(
				sql,
				parameters,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes an INSERT command followed by a SELECT in a single batch and returns the selected row,
		/// applying a mapping function to transform the result.
		/// </summary>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function to transform the data row to the domain model.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<TModel>> InsertAndGetAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.InsertAndGetAsync(
				sql,
				parameters,
				mapper,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes an INSERT command followed by a SELECT in a single batch and returns an <see cref="Optional{T}"/>
		/// containing the selected row if present, allowing the caller to handle the empty case via a mapper.
		/// </summary>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the result from the mapper.</returns>
		public async Task<Result<TModel>> InsertAndGetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<Optional<TData>, TModel> mapper,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.InsertAndGetOptionalAsync(
				sql,
				parameters,
				mapper,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region UPDATE AND GET

		/// <summary>
		/// Executes an UPDATE command followed by a SELECT in a single batch and returns the selected row.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you need to UPDATE a record and immediately SELECT it back to get the updated
		/// values (including any server-side modifications) in a single database roundtrip.
		/// Returns <see cref="NotFoundException"/> (HTTP 404) if no row is returned by the SELECT.
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// UPDATE Orders SET Amount = @Amount, UpdatedAt = GETUTCDATE() WHERE Id = @Id;
		/// SELECT * FROM Orders WHERE Id = @Id;
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the row to return.</typeparam>
		/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being updated, used in the <see cref="NotFoundException"/> if no row is returned.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the selected row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> UpdateAndGetAsync<T>(
			string sql,
			object? parameters,
			object key,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.UpdateAndGetAsync<T>(
				sql,
				parameters,
				key,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes an UPDATE command followed by a SELECT in a single batch and returns the selected row,
		/// applying a mapping function to transform the result.
		/// </summary>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being updated, used in the <see cref="NotFoundException"/> if no row is returned.</param>
		/// <param name="mapper">A function to transform the data row to the domain model.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<TModel>> UpdateAndGetAsync<TData, TModel>(
			string sql,
			object? parameters,
			object key,
			Func<TData, TModel> mapper,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.UpdateAndGetAsync(
				sql,
				parameters,
				key,
				mapper,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes an UPDATE command followed by a SELECT in a single batch and returns an <see cref="Optional{T}"/>
		/// containing the selected row if present, allowing the caller to handle the empty case via a mapper.
		/// </summary>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the result from the mapper.</returns>
		public async Task<Result<TModel>> UpdateAndGetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<Optional<TData>, TModel> mapper,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.UpdateAndGetOptionalAsync(
				sql,
				parameters,
				mapper,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region DELETE AND GET

		/// <summary>
		/// Executes a DELETE command and returns the deleted row using an OUTPUT clause or similar mechanism.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you need to DELETE a record and retrieve what was deleted in a single database roundtrip.
		/// Returns <see cref="NotFoundException"/> (HTTP 404) if no row is returned.
		/// Foreign key violations become <see cref="ConflictException"/> (HTTP 409, record is still referenced).
		/// </para>
		/// <para>
		/// <strong>SQL Pattern (SQL Server with OUTPUT):</strong>
		/// </para>
		/// <code>
		/// DELETE FROM Orders OUTPUT DELETED.* WHERE Id = @Id;
		/// </code>
		/// <para>
		/// <strong>SQL Pattern (PostgreSQL with RETURNING):</strong>
		/// </para>
		/// <code>
		/// DELETE FROM Orders WHERE Id = @Id RETURNING *;
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the row to return.</typeparam>
		/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being deleted, used in the <see cref="NotFoundException"/> if no row is returned.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the deleted row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> DeleteAndGetAsync<T>(
			string sql,
			object? parameters,
			object key,
			string foreignKeyMessage = "Cannot delete, record is in use",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.DeleteAndGetAsync<T>(
				sql,
				parameters,
				key,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes a DELETE command and returns the deleted row using an OUTPUT clause or similar mechanism,
		/// applying a mapping function to transform the result.
		/// </summary>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being deleted, used in the <see cref="NotFoundException"/> if no row is returned.</param>
		/// <param name="mapper">A function to transform the data row to the domain model.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<TModel>> DeleteAndGetAsync<TData, TModel>(
			string sql,
			object? parameters,
			object key,
			Func<TData, TModel> mapper,
			string foreignKeyMessage = "Cannot delete, record is in use",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.DeleteAndGetAsync(
				sql,
				parameters,
				key,
				mapper,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		/// <summary>
		/// Executes a DELETE command and returns an <see cref="Optional{T}"/> containing the deleted row if present,
		/// allowing the caller to handle the empty case via a mapper.
		/// </summary>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the result from the mapper.</returns>
		public async Task<Result<TModel>> DeleteAndGetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<Optional<TData>, TModel> mapper,
			string foreignKeyMessage = "Cannot delete, record is in use",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.DeleteAndGetOptionalAsync(
				sql,
				parameters,
				mapper,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region INSERT WITH COUNT

		/// <summary>
		/// Executes an INSERT command and returns the number of rows affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you want to know how many rows were inserted and treat 0 as a valid outcome
		/// (e.g., conditional inserts like INSERT...WHERE NOT EXISTS).
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400).
		/// </para>
		/// </remarks>
		/// <param name="sql">The SQL INSERT statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> with the number of rows affected.</returns>
		public async Task<Result<int>> InsertWithCountAsync(
			string sql,
			object? parameters = null,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.InsertWithCountAsync(
				sql,
				parameters,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region UPDATE WITH COUNT

		/// <summary>
		/// Executes an UPDATE command and returns the number of rows affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you want to know how many rows were updated and treat 0 as a valid outcome
		/// (e.g., conditional updates or "update if exists" patterns).
		/// Unique constraint violations become <see cref="AlreadyExistsException"/> (HTTP 409).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400).
		/// </para>
		/// </remarks>
		/// <param name="sql">The SQL UPDATE statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> with the number of rows affected.</returns>
		public async Task<Result<int>> UpdateWithCountAsync(
			string sql,
			object? parameters = null,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.UpdateWithCountAsync(
				sql,
				parameters,
				uniqueConstraintMessage,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region DELETE WITH COUNT

		/// <summary>
		/// Executes a DELETE command and returns the number of rows affected.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you want to know how many rows were deleted and treat 0 as a valid outcome
		/// (e.g., "delete if exists" patterns).
		/// Foreign key violations become <see cref="BadRequestException"/> (HTTP 400).
		/// </para>
		/// </remarks>
		/// <param name="sql">The SQL DELETE statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> with the number of rows affected.</returns>
		public async Task<Result<int>> DeleteWithCountAsync(
			string sql,
			object? parameters = null,
			string foreignKeyMessage = "Cannot delete, record is in use",
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.DeleteWithCountAsync(
				sql,
				parameters,
				foreignKeyMessage,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region MULTIPLE GET

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns a failure with <see cref="NotFoundException"/> if the mapper returns null.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="keys">The keys used to identify the resource for the <see cref="NotFoundException"/>.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if the mapper returns null.</returns>
		public async Task<Result<T>> MultipleGetAsync<T>(
			string sql,
			object[] keys,
			Func<IMultipleResult, Task<T?>> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.MultipleGetAsync(sql, null, keys, mapper, null, cancellationToken);
		}

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns a failure with <see cref="NotFoundException"/> if the mapper returns null.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="keys">The keys used to identify the resource for the <see cref="NotFoundException"/>.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if the mapper returns null.</returns>
		public async Task<Result<T>> MultipleGetAsync<T>(
			string sql,
			object? parameters,
			object[] keys,
			Func<IMultipleResult, Task<T?>> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.MultipleGetAsync(
				sql,
				parameters,
				keys,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region MULTIPLE GET OPTIONAL

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns an <see cref="Optional{T}"/> that is empty if the mapper returns null.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// with an <see cref="Optional{T}"/> that is empty if the mapper returns null, or contains the value otherwise.</returns>
		public async Task<Result<Optional<T>>> MultipleGetOptionalAsync<T>(
			string sql,
			Func<IMultipleResult, Task<T?>> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.MultipleGetOptionalAsync(sql, null, mapper, null, cancellationToken);
		}

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns an <see cref="Optional{T}"/> that is empty if the mapper returns null.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// with an <see cref="Optional{T}"/> that is empty if the mapper returns null, or contains the value otherwise.</returns>
		public async Task<Result<Optional<T>>> MultipleGetOptionalAsync<T>(
			string sql,
			object? parameters,
			Func<IMultipleResult, Task<T?>> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.MultipleGetOptionalAsync(
				sql,
				parameters,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

		#region MULTIPLE QUERY ANY

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns the list from the mapper; an empty list is a valid result.
		/// </summary>
		/// <typeparam name="T">The type of the elements in the returned list.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the list.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// wrapping a read-only list of items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<T>>> MultipleQueryAnyAsync<T>(
			string sql,
			Func<IMultipleResult, Task<IReadOnlyList<T>?>> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.MultipleQueryAnyAsync(sql, null, mapper, null, cancellationToken);
		}

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns the list from the mapper; an empty list is a valid result.
		/// </summary>
		/// <typeparam name="T">The type of the elements in the returned list.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the list.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// wrapping a read-only list of items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<T>>> MultipleQueryAnyAsync<T>(
			string sql,
			object? parameters,
			Func<IMultipleResult, Task<IReadOnlyList<T>?>> mapper,
			CancellationToken cancellationToken = default) {
			await using var connection = await factory.CreateConnectionAsync(cancellationToken);
			return await connection.MultipleQueryAnyAsync(
				sql,
				parameters,
				mapper,
				transaction: null,
				cancellationToken: cancellationToken);
		}

		#endregion

	}

}