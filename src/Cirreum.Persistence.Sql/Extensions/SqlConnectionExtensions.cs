namespace Cirreum.Persistence;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence.Internal;
using System.Data;

/// <summary>
/// Provides extension methods for executing SQL queries using Dapper and returning results wrapped in Result types,
/// including support for single, multiple, and paginated query results with optional mapping functions.
/// </summary>
/// <remarks>These extension methods simplify common query patterns by integrating Dapper query execution with the
/// Result and PagedResult types. They support asynchronous operations, parameterized queries, and mapping between data
/// and domain models. Methods are designed to handle not-found cases and pagination scenarios, and to promote
/// consistent result handling across data access layers.</remarks>
public static class SqlConnectionExtensions {

	extension(ISqlConnection conn) {

		#region GET

		/// <summary>
		/// Retrieves a single entity by executing the specified SQL query asynchronously and returns the result wrapped in a <see cref="Result{T}"/>
		/// object.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should be a statement that returns a single row.</param>
		/// <param name="key">A key associated with the query result, used to identify or correlate the returned value.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// queried value, or a NotFound result if no row is found.</returns>
		public Task<Result<T>> GetAsync<T>(
			string sql,
			object key,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.GetAsync<T>(sql, null, key, transaction, cancellationToken);

		/// <summary>
		/// Retrieves a single entity by executing the specified SQL query asynchronously and returns the result wrapped in a <see cref="Result{T}"/>
		/// object.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should be a statement that returns a single row.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are
		/// required.</param>
		/// <param name="key">A key associated with the query result, used to identify or correlate the returned value.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// queried value, or a NotFound result if no row is found.</returns>
		public async Task<Result<T>> GetAsync<T>(
			string sql,
			object? parameters,
			object key,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.QuerySingleOrDefaultAsync<T>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken);
			return Result.FromLookup(result, key);
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if no row is found.</returns>
		public Task<Result<TModel>> GetAsync<TData, TModel>(
			string sql,
			object key,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.GetAsync(sql, null, key, mapper, transaction, cancellationToken);

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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if no row is found.</returns>
		public async Task<Result<TModel>> GetAsync<TData, TModel>(
			string sql,
			object? parameters,
			object key,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.QuerySingleOrDefaultAsync<TData>(
				sql,
				parameters,
				transaction,
				cancellationToken);
			if (result is null) {
				return Result.NotFound<TModel>(key);
			}
			return mapper(result);
		}


		#endregion

		#region GET OPTIONAL

		/// <summary>
		/// Retrieves zero or one entity by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when a row may or may not exist and absence is not an error condition.
		/// Returns <see cref="Optional{T}.Empty"/> if no row is found.
		/// Fails if more than one row is returned.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// SELECT * FROM UserPrefs WHERE UserId = @UserId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// var prefs = await conn.GetOptionalAsync&lt;UserPrefs&gt;(
		///     "SELECT * FROM UserPrefs WHERE UserId = @UserId",
		///     new { UserId = userId },
		///     cancellationToken: cancellationToken);
		///     
		/// // Use Optional methods to handle presence/absence
		/// var theme = prefs.Value.Map(p => p.Theme).GetValueOrDefault("default");
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the value if one row was found.</returns>
		public Task<Result<Optional<T>>> GetOptionalAsync<T>(
			string sql,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.GetOptionalAsync<T>(sql, null, transaction, cancellationToken);

		/// <summary>
		/// Retrieves zero or one entity by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when a row may or may not exist and absence is not an error condition.
		/// Returns <see cref="Optional{T}.Empty"/> if no row is found.
		/// Fails if more than one row is returned.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// SELECT * FROM UserPrefs WHERE UserId = @UserId
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// var prefs = await conn.GetOptionalAsync&lt;UserPrefs&gt;(
		///     "SELECT * FROM UserPrefs WHERE UserId = @UserId",
		///     new { UserId = userId },
		///     cancellationToken: cancellationToken);
		///     
		/// // Use Optional methods to handle presence/absence
		/// var theme = prefs.Value.Map(p => p.Theme).GetValueOrDefault("default");
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return zero or one row.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the value if one row was found.</returns>
		public async Task<Result<Optional<T>>> GetOptionalAsync<T>(
			string sql,
			object? parameters,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			try {
				var result = await conn.QuerySingleOrDefaultAsync<T>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);
				return Optional.From(result);
			} catch (InvalidOperationException ex) when (ex.Message.Contains("Sequence contains more than one element")) {
				return new InvalidOperationException("Query returned more than one row.", ex);
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped value if one row was found.</returns>
		public Task<Result<Optional<TModel>>> GetOptionalAsync<TData, TModel>(
			string sql,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.GetOptionalAsync(sql, null, mapper, transaction, cancellationToken);

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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped value if one row was found.</returns>
		public async Task<Result<Optional<TModel>>> GetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			try {
				var result = await conn.QuerySingleOrDefaultAsync<TData>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);
				return Optional.From(result).Map(mapper);
			} catch (InvalidOperationException ex) when (ex.Message.Contains("Sequence contains more than one element")) {
				return new InvalidOperationException("Query returned more than one row.", ex);
			}
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
		/// return await conn.GetScalarAsync&lt;int&gt;(
		///     "SELECT COUNT(*) FROM Orders",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the scalar value to return.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return a single scalar value.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the scalar value.</returns>
		public Task<Result<T>> GetScalarAsync<T>(
			string sql,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.GetScalarAsync<T>(sql, null, transaction, cancellationToken);

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
		/// return await conn.GetScalarAsync&lt;int&gt;(
		///     "SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId",
		///     new { query.CustomerId },
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the scalar value to return.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return a single scalar value.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the scalar value.</returns>
		public async Task<Result<T>> GetScalarAsync<T>(
			string sql,
			object? parameters,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.ExecuteScalarAsync<T>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return Result.FromNullable(result, new InvalidOperationException("Scalar query returned null. Use ISNULL/COALESCE in SQL."));
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
		/// return await conn.GetScalarAsync&lt;int, OrderCount&gt;(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped scalar value.</returns>
		public Task<Result<TModel>> GetScalarAsync<TData, TModel>(
			string sql,
			Func<TData?, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.GetScalarAsync(sql, null, mapper, transaction, cancellationToken);

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
		/// return await conn.GetScalarAsync&lt;int, OrderCount&gt;(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped scalar value.</returns>
		public async Task<Result<TModel>> GetScalarAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData?, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.ExecuteScalarAsync<TData>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return Result.FromNullable(mapper(result), new InvalidOperationException("Mapper returned null."));
		}

		#endregion

		#region QUERY OPTIONAL

		/// <summary>
		/// Retrieves the first row (if any) by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you want the first matching row and don't care if multiple rows exist.
		/// For strict single-row semantics where multiple rows is an error, use <see cref="GetOptionalAsync{T}(ISqlConnection, string, IDbTransaction?, CancellationToken)"/>.
		/// </para>
		/// </remarks>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the first row's value.</returns>
		public Task<Result<Optional<T>>> QueryOptionalAsync<T>(
			string sql,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryOptionalAsync<T>(sql, null, transaction, cancellationToken);

		/// <summary>
		/// Retrieves the first row (if any) by executing the specified SQL query asynchronously and returns 
		/// the result wrapped in a <see cref="Result{T}"/> containing an <see cref="Optional{T}"/>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method when you want the first matching row and don't care if multiple rows exist.
		/// For strict single-row semantics where multiple rows is an error, use <see cref="GetOptionalAsync{T}(ISqlConnection, string, object?, IDbTransaction?, CancellationToken)"/>.
		/// </para>
		/// </remarks>
		/// <typeparam name="T">The type of the object to be returned from the query.</typeparam>
		/// <param name="sql">The SQL query to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the first row's value.</returns>
		public async Task<Result<Optional<T>>> QueryOptionalAsync<T>(
			string sql,
			object? parameters,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.QueryFirstOrDefaultAsync<T>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return Optional.From(result);
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped first row's value.</returns>
		public Task<Result<Optional<TModel>>> QueryOptionalAsync<TData, TModel>(
			string sql,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryOptionalAsync(sql, null, mapper, transaction, cancellationToken);

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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> 
		/// with an <see cref="Optional{T}"/> that is empty if no row was found, or contains the mapped first row's value.</returns>
		public async Task<Result<Optional<TModel>>> QueryOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.QueryFirstOrDefaultAsync<TData>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return Optional.From(result).Map(mapper);
		}

		#endregion

		#region QUERY ANY

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result.
		/// </summary>
		/// <typeparam name="T">The type of the elements to be returned in the result list.</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of items (which may be empty).</returns>
		public Task<Result<IReadOnlyList<T>>> QueryAnyAsync<T>(
			string sql,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryAnyAsync<T>(sql, null, transaction, cancellationToken);

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result.
		/// </summary>
		/// <typeparam name="T">The type of the elements to be returned in the result list.</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or null if no parameters are required.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<T>>> QueryAnyAsync<T>(
			string sql,
			object? parameters,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.QueryAsync<T>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return Result.From<IReadOnlyList<T>>([.. result]);
		}

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result, applying a mapping function to transform each item.
		/// </summary>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final result list (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of mapped items (which may be empty).</returns>
		public Task<Result<IReadOnlyList<TModel>>> QueryAnyAsync<TData, TModel>(
			string sql,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryAnyAsync(sql, null, mapper, transaction, cancellationToken);

		/// <summary>
		/// Executes the specified SQL query asynchronously and returns zero or more results as a read-only list
		/// wrapped in a successful Result, applying a mapping function to transform each item.
		/// </summary>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final result list (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute against the database.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or null if no parameters are required.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful Result
		/// object wrapping a read-only list of mapped items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<TModel>>> QueryAnyAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var result = await conn.QueryAsync<TData>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return Result.From<IReadOnlyList<TModel>>([.. result.Select(mapper)]);
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the queried items and pagination metadata.</returns>
		public Task<Result<PagedResult<T>>> QueryPagedAsync<T>(
			string sql,
			int totalCount,
			int pageSize,
			int page,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryPagedAsync<T>(sql, null, totalCount, pageSize, page, transaction, cancellationToken);

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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the queried items and pagination metadata.</returns>
		public async Task<Result<PagedResult<T>>> QueryPagedAsync<T>(
			string sql,
			object? parameters,
			int totalCount,
			int pageSize,
			int page,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var items = await conn.QueryAsync<T>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return new PagedResult<T>(
				[.. items],
				totalCount,
				pageSize,
				page
			);
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="PagedResult{T}"/> with the mapped items and pagination metadata.</returns>
		public Task<Result<PagedResult<TModel>>> QueryPagedAsync<TData, TModel>(
			string sql,
			int totalCount,
			int pageSize,
			int page,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryPagedAsync(sql, null, totalCount, pageSize, page, mapper, transaction, cancellationToken);

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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
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
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {
			var items = await conn.QueryAsync<TData>(
				sql,
				parameters,
				transaction: transaction,
				cancellationToken: cancellationToken);
			return new PagedResult<TModel>(
				[.. items.Select(mapper)],
				totalCount,
				pageSize,
				page
			);
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the queried items and cursor metadata.</returns>
		public Task<Result<CursorResult<T>>> QueryCursorAsync<T, TColumn>(
			string sql,
			int pageSize,
			Func<T, (TColumn Column, Guid Id)> cursorSelector,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryCursorAsync(sql, null, pageSize, cursorSelector, transaction, cancellationToken);

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
		/// return await conn.QueryCursorAsync&lt;Order, DateTime&gt;(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the queried items and cursor metadata.</returns>
		public async Task<Result<CursorResult<T>>> QueryCursorAsync<T, TColumn>(
			string sql,
			object? parameters,
			int pageSize,
			Func<T, (TColumn Column, Guid Id)> cursorSelector,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			var mergedParams = ParameterHelper.MergeWithPageSize(parameters, pageSize + 1);

			var items = (await conn.QueryAsync<T>(
				sql,
				mergedParams,
				transaction: transaction,
				cancellationToken: cancellationToken)).ToList();

			var hasNextPage = items.Count > pageSize;
			if (hasNextPage) {
				items.RemoveAt(items.Count - 1);
			}

			string? nextCursor = null;
			if (hasNextPage && items.Count > 0) {
				var (column, id) = cursorSelector(items[^1]);
				nextCursor = Cursor.Encode(column, id);
			}

			return new CursorResult<T>(items, nextCursor, hasNextPage);
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the mapped items and cursor metadata.</returns>
		public Task<Result<CursorResult<TModel>>> QueryCursorAsync<TData, TModel, TColumn>(
			string sql,
			int pageSize,
			Func<TData, TModel> mapper,
			Func<TModel, (TColumn Column, Guid Id)> cursorSelector,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QueryCursorAsync(sql, null, pageSize, mapper, cursorSelector, transaction, cancellationToken);

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
		/// return await conn.QueryCursorAsync&lt;OrderData, Order, DateTime&gt;(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="CursorResult{T}"/> with the mapped items and cursor metadata.</returns>
		public async Task<Result<CursorResult<TModel>>> QueryCursorAsync<TData, TModel, TColumn>(
			string sql,
			object? parameters,
			int pageSize,
			Func<TData, TModel> mapper,
			Func<TModel, (TColumn Column, Guid Id)> cursorSelector,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			var mergedParams = ParameterHelper.MergeWithPageSize(parameters, pageSize + 1);

			var data = (await conn.QueryAsync<TData>(
				sql,
				mergedParams,
				transaction: transaction,
				cancellationToken: cancellationToken)).ToList();

			var hasNextPage = data.Count > pageSize;
			if (hasNextPage) {
				data.RemoveAt(data.Count - 1);
			}

			var items = data.Select(mapper).ToList();

			string? nextCursor = null;
			if (hasNextPage && items.Count > 0) {
				var (column, id) = cursorSelector(items[^1]);
				nextCursor = Cursor.Encode(column, id);
			}

			return new CursorResult<TModel>(items, nextCursor, hasNextPage);
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
		/// return await conn.QuerySliceAsync&lt;Order&gt;(
		///     "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC",
		///     query.PageSize,
		///     cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the slice result.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="pageSize">The maximum number of items to return.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the queried items and a flag indicating if more items exist.</returns>
		public Task<Result<SliceResult<T>>> QuerySliceAsync<T>(
			string sql,
			int pageSize,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QuerySliceAsync<T>(sql, null, pageSize, transaction, cancellationToken);

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
		/// return await conn.QuerySliceAsync&lt;Order&gt;(
		///     "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC",
		///     new { query.CustomerId },
		///     query.PageSize,
		///     cancellationToken);
		/// </code>
		/// <para>
		/// Use this for simple "load more" patterns without full pagination metadata.
		/// For stable cursor-based pagination, use <see cref="QueryCursorAsync{T, TColumn}(ISqlConnection, string, object?, int, Func{T, ValueTuple{TColumn, Guid}}, IDbTransaction?, CancellationToken)"/> instead.
		/// For full pagination with total counts, use <see cref="QueryPagedAsync{T}(ISqlConnection, string, object?, int, int, int, IDbTransaction?, CancellationToken)"/> instead.
		/// </para>
		/// </remarks>
		/// <typeparam name="T">The type of the elements to be returned in the slice result.</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="pageSize">The maximum number of items to return.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the queried items and a flag indicating if more items exist.</returns>
		public async Task<Result<SliceResult<T>>> QuerySliceAsync<T>(
			string sql,
			object? parameters,
			int pageSize,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			var mergedParams = ParameterHelper.MergeWithPageSize(parameters, pageSize + 1);

			var items = (await conn.QueryAsync<T>(
				sql,
				mergedParams,
				transaction: transaction,
				cancellationToken: cancellationToken)).ToList();

			var hasMore = items.Count > pageSize;
			if (hasMore) {
				items.RemoveAt(items.Count - 1);
			}

			return new SliceResult<T>(items, hasMore);
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the mapped items and a flag indicating if more items exist.</returns>
		public Task<Result<SliceResult<TModel>>> QuerySliceAsync<TData, TModel>(
			string sql,
			int pageSize,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.QuerySliceAsync(sql, null, pageSize, mapper, transaction, cancellationToken);

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
		/// return await conn.QuerySliceAsync&lt;OrderData, Order&gt;(
		///     "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC",
		///     new { query.CustomerId },
		///     query.PageSize,
		///     data =&gt; new Order(data),
		///     cancellationToken);
		/// </code>
		/// <para>
		/// Use this for simple "load more" patterns without full pagination metadata.
		/// For stable cursor-based pagination, use <see cref="QueryCursorAsync{TData, TModel, TColumn}(ISqlConnection, string, object?, int, Func{TData, TModel}, Func{TModel, ValueTuple{TColumn, Guid}}, IDbTransaction?, CancellationToken)"/> instead.
		/// For full pagination with total counts, use <see cref="QueryPagedAsync{TData, TModel}(ISqlConnection, string, object?, int, int, int, Func{TData, TModel}, IDbTransaction?, CancellationToken)"/> instead.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the elements returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the elements in the final slice result (domain layer).</typeparam>
		/// <param name="sql">The SQL query to execute. Should use <c>TOP (@PageSize)</c>.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="pageSize">The maximum number of items to return.</param>
		/// <param name="mapper">A function to transform each data item to the domain model.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// wrapping a <see cref="SliceResult{T}"/> with the mapped items and a flag indicating if more items exist.</returns>
		public async Task<Result<SliceResult<TModel>>> QuerySliceAsync<TData, TModel>(
			string sql,
			object? parameters,
			int pageSize,
			Func<TData, TModel> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			var mergedParams = ParameterHelper.MergeWithPageSize(parameters, pageSize + 1);

			var data = (await conn.QueryAsync<TData>(
				sql,
				mergedParams,
				transaction: transaction,
				cancellationToken: cancellationToken)).ToList();

			var hasMore = data.Count > pageSize;
			if (hasMore) {
				data.RemoveAt(data.Count - 1);
			}

			var items = data.Select(mapper).ToList();

			return new SliceResult<TModel>(items, hasMore);
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
		/// return await conn.InsertAsync(
		///     "INSERT INTO Orders (OrderId, CustomerId, Amount) VALUES (@OrderId, @CustomerId, @Amount)",
		///     new { OrderId = Guid.CreateVersion7(), command.CustomerId, command.Amount },
		///     uniqueConstraintMessage: "Order already exists",
		///     foreignKeyMessage: "Customer not found",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <param name="sql">The SQL INSERT statement to execute.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful <see cref="Result"/>
		/// if at least one row was inserted, or a failure result with an appropriate exception.</returns>
		public async Task<Result> InsertAsync(
			string sql,
			object? parameters = null,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected > 0
					? Result.Success
					: Result.Fail(new InvalidOperationException("Insert operation did not affect any rows."));
			} catch (Exception ex) when (ex.TryToResult(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// return await conn.InsertAndReturnAsync(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the value from <paramref name="resultSelector"/> if at least one row was inserted, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> InsertAndReturnAsync<T>(
			string sql,
			object? parameters,
			Func<T> resultSelector,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected > 0
					? resultSelector()
					: Result.Fail<T>(new InvalidOperationException("Insert operation did not affect any rows."));
			} catch (Exception ex) when (ex.TryToResult<T>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// return await conn.UpdateAsync(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful <see cref="Result"/>
		/// if at least one row was updated, or a failure result with an appropriate exception.</returns>
		public async Task<Result> UpdateAsync(
			string sql,
			object? parameters,
			object key,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected > 0
					? Result.Success
					: Result.NotFound(key);
			} catch (Exception ex) when (ex.TryToResult(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// return await conn.UpdateAndReturnAsync(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
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
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected > 0
					? resultSelector()
					: Result.NotFound<T>(key);
			} catch (Exception ex) when (ex.TryToResult<T>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// return await conn.DeleteAsync(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a successful <see cref="Result"/>
		/// if at least one row was deleted, or a failure result with an appropriate exception.</returns>
		public async Task<Result> DeleteAsync(
			string sql,
			object? parameters,
			object key,
			string foreignKeyMessage = "Cannot delete, record is in use",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected > 0
					? Result.Success
					: Result.Fail(new NotFoundException(key));
			} catch (Exception ex) when (ex.TryToDeleteResult(foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// return await conn.DeleteAndReturnAsync(
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the value from <paramref name="resultSelector"/> if at least one row was deleted, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> DeleteAndReturnAsync<T>(
			string sql,
			object? parameters,
			object key,
			Func<T> resultSelector,
			string foreignKeyMessage = "Cannot delete, record is in use",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected > 0
					? resultSelector()
					: Result.NotFound<T>(key);
			} catch (Exception ex) when (ex.TryToDeleteResult<T>(foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await conn.InsertAndGetAsync&lt;Order&gt;(
		///     """
		///     INSERT INTO Orders (CustomerId, Amount) VALUES (@CustomerId, @Amount);
		///     SELECT * FROM Orders WHERE Id = SCOPE_IDENTITY();
		///     """,
		///     new { command.CustomerId, command.Amount },
		///     uniqueConstraintMessage: "Order already exists",
		///     foreignKeyMessage: "Customer not found",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the row to return.</typeparam>
		/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the selected row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> InsertAndGetAsync<T>(
			string sql,
			object? parameters = null,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<T>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return result is not null
					? result
					: Result.Fail<T>(new InvalidOperationException("Insert and select operation did not return a row."));
			} catch (Exception ex) when (ex.TryToResult<T>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<TModel>> InsertAndGetAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<TData, TModel> mapper,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<TData>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return result is not null
					? mapper(result)
					: Result.Fail<TModel>(new InvalidOperationException("Insert and select operation did not return a row."));
			} catch (Exception ex) when (ex.TryToResult<TModel>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
		}

		/// <summary>
		/// Executes an INSERT command followed by a SELECT in a single batch and returns an <see cref="Optional{T}"/>
		/// containing the selected row if present, allowing the caller to handle the empty case via a mapper.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for conditional INSERT operations (e.g., INSERT...WHERE NOT EXISTS) where no row being
		/// returned is a valid outcome that the caller wants to handle explicitly.
		/// </para>
		/// <para>
		/// <strong>SQL Pattern:</strong>
		/// </para>
		/// <code>
		/// INSERT INTO Orders (CustomerId, Amount)
		/// SELECT @CustomerId, @Amount
		/// WHERE NOT EXISTS (SELECT 1 FROM Orders WHERE CustomerId = @CustomerId AND Status = 'Pending');
		///
		/// SELECT * FROM Orders WHERE Id = SCOPE_IDENTITY();
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await conn.InsertAndGetOptionalAsync&lt;OrderData, OrderResult&gt;(
		///     sql,
		///     new { command.CustomerId, command.Amount },
		///     opt =&gt; opt.Match(
		///         some: data =&gt; new OrderResult.Created(data),
		///         none: () =&gt; new OrderResult.AlreadyHadPending()),
		///     uniqueConstraintMessage: "Order already exists",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch containing INSERT and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the result from the mapper.</returns>
		public async Task<Result<TModel>> InsertAndGetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<Optional<TData>, TModel> mapper,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<TData>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return mapper(Optional.From(result));
			} catch (Exception ex) when (ex.TryToResult<TModel>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await conn.UpdateAndGetAsync&lt;Order&gt;(
		///     """
		///     UPDATE Orders SET Amount = @Amount WHERE Id = @Id;
		///     SELECT * FROM Orders WHERE Id = @Id;
		///     """,
		///     new { command.Id, command.Amount },
		///     key: command.Id,
		///     uniqueConstraintMessage: "Order reference conflict",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the row to return.</typeparam>
		/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being updated, used in the <see cref="NotFoundException"/> if no row is returned.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the selected row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> UpdateAndGetAsync<T>(
			string sql,
			object? parameters,
			object key,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<T>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return Result.FromLookup(result, key);
			} catch (Exception ex) when (ex.TryToResult<T>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
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
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<TData>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				if (result is null) {
					return Result.NotFound<TModel>(key);
				}
				return mapper(result);
			} catch (Exception ex) when (ex.TryToResult<TModel>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
		}

		/// <summary>
		/// Executes an UPDATE command followed by a SELECT in a single batch and returns an <see cref="Optional{T}"/>
		/// containing the selected row if present, allowing the caller to handle the empty case via a mapper.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for conditional UPDATE operations where no row being returned is a valid outcome
		/// that the caller wants to handle explicitly.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL batch containing UPDATE and SELECT statements.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
		/// <param name="uniqueConstraintMessage">The error message to use if a unique constraint violation occurs.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs, or <see langword="null"/> to let the exception propagate.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the result from the mapper.</returns>
		public async Task<Result<TModel>> UpdateAndGetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<Optional<TData>, TModel> mapper,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<TData>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return mapper(Optional.From(result));
			} catch (Exception ex) when (ex.TryToResult<TModel>(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result;
			}
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
		/// DELETE FROM Orders
		/// OUTPUT DELETED.*
		/// WHERE Id = @Id;
		/// </code>
		/// <para>
		/// <strong>SQL Pattern (PostgreSQL with RETURNING):</strong>
		/// </para>
		/// <code>
		/// DELETE FROM Orders WHERE Id = @Id RETURNING *;
		/// </code>
		/// <para>
		/// <strong>Usage Pattern:</strong>
		/// </para>
		/// <code>
		/// return await conn.DeleteAndGetAsync&lt;Order&gt;(
		///     "DELETE FROM Orders OUTPUT DELETED.* WHERE Id = @Id",
		///     new { command.Id },
		///     key: command.Id,
		///     foreignKeyMessage: "Cannot delete order, it has associated line items",
		///     cancellationToken: cancellationToken);
		/// </code>
		/// </remarks>
		/// <typeparam name="T">The type of the row to return.</typeparam>
		/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="key">The key of the entity being deleted, used in the <see cref="NotFoundException"/> if no row is returned.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the deleted row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<T>> DeleteAndGetAsync<T>(
			string sql,
			object? parameters,
			object key,
			string foreignKeyMessage = "Cannot delete, record is in use",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<T>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return Result.FromLookup(result, key);
			} catch (Exception ex) when (ex.TryToDeleteResult(foreignKeyMessage, out var deleteResult)) {
				return deleteResult.Error!;
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the mapped row if successful, or a failure result with an appropriate exception.</returns>
		public async Task<Result<TModel>> DeleteAndGetAsync<TData, TModel>(
			string sql,
			object? parameters,
			object key,
			Func<TData, TModel> mapper,
			string foreignKeyMessage = "Cannot delete, record is in use",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<TData>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				if (result is null) {
					return Result.NotFound<TModel>(key);
				}
				return mapper(result);
			} catch (Exception ex) when (ex.TryToDeleteResult(foreignKeyMessage, out var deleteResult)) {
				return deleteResult.Error!;
			}
		}

		/// <summary>
		/// Executes a DELETE command and returns an <see cref="Optional{T}"/> containing the deleted row if present,
		/// allowing the caller to handle the empty case via a mapper.
		/// </summary>
		/// <remarks>
		/// <para>
		/// Use this method for conditional DELETE operations where no row being returned is a valid outcome
		/// that the caller wants to handle explicitly.
		/// </para>
		/// </remarks>
		/// <typeparam name="TData">The type of the row returned by the SQL query (data layer).</typeparam>
		/// <typeparam name="TModel">The type of the object in the final result (domain layer).</typeparam>
		/// <param name="sql">The SQL DELETE statement with OUTPUT or RETURNING clause.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL command, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">A function that receives an <see cref="Optional{T}"/> and returns the final result.</param>
		/// <param name="foreignKeyMessage">The error message to use if a foreign key violation occurs.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="Result{T}"/>
		/// with the result from the mapper.</returns>
		public async Task<Result<TModel>> DeleteAndGetOptionalAsync<TData, TModel>(
			string sql,
			object? parameters,
			Func<Optional<TData>, TModel> mapper,
			string foreignKeyMessage = "Cannot delete, record is in use",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var result = await conn.QuerySingleOrDefaultAsync<TData>(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return mapper(Optional.From(result));
			} catch (Exception ex) when (ex.TryToDeleteResult(foreignKeyMessage, out var deleteResult)) {
				return deleteResult.Error!;
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> with the number of rows affected.</returns>
		public async Task<Result<int>> InsertWithCountAsync(
			string sql,
			object? parameters = null,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected;
			} catch (Exception ex) when (ex.TryToResult(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result.Error!;
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> with the number of rows affected.</returns>
		public async Task<Result<int>> UpdateWithCountAsync(
			string sql,
			object? parameters = null,
			string uniqueConstraintMessage = "Record already exists",
			string? foreignKeyMessage = "Referenced record does not exist",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected;
			} catch (Exception ex) when (ex.TryToResult(uniqueConstraintMessage, foreignKeyMessage, out var result)) {
				return result.Error!;
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> with the number of rows affected.</returns>
		public async Task<Result<int>> DeleteWithCountAsync(
			string sql,
			object? parameters = null,
			string foreignKeyMessage = "Cannot delete, record is in use",
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			try {
				var rowsAffected = await conn.ExecuteAsync(
					sql,
					parameters,
					transaction: transaction,
					cancellationToken: cancellationToken);

				return rowsAffected;
			} catch (Exception ex) when (ex.TryToDeleteResult(foreignKeyMessage, out var result)) {
				return result.Error!;
			}
		}

		#endregion

		#region EXECUTE

		/// <summary>
		/// Executes the specified asynchronous operation using a <see cref="DbContext"/>
		/// wrapper over the current connection.
		/// </summary>
		/// <param name="action">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is passed into the created <see cref="DbContext"/> and can be observed by the operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result"/> produced by <paramref name="action"/>.
		/// Database exceptions are converted to appropriate failure results; all other exceptions are
		/// captured and returned as <see cref="Result.Fail(Exception)"/>.
		/// </returns>
		/// <remarks>
		/// <para>
		/// This method does not create, open, or dispose the underlying connection; it only wraps the current connection
		/// in a <see cref="DbContext"/> and invokes the delegate.
		/// </para>
		/// <para>
		/// Exceptions thrown by <paramref name="action"/> are translated into failure results rather than being rethrown.
		/// </para>
		/// </remarks>
		public async Task<Result> ExecuteAsync(
			Func<DbContext, Task<Result>> action,
			CancellationToken cancellationToken = default) {

			var context = new DbContext(conn, null, cancellationToken);

			try {
				return await action(context).ConfigureAwait(false);
			} catch (Exception ex) {
				return ex.ToResult();
			}
		}

		/// <summary>
		/// Executes the specified asynchronous operation using a <see cref="DbContext"/>
		/// wrapper over the current connection and returns a typed result.
		/// </summary>
		/// <typeparam name="T">The value type carried by the returned <see cref="Result{T}"/>.</typeparam>
		/// <param name="action">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result{T}"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is passed into the created <see cref="DbContext"/> and can be observed by the operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result{T}"/> produced by <paramref name="action"/>.
		/// Database exceptions are converted to appropriate failure results. Any other exception is
		/// captured and returned as <see cref="Result.Fail{T}(Exception)"/>.
		/// </returns>
		/// <remarks>
		/// <para>
		/// This method does not create, open, or dispose the underlying connection; it only wraps the current connection
		/// in a <see cref="DbContext"/> and invokes the delegate.
		/// </para>
		/// <para>
		/// Exceptions thrown by <paramref name="action"/> are translated into failure results rather than being rethrown.
		/// </para>
		/// </remarks>
		public async Task<Result<T>> ExecuteAsync<T>(
			Func<DbContext, Task<Result<T>>> action,
			CancellationToken cancellationToken = default) {

			var context = new DbContext(conn, null, cancellationToken);

			try {
				return await action(context).ConfigureAwait(false);
			} catch (Exception ex) {
				return ex.ToResult<T>();
			}
		}

		#endregion

		#region EXECUTE TRANSACTION

		/// <summary>
		/// Executes the specified asynchronous operation within a database transaction.
		/// </summary>
		/// <param name="action">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is passed into the created <see cref="DbContext"/> and can be observed by the operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result"/> produced by <paramref name="action"/>.
		/// On success, the transaction is committed; otherwise it is rolled back.
		/// Database exceptions are converted to appropriate failure results after rolling back.
		/// Any other exception is captured and returned as <see cref="Result.Fail(Exception)"/> after rolling back.
		/// </returns>
		/// <remarks>
		/// <para>
		/// A transaction is started via <c>conn.BeginTransaction()</c> and is disposed when the operation completes.
		/// </para>
		/// <para>
		/// Commit/rollback behavior is determined by the returned result:
		/// the transaction is committed when <see cref="Result.IsSuccess"/> is <c>true</c>;
		/// otherwise it is rolled back.
		/// </para>
		/// <para>
		/// Exceptions thrown by <paramref name="action"/> are translated into failure results rather than being rethrown.
		/// </para>
		/// </remarks>
		public async Task<Result> ExecuteTransactionAsync(
			Func<DbContext, Task<Result>> action,
			CancellationToken cancellationToken = default) {

			using var transaction = conn.BeginTransaction();
			var context = new DbContext(conn, transaction, cancellationToken);

			try {
				var result = await action(context).ConfigureAwait(false);

				if (result.IsSuccess) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}

				return result;
			} catch (Exception ex) {
				transaction.Rollback();
				return ex.ToResult();
			}
		}

		/// <summary>
		/// Executes the specified asynchronous operation within a database transaction and returns a typed result.
		/// </summary>
		/// <typeparam name="T">The value type carried by the returned <see cref="Result{T}"/>.</typeparam>
		/// <param name="action">
		/// A delegate that performs work using the provided <see cref="DbContext"/> and returns a <see cref="Result{T}"/>
		/// describing the outcome.
		/// </param>
		/// <param name="cancellationToken">
		/// A token that is passed into the created <see cref="DbContext"/> and can be observed by the operation.
		/// </param>
		/// <returns>
		/// A task that completes with the <see cref="Result{T}"/> produced by <paramref name="action"/>.
		/// On success, the transaction is committed; otherwise it is rolled back.
		/// Database exceptions are converted to appropriate failure results after rolling back.
		/// Any other exception is captured and returned as <see cref="Result.Fail{T}(Exception)"/> after rolling back.
		/// </returns>
		/// <remarks>
		/// <para>
		/// A transaction is started via <c>conn.BeginTransaction()</c> and is disposed when the operation completes.
		/// </para>
		/// <para>
		/// Commit/rollback behavior is determined by the returned result:
		/// the transaction is committed when <see cref="Result.IsSuccess"/> is <c>true</c>;
		/// otherwise it is rolled back.
		/// </para>
		/// <para>
		/// Exceptions thrown by <paramref name="action"/> are translated into failure results rather than being rethrown.
		/// </para>
		/// </remarks>
		public async Task<Result<T>> ExecuteTransactionAsync<T>(
			Func<DbContext, Task<Result<T>>> action,
			CancellationToken cancellationToken = default) {

			using var transaction = conn.BeginTransaction();
			var context = new DbContext(conn, transaction, cancellationToken);

			try {
				var result = await action(context).ConfigureAwait(false);

				if (result.IsSuccess) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}

				return result;
			} catch (Exception ex) {
				transaction.Rollback();
				return ex.ToResult<T>();
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if the mapper returns null.</returns>
		public Task<Result<T>> MultipleGetAsync<T>(
			string sql,
			object[] keys,
			Func<IMultipleResult, Task<T?>> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.MultipleGetAsync(sql, null, keys, mapper, transaction, cancellationToken);

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns a failure with <see cref="NotFoundException"/> if the mapper returns null.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="keys">The keys used to identify the resource for the <see cref="NotFoundException"/>.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/> object with the
		/// mapped value, or a NotFound result if the mapper returns null.</returns>
		public async Task<Result<T>> MultipleGetAsync<T>(
			string sql,
			object? parameters,
			object[] keys,
			Func<IMultipleResult, Task<T?>> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			IMultipleResult reader;
			try {
				reader = await conn
					.QueryMultipleAsync(sql, parameters, transaction, cancellationToken)
					.ConfigureAwait(false);
			} catch (Exception ex) {
				return ex.ToResult<T>();
			}

			await using (reader) {
				T? result;
				try {
					result = await mapper(reader).ConfigureAwait(false);
				} catch (Exception ex) {
					return ex.ToResult<T>();
				}

				return result is null
					? Result.NotFound<T>(keys)
					: Result<T>.Success(result);
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// with an <see cref="Optional{T}"/> that is empty if the mapper returns null, or contains the value otherwise.</returns>
		public Task<Result<Optional<T>>> MultipleGetOptionalAsync<T>(
			string sql,
			Func<IMultipleResult, Task<T?>> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.MultipleGetOptionalAsync(sql, null, mapper, transaction, cancellationToken);

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns an <see cref="Optional{T}"/> that is empty if the mapper returns null.
		/// </summary>
		/// <typeparam name="T">The type of the object to be returned.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the mapped value.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// with an <see cref="Optional{T}"/> that is empty if the mapper returns null, or contains the value otherwise.</returns>
		public async Task<Result<Optional<T>>> MultipleGetOptionalAsync<T>(
			string sql,
			object? parameters,
			Func<IMultipleResult, Task<T?>> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			IMultipleResult reader;
			try {
				reader = await conn
					.QueryMultipleAsync(sql, parameters, transaction, cancellationToken)
					.ConfigureAwait(false);
			} catch (Exception ex) {
				return ex.ToResult<Optional<T>>();
			}

			await using (reader) {
				T? result;
				try {
					result = await mapper(reader).ConfigureAwait(false);
				} catch (Exception ex) {
					return ex.ToResult<Optional<T>>();
				}

				return Optional.From(result);
			}
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
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// wrapping a read-only list of items (which may be empty).</returns>
		public Task<Result<IReadOnlyList<T>>> MultipleQueryAnyAsync<T>(
			string sql,
			Func<IMultipleResult, Task<IReadOnlyList<T>?>> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default)
			=> conn.MultipleQueryAnyAsync(sql, null, mapper, transaction, cancellationToken);

		/// <summary>
		/// Executes a query returning multiple result sets and processes them using the provided mapper.
		/// Returns the list from the mapper; an empty list is a valid result.
		/// </summary>
		/// <typeparam name="T">The type of the elements in the returned list.</typeparam>
		/// <param name="sql">The SQL query to execute. Should return multiple result sets.</param>
		/// <param name="parameters">An object containing the parameters to be passed to the SQL query, or <see langword="null"/> if no parameters are required.</param>
		/// <param name="mapper">An async function that reads from the <see cref="IMultipleResult"/> and returns the list.</param>
		/// <param name="transaction">An optional transaction within which the command executes.</param>
		/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous operation. The result contains a <see cref="Result{T}"/>
		/// wrapping a read-only list of items (which may be empty).</returns>
		public async Task<Result<IReadOnlyList<T>>> MultipleQueryAnyAsync<T>(
			string sql,
			object? parameters,
			Func<IMultipleResult, Task<IReadOnlyList<T>?>> mapper,
			IDbTransaction? transaction = null,
			CancellationToken cancellationToken = default) {

			IMultipleResult reader;
			try {
				reader = await conn.QueryMultipleAsync(sql, parameters, transaction, cancellationToken)
					.ConfigureAwait(false);
			} catch (Exception ex) {
				return ex.ToResult<IReadOnlyList<T>>();
			}

			await using (reader) {
				IReadOnlyList<T>? result;
				try {
					result = await mapper(reader).ConfigureAwait(false);
				} catch (Exception ex) {
					return ex.ToResult<IReadOnlyList<T>>();
				}

				return Result.From(result ?? []);
			}
		}

		#endregion

	}

}