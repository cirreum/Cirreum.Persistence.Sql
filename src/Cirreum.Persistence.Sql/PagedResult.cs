namespace Cirreum.Persistence;

/// <summary>
/// Represents a paginated result set using offset-based pagination.
/// </summary>
/// <remarks>
/// <para>
/// Use offset pagination when clients need to jump to arbitrary pages or display total counts.
/// This approach works well for smaller datasets and traditional paged interfaces with numbered pages.
/// </para>
/// <para>
/// <strong>SQL Pattern:</strong>
/// </para>
/// <code>
/// -- Query 1: Get total count
/// SELECT COUNT(*) 
/// FROM Orders 
/// WHERE CustomerId = @CustomerId
///
/// -- Query 2: Get page data
/// SELECT *
/// FROM Orders
/// WHERE CustomerId = @CustomerId
/// ORDER BY CreatedAt DESC
/// OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY
/// </code>
/// <para>
/// <strong>Usage Pattern:</strong>
/// </para>
/// <code>
/// var offset = (query.PageNumber - 1) * query.PageSize;
///
/// var totalCount = await conn.ExecuteScalarAsync&lt;int&gt;(
///     "SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId",
///     new { query.CustomerId });
///
/// return await conn.QueryPagedAsync&lt;Order&gt;(
///     "SELECT ... ORDER BY CreatedAt DESC OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY",
///     new { query.CustomerId, Offset = offset, query.PageSize },
///     totalCount,
///     query.PageSize,
///     query.PageNumber,
///     cancellationToken);
/// </code>
/// <para>
/// <strong>Trade-offs:</strong>
/// </para>
/// <list type="bullet">
///   <item><description>Requires two queries (count + data)</description></item>
///   <item><description>Performance degrades on deep pages (high OFFSET values)</description></item>
///   <item><description>Results can shift if data is inserted/deleted between requests</description></item>
/// </list>
/// <para>
/// For large datasets, real-time data, or infinite scroll interfaces where consistency matters,
/// consider <see cref="CursorResult{T}"/> instead. For simple "load more" without counts,
/// consider <see cref="SliceResult{T}"/>.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of items in the result set.</typeparam>
/// <param name="Items">The items for the current page.</param>
/// <param name="TotalCount">The total number of items across all pages.</param>
/// <param name="PageSize">The maximum number of items per page.</param>
/// <param name="PageNumber">The current page number (1-based).</param>
public sealed record PagedResult<T>(
	IReadOnlyList<T> Items,
	int TotalCount,
	int PageSize,
	int PageNumber) {

	/// <summary>
	/// Gets the number of items contained in the current page.
	/// </summary>
	public int Count => this.Items.Count;

	/// <summary>
	/// Gets the total number of pages available.
	/// </summary>
	public int TotalPages => this.PageSize > 0
		? (int)Math.Ceiling((double)this.TotalCount / this.PageSize)
		: 0;

	/// <summary>
	/// Gets a value indicating whether there is a subsequent page.
	/// </summary>
	public bool HasNextPage => this.PageNumber < this.TotalPages;

	/// <summary>
	/// Gets a value indicating whether there is a preceding page.
	/// </summary>
	public bool HasPreviousPage => this.PageNumber > 1;
}