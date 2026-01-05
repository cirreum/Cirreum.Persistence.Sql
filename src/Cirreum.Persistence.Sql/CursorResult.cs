namespace Cirreum.Persistence;

/// <summary>
/// Represents a paginated result set using cursor-based (keyset) pagination.
/// </summary>
/// <remarks>
/// <para>
/// Cursor pagination provides stable results when data is being inserted or deleted, and performs
/// consistently regardless of how deep into the result set the client navigates. This makes it
/// ideal for large datasets, real-time data, and infinite scroll interfaces.
/// </para>
/// <para>
/// The cursor is an opaque token encoding the sort key(s) of the boundary item. Clients should
/// treat cursors as opaque strings and not attempt to parse or construct them. Use the
/// <see cref="Cursor"/> helper class to encode and decode cursor values.
/// </para>
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
///     new { cursor?.Column, cursor?.Id },
///     query.PageSize,
///     o =&gt; (o.CreatedAt, o.Id),
///     cancellationToken);
/// </code>
/// <para>
/// For scenarios requiring arbitrary page jumps or total counts, consider <see cref="PagedResult{T}"/> instead.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of items in the result set.</typeparam>
/// <param name="Items">The items for the current page.</param>
/// <param name="NextCursor">The cursor to fetch the next page, or null if there are no more items.</param>
/// <param name="HasNextPage">A value indicating whether there is a subsequent page.</param>
public sealed record CursorResult<T>(
	IReadOnlyList<T> Items,
	string? NextCursor,
	bool HasNextPage) {

	/// <summary>
	/// Gets the number of items contained in the current page.
	/// </summary>
	public int Count => this.Items.Count;

	/// <summary>
	/// Gets or sets the cursor to fetch the previous page, or null if this is the first page.
	/// </summary>
	public string? PreviousCursor { get; init; }

	/// <summary>
	/// Gets a value indicating whether there is a preceding page.
	/// </summary>
	public bool HasPreviousPage => this.PreviousCursor is not null;

	/// <summary>
	/// Gets or sets the total number of items across all pages, if known.
	/// </summary>
	/// <remarks>
	/// This value is optional and may be null if computing the total count is too expensive.
	/// </remarks>
	public int? TotalCount { get; init; }
}