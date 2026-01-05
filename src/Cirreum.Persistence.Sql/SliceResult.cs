namespace Cirreum.Persistence;

/// <summary>
/// Represents a slice of results with an indicator for whether more items exist.
/// </summary>
/// <remarks>
/// <para>
/// Use this when you need a simple "load more" pattern without full pagination metadata.
/// No count query required — just fetch N+1 rows and check if more exist.
/// </para>
/// <para>
/// This is ideal for scenarios where you don't need cursor stability or page numbers,
/// such as loading initial data with a "Show More" button, or batch processing.
/// </para>
/// <para>
/// <strong>SQL Pattern:</strong>
/// </para>
/// <code>
/// -- Fetch pageSize + 1 to detect if more exist
/// SELECT TOP (@PageSize) *
/// FROM Orders
/// WHERE CustomerId = @CustomerId
/// ORDER BY CreatedAt DESC
/// </code>
/// <para>
/// <strong>Usage Pattern:</strong>
/// </para>
/// <code>
/// // Extension method handles +1 internally
/// return await conn.QuerySliceAsync&lt;Order&gt;(
///     "SELECT TOP (@PageSize) ... ORDER BY CreatedAt DESC",
///     new { query.CustomerId },
///     query.PageSize,
///     cancellationToken);
/// </code>
/// <para>
/// For stable pagination across requests (e.g., infinite scroll with inserts/deletes),
/// consider <see cref="CursorResult{T}"/> instead. For full pagination metadata with
/// page numbers and total counts, consider <see cref="PagedResult{T}"/>.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of items in the result set.</typeparam>
/// <param name="Items">The items for the current slice.</param>
/// <param name="HasMore">A value indicating whether additional items exist beyond this slice.</param>
public sealed record SliceResult<T>(
	IReadOnlyList<T> Items,
	bool HasMore) {

	/// <summary>
	/// Gets the number of items contained in the current slice.
	/// </summary>
	public int Count => this.Items.Count;
}