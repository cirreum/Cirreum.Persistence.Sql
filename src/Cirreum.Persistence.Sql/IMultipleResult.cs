namespace Cirreum.Persistence;

/// <summary>
/// Represents a reader for multiple result sets returned from a single SQL query.
/// </summary>
/// <remarks>
/// <para>
/// Use this interface to read multiple result sets from a single query, reducing database round trips
/// when you need to fetch related data together.
/// </para>
/// <para>
/// Results must be read in order. Each Read method advances to the next result set.
/// </para>
/// </remarks>
public interface IMultipleResult : IAsyncDisposable {

	/// <summary>
	/// Gets a value indicating whether all result sets have been consumed.
	/// </summary>
	bool IsConsumed { get; }

	/// <summary>
	/// Reads the next result set as a single required item.
	/// </summary>
	/// <typeparam name="T">The type of the object to return.</typeparam>
	/// <returns>The single item from the result set, or null if no rows were returned.</returns>
	Task<T?> ReadSingleOrDefaultAsync<T>();

	/// <summary>
	/// Reads the next result set as a single item (takes first if multiple exist).
	/// </summary>
	/// <typeparam name="T">The type of the object to return.</typeparam>
	/// <returns>The first item from the result set, or null if no rows were returned.</returns>
	Task<T?> ReadFirstOrDefaultAsync<T>();

	/// <summary>
	/// Reads the next result set as a collection.
	/// </summary>
	/// <typeparam name="T">The type of the elements to return.</typeparam>
	/// <param name="buffered">
	/// Whether to buffer results in memory. Defaults to true.
	/// When false, the results are streamed and must be fully consumed before reading the next result set.
	/// </param>
	/// <returns>A collection of items from the result set.</returns>
	Task<IEnumerable<T>> ReadAsync<T>(bool buffered = true);

}
