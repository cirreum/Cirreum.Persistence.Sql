namespace Cirreum.Persistence;

using System.Text;
using System.Text.Json;

/// <summary>
/// Represents a generic cursor for single-column sorting with a unique identifier tie-breaker.
/// </summary>
/// <remarks>
/// <para>
/// This cursor type is suitable for the common pagination scenario where results are sorted by a single
/// column (such as a date or timestamp) with a <see cref="Guid"/> identifier used as a tie-breaker when
/// sort column values are equal.
/// </para>
/// <para>
/// For composite sorting scenarios involving multiple columns or non-Guid identifiers, define a custom
/// cursor record and use <see cref="Cursor.DecodeAs{T}"/> for decoding.
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
/// <strong>Recommended Index:</strong>
/// </para>
/// <code>
/// CREATE INDEX IX_Orders_Customer_Created 
/// ON Orders (CustomerId, CreatedAt DESC, OrderId DESC)
/// INCLUDE (...) -- columns being selected
/// </code>
/// <para>
/// For ascending sort, flip the comparison operators from <c>&lt;</c> to <c>&gt;</c> and the ORDER BY direction.
/// </para>
/// </remarks>
/// <typeparam name="TColumn">The type of the sort column value.</typeparam>
/// <param name="Column">The value of the sort column at the cursor position.</param>
/// <param name="Id">The unique identifier used as a tie-breaker when sort column values are equal.</param>
public sealed record Cursor<TColumn>(TColumn Column, Guid Id);

/// <summary>
/// Provides encoding and decoding utilities for cursor-based pagination tokens.
/// </summary>
/// <remarks>
/// <para>
/// Cursors are opaque tokens that encode the position within a sorted result set. They enable efficient
/// keyset pagination by allowing the database to seek directly to a position rather than scanning and
/// discarding rows.
/// </para>
/// <para>
/// Cursor values are encoded as URL-safe base64 JSON strings. Clients should treat cursors as opaque and not
/// attempt to parse, construct, or modify them directly.
/// </para>
/// </remarks>
public static class Cursor {

	/// <summary>
	/// Encodes a cursor value as a URL-safe base64 string.
	/// </summary>
	/// <typeparam name="T">The type of the cursor data.</typeparam>
	/// <param name="data">The cursor data to encode.</param>
	/// <returns>A URL-safe base64-encoded string representing the cursor.</returns>
	public static string Encode<T>(T data) {
		var json = JsonSerializer.Serialize(data);
		return Base64UrlEncode(Encoding.UTF8.GetBytes(json));
	}

	/// <summary>
	/// Encodes a sort column value and identifier as a URL-safe cursor string.
	/// </summary>
	/// <remarks>
	/// This is a convenience method for the common case of single-column sorting with a <see cref="Guid"/>
	/// tie-breaker. The resulting cursor can be decoded using <see cref="Decode{TColumn}"/>.
	/// </remarks>
	/// <typeparam name="TColumn">The type of the sort column value.</typeparam>
	/// <param name="column">The value of the sort column at the cursor position.</param>
	/// <param name="id">The unique identifier used as a tie-breaker.</param>
	/// <returns>A URL-safe base64-encoded string representing the cursor.</returns>
	public static string Encode<TColumn>(TColumn column, Guid id) {
		return Encode(new Cursor<TColumn>(column, id));
	}

	/// <summary>
	/// Decodes a cursor string into a <see cref="Cursor{TColumn}"/>.
	/// </summary>
	/// <remarks>
	/// Use this method to decode cursors created with <see cref="Encode{TColumn}(TColumn, Guid)"/>.
	/// For custom cursor types, use <see cref="DecodeAs{T}"/> instead.
	/// </remarks>
	/// <typeparam name="TColumn">The type of the sort column value.</typeparam>
	/// <param name="cursor">The URL-safe base64-encoded cursor string, or null.</param>
	/// <returns>The decoded cursor, or null if the cursor is null, empty, or invalid.</returns>
	public static Cursor<TColumn>? Decode<TColumn>(string? cursor) {
		return DecodeAs<Cursor<TColumn>>(cursor);
	}

	/// <summary>
	/// Decodes a cursor string into a custom cursor type.
	/// </summary>
	/// <remarks>
	/// Use this method when working with custom cursor records that contain multiple sort columns
	/// or non-Guid identifiers.
	/// </remarks>
	/// <typeparam name="T">The type of the cursor data.</typeparam>
	/// <param name="cursor">The URL-safe base64-encoded cursor string, or null.</param>
	/// <returns>The decoded cursor data, or null if the cursor is null, empty, or invalid.</returns>
	public static T? DecodeAs<T>(string? cursor) where T : class {
		if (string.IsNullOrEmpty(cursor)) {
			return null;
		}

		try {
			var bytes = Base64UrlDecode(cursor);
			var json = Encoding.UTF8.GetString(bytes);
			return JsonSerializer.Deserialize<T>(json);
		} catch {
			return null;
		}
	}

	/// <summary>
	/// Decodes a cursor string into a custom value type cursor.
	/// </summary>
	/// <remarks>
	/// Use this method when working with custom cursor value types (structs or records structs).
	/// </remarks>
	/// <typeparam name="T">The type of the cursor data.</typeparam>
	/// <param name="cursor">The URL-safe base64-encoded cursor string, or null.</param>
	/// <returns>The decoded cursor data, or null if the cursor is null, empty, or invalid.</returns>
	public static T? DecodeAsValue<T>(string? cursor) where T : struct {
		if (string.IsNullOrEmpty(cursor)) {
			return null;
		}

		try {
			var bytes = Base64UrlDecode(cursor);
			var json = Encoding.UTF8.GetString(bytes);
			return JsonSerializer.Deserialize<T>(json);
		} catch {
			return null;
		}
	}

	private static string Base64UrlEncode(byte[] bytes) {
		return Convert.ToBase64String(bytes)
			.TrimEnd('=')
			.Replace('+', '-')
			.Replace('/', '_');
	}

	private static byte[] Base64UrlDecode(string input) {
		var base64 = input
			.Replace('-', '+')
			.Replace('_', '/');

		switch (base64.Length % 4) {
			case 2:
				base64 += "==";
				break;
			case 3:
				base64 += "=";
				break;
		}

		return Convert.FromBase64String(base64);
	}

}