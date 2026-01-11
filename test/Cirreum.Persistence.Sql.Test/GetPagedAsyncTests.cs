namespace Cirreum.Persistence.Sql.Test;

using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Data;

[TestClass]
public sealed class GetPagedAsyncTests {

	private static (ISqlConnection conn, SqliteConnection rawConn) CreateConnection() {
		var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		return (new SqliteSqlConnection(connection), connection);
	}

	private static void CreateTestSchema(IDbConnection connection) {
		connection.Execute("""
			CREATE TABLE Products (
				Id INTEGER PRIMARY KEY,
				Name TEXT NOT NULL,
				Price REAL NOT NULL
			);

			CREATE TABLE Categories (
				Id INTEGER PRIMARY KEY,
				Name TEXT NOT NULL
			);
			""");
	}

	private static void SeedProducts(IDbConnection connection, int count) {
		for (var i = 1; i <= count; i++) {
			connection.Execute(
				"INSERT INTO Products (Id, Name, Price) VALUES (@Id, @Name, @Price)",
				new { Id = i, Name = $"Product {i}", Price = i * 10.0 });
		}
	}

	public TestContext TestContext { get; set; } = null!;

	#region ISqlConnection.GetPagedAsync

	[TestMethod]
	public async Task GetPagedAsync_WithExplicitPaging_ReturnsCorrectPage() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		SeedProducts(rawConn, 25);

		// SQLite uses LIMIT/OFFSET syntax - include OFFSET in SQL to prevent auto-append of SQL Server syntax
		const string sql = """
			SELECT COUNT(*) FROM Products;
			SELECT Id, Name, Price FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		// Act - get page 2 with 10 items per page
		var result = await conn.GetPagedAsync<ProductDto>(
			sql,
			pageSize: 10,
			page: 2,
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(25, result.Value.TotalCount);
		Assert.AreEqual(10, result.Value.PageSize);
		Assert.AreEqual(3, result.Value.TotalPages);
		Assert.HasCount(10, result.Value.Items);
		Assert.AreEqual("Product 11", result.Value.Items[0].Name); // First item on page 2
		Assert.AreEqual("Product 20", result.Value.Items[9].Name); // Last item on page 2
	}

	[TestMethod]
	public async Task GetPagedAsync_WithParametersObject_ReturnsCorrectPage() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		SeedProducts(rawConn, 15);

		const string sql = """
			SELECT COUNT(*) FROM Products WHERE Price >= @MinPrice;
			SELECT Id, Name, Price FROM Products WHERE Price >= @MinPrice ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		var request = new { MinPrice = 50.0, PageSize = 5, Page = 2 };

		// Act
		var result = await conn.GetPagedAsync<ProductDto>(
			sql,
			request,
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(11, result.Value.TotalCount); // Products 5-15 have Price >= 50
		Assert.AreEqual(5, result.Value.PageSize);
		Assert.AreEqual(3, result.Value.TotalPages);
		Assert.HasCount(5, result.Value.Items);
		Assert.AreEqual("Product 10", result.Value.Items[0].Name); // First item on page 2
	}

	[TestMethod]
	public async Task GetPagedAsync_WithMapper_TransformsResults() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		SeedProducts(rawConn, 10);

		const string sql = """
			SELECT COUNT(*) FROM Products;
			SELECT Id, Name, Price FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		// Act
		var result = await conn.GetPagedAsync<ProductDto, Product>(
			sql,
			new { PageSize = 5, Page = 1 },
			dto => new Product(dto.Id, dto.Name.ToUpperInvariant(), dto.Price),
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(10, result.Value.TotalCount);
		Assert.AreEqual("PRODUCT 1", result.Value.Items[0].Name);
	}

	[TestMethod]
	public async Task GetPagedAsync_LastPage_ReturnsPartialResults() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		SeedProducts(rawConn, 23);

		const string sql = """
			SELECT COUNT(*) FROM Products;
			SELECT Id, Name, Price FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		// Act - get page 3 (last page) with 10 items per page
		var result = await conn.GetPagedAsync<ProductDto>(
			sql,
			pageSize: 10,
			page: 3,
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(23, result.Value.TotalCount);
		Assert.AreEqual(3, result.Value.TotalPages);
		Assert.HasCount(3, result.Value.Items); // Only 3 items on last page
		Assert.AreEqual("Product 21", result.Value.Items[0].Name);
		Assert.AreEqual("Product 23", result.Value.Items[2].Name);
	}

	[TestMethod]
	public async Task GetPagedAsync_EmptyResults_ReturnsEmptyPage() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		// No products seeded

		const string sql = """
			SELECT COUNT(*) FROM Products;
			SELECT Id, Name, Price FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		// Act
		var result = await conn.GetPagedAsync<ProductDto>(
			sql,
			pageSize: 10,
			page: 1,
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(0, result.Value.TotalCount);
		Assert.AreEqual(0, result.Value.TotalPages);
		Assert.IsEmpty(result.Value.Items);
	}

	#endregion

	#region DbContext.GetPagedAsync

	[TestMethod]
	public async Task DbContext_GetPagedAsync_ReturnsCorrectPage() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		SeedProducts(rawConn, 20);

		var ctx = new DbContext(conn, null, this.TestContext.CancellationToken);

		const string sql = """
			SELECT COUNT(*) FROM Products;
			SELECT Id, Name, Price FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		// Act
		var result = await ctx.GetPagedAsync<ProductDto>(sql, new { PageSize = 5, Page = 2 }).Result;

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(20, result.Value.TotalCount);
		Assert.HasCount(5, result.Value.Items);
		Assert.AreEqual("Product 6", result.Value.Items[0].Name);
	}

	#endregion

	#region DbResult.ThenGetPagedAsync

	[TestMethod]
	public async Task ThenGetPagedAsync_WithParametersFactory_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Categories (Id, Name) VALUES (1, 'Electronics')");
		rawConn.Execute("ALTER TABLE Products ADD COLUMN CategoryId INTEGER REFERENCES Categories(Id)");
		SeedProducts(rawConn, 15);
		rawConn.Execute("UPDATE Products SET CategoryId = 1");

		var ctx = new DbContext(conn, null, this.TestContext.CancellationToken);

		// Act - Get category first, then get its products paged
		var result = await ctx
			.GetAsync<CategoryDto>("SELECT Id, Name FROM Categories WHERE Id = @Id", new { Id = 1 }, 1)
			.ThenGetPagedAsync<ProductDto>(
				"""
				SELECT COUNT(*) FROM Products WHERE CategoryId = @CategoryId;
				SELECT Id, Name, Price FROM Products WHERE CategoryId = @CategoryId ORDER BY Id LIMIT @PageSize OFFSET @Offset
				""",
				category => new { CategoryId = category.Id, PageSize = 5, Page = 2 })
			.Result;

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(15, result.Value.TotalCount);
		Assert.HasCount(5, result.Value.Items);
		Assert.AreEqual("Product 6", result.Value.Items[0].Name);
	}

	[TestMethod]
	public async Task ThenGetPagedAsync_WithParametersObject_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Categories (Id, Name) VALUES (1, 'Electronics')");
		rawConn.Execute("ALTER TABLE Products ADD COLUMN CategoryId INTEGER REFERENCES Categories(Id)");
		SeedProducts(rawConn, 12);
		rawConn.Execute("UPDATE Products SET CategoryId = 1");

		var ctx = new DbContext(conn, null, this.TestContext.CancellationToken);

		var request = new GetCategoryProductsRequest { CategoryId = 1, PageSize = 4, Page = 3 };

		// Act - Get category first, then get its products paged using the request object
		var result = await ctx
			.GetAsync<CategoryDto>("SELECT Id, Name FROM Categories WHERE Id = @CategoryId", request, request.CategoryId)
			.ThenGetPagedAsync<ProductDto>(
				"""
				SELECT COUNT(*) FROM Products WHERE CategoryId = @CategoryId;
				SELECT Id, Name, Price FROM Products WHERE CategoryId = @CategoryId ORDER BY Id LIMIT @PageSize OFFSET @Offset
				""",
				_ => request)
			.Result;

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(12, result.Value.TotalCount);
		Assert.AreEqual(4, result.Value.PageSize);
		Assert.AreEqual(3, result.Value.TotalPages);
		Assert.HasCount(4, result.Value.Items);
		Assert.AreEqual("Product 9", result.Value.Items[0].Name);
	}

	[TestMethod]
	public async Task ThenGetPagedAsync_WhenFirstQueryFails_PropagatesFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		SeedProducts(rawConn, 10);

		var ctx = new DbContext(conn, null, this.TestContext.CancellationToken);

		// Act - First query fails (category doesn't exist)
		var result = await ctx
			.GetAsync<CategoryDto>("SELECT Id, Name FROM Categories WHERE Id = @Id", new { Id = 999 }, 999)
			.ThenGetPagedAsync<ProductDto>(
				"SELECT COUNT(*) FROM Products; SELECT * FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset",
				new { PageSize = 5, Page = 1 })
			.Result;

		// Assert
		Assert.IsTrue(result.IsFailure);
	}

	#endregion

	#region ParameterHelper.MergeWithPaging validation

	[TestMethod]
	public async Task GetPagedAsync_WithMissingPageSize_ThrowsArgumentException() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		const string sql = """
			SELECT COUNT(*) FROM Products;
			SELECT Id, Name, Price FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		// Act & Assert
		await Assert.ThrowsExactlyAsync<ArgumentException>(async () =>
			await conn.GetPagedAsync<ProductDto>(
				sql,
				new { Page = 1 }, // Missing PageSize
				cancellationToken: this.TestContext.CancellationToken));
	}

	[TestMethod]
	public async Task GetPagedAsync_WithMissingPage_ThrowsArgumentException() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		const string sql = """
			SELECT COUNT(*) FROM Products;
			SELECT Id, Name, Price FROM Products ORDER BY Id LIMIT @PageSize OFFSET @Offset
			""";

		// Act & Assert
		await Assert.ThrowsExactlyAsync<ArgumentException>(async () =>
			await conn.GetPagedAsync<ProductDto>(
				sql,
				new { PageSize = 10 }, // Missing Page
				cancellationToken: this.TestContext.CancellationToken));
	}

	#endregion

	#region Test DTOs

	private record ProductDto(long Id, string Name, double Price);
	private record Product(long Id, string Name, double Price);
	private record CategoryDto(long Id, string Name);
	private record GetCategoryProductsRequest {
		public long CategoryId { get; init; }
		public int PageSize { get; init; }
		public int Page { get; init; }
	}

	#endregion

}
