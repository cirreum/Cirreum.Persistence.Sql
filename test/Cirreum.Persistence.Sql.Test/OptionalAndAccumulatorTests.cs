namespace Cirreum.Persistence.Sql.Test;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Data;

/// <summary>
/// Tests for Optional&lt;T&gt; support (GetOptionalAsync, QueryOptionalAsync) and And* accumulator extensions.
/// </summary>
[TestClass]
public sealed class OptionalAndAccumulatorTests {

	public TestContext TestContext { get; set; } = null!;

	private static (ISqlConnection conn, SqliteConnection rawConn) CreateConnection() {
		var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		return (new SqliteSqlConnection(connection), connection);
	}

	private static void CreateTestSchema(IDbConnection connection) {
		connection.Execute("""
			CREATE TABLE Users (
				Id TEXT PRIMARY KEY,
				Name TEXT NOT NULL,
				Email TEXT UNIQUE NOT NULL
			);

			CREATE TABLE UserPrefs (
				UserId TEXT PRIMARY KEY,
				Theme TEXT NOT NULL,
				FOREIGN KEY (UserId) REFERENCES Users(Id)
			);

			CREATE TABLE Orders (
				Id TEXT PRIMARY KEY,
				UserId TEXT NOT NULL,
				Amount REAL NOT NULL,
				FOREIGN KEY (UserId) REFERENCES Users(Id)
			);
			""");
	}

	private record UserDto(string Id, string Name, string Email);
	private record User(string Id, string Name, string Email);
	private record UserPrefsDto(string UserId, string Theme);
	private record OrderDto(string Id, string UserId, double Amount);

	#region GetOptionalAsync - IDbConnection

	[TestMethod]
	public async Task GetOptionalAsync_WhenNoRowExists_ReturnsEmptyOptional() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.GetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.IsEmpty);
	}

	[TestMethod]
	public async Task GetOptionalAsync_WhenOneRowExists_ReturnsOptionalWithValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.GetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("John", result.Value.Value.Name);
	}

	[TestMethod]
	public async Task GetOptionalAsync_WhenMultipleRowsExist_ReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" });

		// Act
		var result = await conn.GetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users",
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<InvalidOperationException>(result.Error);
		Assert.Contains("more than one", result.Error.Message);
	}

	[TestMethod]
	public async Task GetOptionalAsync_WithMapper_WhenRowExists_TransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Jane", Email = "jane@test.com" });

		// Act
		var result = await conn.GetOptionalAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			dto => new User(dto.Id, dto.Name.ToUpper(), dto.Email),
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("JANE", result.Value.Value.Name);
	}

	[TestMethod]
	public async Task GetOptionalAsync_WithMapper_WhenNoRow_ReturnsEmptyOptional() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.GetOptionalAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			dto => new User(dto.Id, dto.Name.ToUpper(), dto.Email),
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.IsEmpty);
	}

	#endregion

	#region QueryOptionalAsync - IDbConnection

	[TestMethod]
	public async Task QueryOptionalAsync_WhenNoRowExists_ReturnsEmptyOptional() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.QueryOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.IsEmpty);
	}

	[TestMethod]
	public async Task QueryOptionalAsync_WhenOneRowExists_ReturnsOptionalWithValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.QueryOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("John", result.Value.Value.Name);
	}

	[TestMethod]
	public async Task QueryOptionalAsync_WhenMultipleRowsExist_ReturnsFirstRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = "1", Name = "First", Email = "first@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = "2", Name = "Second", Email = "second@test.com" });

		// Act - Query without ORDER BY, but SQLite returns in insert order
		var result = await conn.QueryOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users ORDER BY Id",
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("First", result.Value.Value.Name); // Should return first row
	}

	[TestMethod]
	public async Task QueryOptionalAsync_WithMapper_WhenRowExists_TransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Jane", Email = "jane@test.com" });

		// Act
		var result = await conn.QueryOptionalAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			dto => new User(dto.Id, dto.Name.ToUpper(), dto.Email),
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("JANE", result.Value.Value.Name);
	}

	#endregion

	#region GetOptionalAsync/QueryOptionalAsync - DbContext

	[TestMethod]
	public async Task DbContext_GetOptionalAsync_WhenNoRow_ReturnsEmptyOptional() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.ExecuteTransactionAsync<Optional<UserDto>>(ctx =>
			ctx.GetOptionalAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = "nonexistent" })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.IsEmpty);
	}

	[TestMethod]
	public async Task DbContext_GetOptionalAsync_WhenRowExists_ReturnsOptionalWithValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Test", Email = "test@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<Optional<UserDto>>(ctx =>
			ctx.GetOptionalAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("Test", result.Value.Value.Name);
	}

	[TestMethod]
	public async Task DbContext_QueryOptionalAsync_WhenMultipleRows_ReturnsFirstRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = "1", Name = "First", Email = "first@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = "2", Name = "Second", Email = "second@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<Optional<UserDto>>(ctx =>
			ctx.QueryOptionalAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users ORDER BY Id")
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("First", result.Value.Value.Name);
	}

	#endregion

	#region And* Accumulator Extensions - Basic Tuple Building

	[TestMethod]
	public async Task AndGetAsync_BuildsTuple_T1_T2() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var user1Id = Guid.NewGuid().ToString();
		var user2Id = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = user1Id, Name = "User1", Email = "user1@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = user2Id, Name = "User2", Email = "user2@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, UserDto)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user1Id },
				user1Id)
			.AndGetAsync<UserDto, UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user2Id },
				user2Id)
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("User1", result.Value.Item1.Name);
		Assert.AreEqual("User2", result.Value.Item2.Name);
	}

	[TestMethod]
	public async Task AndGetAsync_BuildsTuple_T1_T2_T3() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var user1Id = Guid.NewGuid().ToString();
		var user2Id = Guid.NewGuid().ToString();
		var user3Id = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = user1Id, Name = "User1", Email = "user1@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = user2Id, Name = "User2", Email = "user2@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = user3Id, Name = "User3", Email = "user3@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, UserDto, UserDto)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user1Id },
				user1Id)
			.AndGetAsync<UserDto, UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user2Id },
				user2Id)
			.AndGetAsync<UserDto, UserDto, UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user3Id },
				user3Id)
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("User1", result.Value.Item1.Name);
		Assert.AreEqual("User2", result.Value.Item2.Name);
		Assert.AreEqual("User3", result.Value.Item3.Name);
	}

	[TestMethod]
	public async Task AndGetOptionalAsync_BuildsTupleWithOptional() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		// No UserPrefs for this user

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, Optional<UserPrefsDto>)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.AndGetOptionalAsync<UserDto, UserPrefsDto>(
				"SELECT UserId, Theme FROM UserPrefs WHERE UserId = @UserId",
				new { UserId = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Item1.Name);
		Assert.IsTrue(result.Value.Item2.IsEmpty); // No prefs found
	}

	[TestMethod]
	public async Task AndGetOptionalAsync_WithExistingOptional_ReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO UserPrefs (UserId, Theme) VALUES (@UserId, @Theme)",
			new { UserId = userId, Theme = "dark" });

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, Optional<UserPrefsDto>)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.AndGetOptionalAsync<UserDto, UserPrefsDto>(
				"SELECT UserId, Theme FROM UserPrefs WHERE UserId = @UserId",
				new { UserId = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Item1.Name);
		Assert.IsTrue(result.Value.Item2.HasValue);
		Assert.AreEqual("dark", result.Value.Item2.Value.Theme);
	}

	[TestMethod]
	public async Task AndQueryOptionalAsync_WithMultipleRows_ReturnsFirst() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, Optional<OrderDto>)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.AndQueryOptionalAsync<UserDto, OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId ORDER BY Amount",
				new { UserId = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Item1.Name);
		Assert.IsTrue(result.Value.Item2.HasValue);
		Assert.AreEqual(100.0, result.Value.Item2.Value.Amount); // First by amount
	}

	[TestMethod]
	public async Task AndQueryAnyAsync_AccumulatesList() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, IReadOnlyList<OrderDto>)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.AndQueryAnyAsync<UserDto, OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId",
				new { UserId = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Item1.Name);
		Assert.HasCount(2, result.Value.Item2);
	}

	[TestMethod]
	public async Task AndGetScalarAsync_AccumulatesScalar() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, long)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.AndGetScalarAsync<UserDto, long>(
				"SELECT COUNT(*) FROM Orders WHERE UserId = @UserId",
				new { UserId = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Item1.Name);
		Assert.AreEqual(2L, result.Value.Item2);
	}

	#endregion

	#region And* Accumulator Extensions - Short-circuit on failure

	[TestMethod]
	public async Task AndGetAsync_WhenFirstFails_ShortCircuits() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var user2Id = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = user2Id, Name = "User2", Email = "user2@test.com" });
		var secondQueryExecuted = false;

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, UserDto)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = "nonexistent" },
				"nonexistent") // This will fail
			.AndGetAsync<UserDto, UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user2Id },
				user2Id)
			.MapAsync(tuple => {
				secondQueryExecuted = true;
				return tuple;
			})
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
		Assert.IsFalse(secondQueryExecuted, "Second query should not execute after first failure");
	}

	[TestMethod]
	public async Task AndGetAsync_WhenSecondFails_PropagatesError() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var user1Id = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = user1Id, Name = "User1", Email = "user1@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<(UserDto, UserDto)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user1Id },
				user1Id) // This succeeds
			.AndGetAsync<UserDto, UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = "nonexistent" },
				"nonexistent") // This fails
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
	}

	#endregion

	#region Complex Accumulator Scenarios

	[TestMethod]
	public async Task ComplexAccumulator_UserWithPrefsAndOrders() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO UserPrefs (UserId, Theme) VALUES (@UserId, @Theme)",
			new { UserId = userId, Theme = "dark" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act - Get user, optional prefs, and all orders
		var result = await conn.ExecuteTransactionAsync<(UserDto, Optional<UserPrefsDto>, IReadOnlyList<OrderDto>)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.AndGetOptionalAsync<UserDto, UserPrefsDto>(
				"SELECT UserId, Theme FROM UserPrefs WHERE UserId = @UserId",
				new { UserId = userId })
			.AndQueryAnyAsync<UserDto, Optional<UserPrefsDto>, OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId",
				new { UserId = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		var (user, prefs, orders) = result.Value;
		Assert.AreEqual("John", user.Name);
		Assert.IsTrue(prefs.HasValue);
		Assert.AreEqual("dark", prefs.Value.Theme);
		Assert.HasCount(2, orders);
	}

	[TestMethod]
	public async Task ComplexAccumulator_UserWithOrderCountAndTotal() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act - Get user, order count, and total amount
		var result = await conn.ExecuteTransactionAsync<(UserDto, long, double)>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.AndGetScalarAsync<UserDto, long>(
				"SELECT COUNT(*) FROM Orders WHERE UserId = @UserId",
				new { UserId = userId })
			.AndGetScalarAsync<UserDto, long, double>(
				"SELECT COALESCE(SUM(Amount), 0) FROM Orders WHERE UserId = @UserId",
				new { UserId = userId })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		var (user, count, total) = result.Value;
		Assert.AreEqual("John", user.Name);
		Assert.AreEqual(2L, count);
		Assert.AreEqual(300.0, total);
	}

	#endregion
}
