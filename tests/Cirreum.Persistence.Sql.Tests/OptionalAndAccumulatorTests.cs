namespace Cirreum.Persistence.Sql.Tests;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Data;

/// <summary>
/// Tests for Optional&lt;T&gt; support (GetOptionalAsync, QueryOptionalAsync) and And* accumulator extensions.
/// </summary>
public sealed class OptionalAndAccumulatorTests {

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

	[Fact]
	public async Task GetOptionalAsync_WhenNoRowExists_ReturnsEmptyOptional() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.GetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.IsEmpty.Should().BeTrue();
	}

	[Fact]
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
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("John");
	}

	[Fact]
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
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidOperationException>();
		result.Error.Message.Should().Contain("more than one");
	}

	[Fact]
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
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("JANE");
	}

	[Fact]
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
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.IsEmpty.Should().BeTrue();
	}

	#endregion

	#region QueryOptionalAsync - IDbConnection

	[Fact]
	public async Task QueryOptionalAsync_WhenNoRowExists_ReturnsEmptyOptional() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.QueryOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.IsEmpty.Should().BeTrue();
	}

	[Fact]
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
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("John");
	}

	[Fact]
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
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("First"); // Should return first row
	}

	[Fact]
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
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("JANE");
	}

	#endregion

	#region GetOptionalAsync/QueryOptionalAsync - DbContext

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.IsEmpty.Should().BeTrue();
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("Test");
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("First");
	}

	#endregion

	#region And* Accumulator Extensions - Basic Tuple Building

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Item1.Name.Should().Be("User1");
		result.Value.Item2.Name.Should().Be("User2");
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Item1.Name.Should().Be("User1");
		result.Value.Item2.Name.Should().Be("User2");
		result.Value.Item3.Name.Should().Be("User3");
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Item1.Name.Should().Be("John");
		result.Value.Item2.IsEmpty.Should().BeTrue(); // No prefs found
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Item1.Name.Should().Be("John");
		result.Value.Item2.HasValue.Should().BeTrue();
		result.Value.Item2.Value.Theme.Should().Be("dark");
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Item1.Name.Should().Be("John");
		result.Value.Item2.HasValue.Should().BeTrue();
		result.Value.Item2.Value.Amount.Should().Be(100.0); // First by amount
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Item1.Name.Should().Be("John");
		result.Value.Item2.Should().HaveCount(2);
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Item1.Name.Should().Be("John");
		result.Value.Item2.Should().Be(2L);
	}

	#endregion

	#region And* Accumulator Extensions - Short-circuit on failure

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
		secondQueryExecuted.Should().BeFalse("Second query should not execute after first failure");
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	#endregion

	#region Complex Accumulator Scenarios

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var (user, prefs, orders) = result.Value;
		user.Name.Should().Be("John");
		prefs.HasValue.Should().BeTrue();
		prefs.Value.Theme.Should().Be("dark");
		orders.Should().HaveCount(2);
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var (user, count, total) = result.Value;
		user.Name.Should().Be("John");
		count.Should().Be(2L);
		total.Should().Be(300.0);
	}

	#endregion
}
