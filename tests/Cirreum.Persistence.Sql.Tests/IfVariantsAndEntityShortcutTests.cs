namespace Cirreum.Persistence.Sql.Tests;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Data;

/// <summary>
/// Tests for If variant methods (ThenInsertIfAsync, ThenUpdateIfAsync, ThenDeleteIfAsync with bool/Func&lt;bool&gt;/Func&lt;T, bool&gt;)
/// and entity shortcut overloads (InsertAndReturnAsync&lt;TEntity&gt;(sql, entity)).
/// </summary>
public sealed class IfVariantsAndEntityShortcutTests {

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
				Email TEXT UNIQUE NOT NULL,
				IsActive INTEGER NOT NULL DEFAULT 1
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
	private record UserWithActiveDto(string Id, string Name, string Email, long IsActive);
	private record User(string Id, string Name, string Email);
	private record OrderDto(string Id, string UserId, double Amount);

	#region DbResult (non-generic) - ThenInsertIfAsync with bool

	[Fact]
	public async Task DbResult_ThenInsertIfAsync_WhenTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: true)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task DbResult_ThenInsertIfAsync_WhenFalse_SkipsInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: false)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L); // Order should NOT be inserted
	}

	#endregion

	#region DbResult (non-generic) - ThenInsertIfAsync with Func<bool>

	[Fact]
	public async Task DbResult_ThenInsertIfAsync_WithFuncTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		var shouldInsert = true;

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task DbResult_ThenInsertIfAsync_WithFuncFalse_SkipsInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		var shouldInsert = false;

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L);
	}

	#endregion

	#region DbResult (non-generic) - ThenInsertIfAndReturnAsync

	[Fact]
	public async Task DbResult_ThenInsertIfAndReturnAsync_WhenTrue_ExecutesAndReturns() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: true,
				resultSelector: () => orderId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(orderId);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task DbResult_ThenInsertIfAndReturnAsync_WhenFalse_SkipsButStillReturns() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: false,
				resultSelector: () => orderId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(orderId); // Still returns the value
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L); // But no insert happened
	}

	#endregion

	#region DbResult<T> - ThenInsertIfAsync with bool

	[Fact]
	public async Task DbResultT_ThenInsertIfAsync_WhenTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				u => new { Id = orderId, UserId = u.Id, Amount = 100.0 },
				when: _ => true)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value!.Name.Should().Be("John"); // Pass-through
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task DbResultT_ThenInsertIfAsync_WhenFalse_SkipsInsertPassesThroughValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				u => new { Id = orderId, UserId = u.Id, Amount = 100.0 },
				when: _ => false)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value!.Name.Should().Be("John"); // Pass-through still works
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L); // No insert
	}

	#endregion

	#region DbResult<T> - ThenInsertIfAsync with Func<T, bool>

	[Fact]
	public async Task DbResultT_ThenInsertIfAsync_WithFuncT_WhenPredicateTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email, IsActive) VALUES (@Id, @Name, @Email, @IsActive)",
			new { Id = userId, Name = "John", Email = "john@test.com", IsActive = 1 });

		// Act - Insert order only if user is active
		var result = await conn.ExecuteTransactionAsync<UserWithActiveDto>(ctx =>
			ctx.GetAsync<UserWithActiveDto>(
				"SELECT Id, Name, Email, IsActive FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				u => new { Id = orderId, UserId = u.Id, Amount = 100.0 },
				when: u => u.IsActive == 1) // Predicate based on fetched value
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value!.Name.Should().Be("John");
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task DbResultT_ThenInsertIfAsync_WithFuncT_WhenPredicateFalse_SkipsInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email, IsActive) VALUES (@Id, @Name, @Email, @IsActive)",
			new { Id = userId, Name = "John", Email = "john@test.com", IsActive = 0 }); // Inactive

		// Act - Insert order only if user is active
		var result = await conn.ExecuteTransactionAsync<UserWithActiveDto>(ctx =>
			ctx.GetAsync<UserWithActiveDto>(
				"SELECT Id, Name, Email, IsActive FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				u => new { Id = orderId, UserId = u.Id, Amount = 100.0 },
				when: u => u.IsActive == 1) // Predicate based on fetched value
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value!.Name.Should().Be("John"); // Pass-through still works
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L); // No insert because user is inactive
	}

	#endregion

	#region DbResult<T> - ThenUpdateIfAsync

	[Fact]
	public async Task DbResultT_ThenUpdateIfAsync_WhenTrue_ExecutesUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				u => new { u.Id, Name = "Jane" },
				userId,
				when: _ => true)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value!.Name.Should().Be("John"); // Original value passed through
		var updatedName = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		updatedName.Should().Be("Jane"); // But DB was updated
	}

	[Fact]
	public async Task DbResultT_ThenUpdateIfAsync_WhenFalse_SkipsUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				u => new { u.Id, Name = "Jane" },
				userId,
				when: _ => false)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value!.Name.Should().Be("John");
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("John"); // NOT updated
	}

	[Fact]
	public async Task DbResultT_ThenUpdateIfAsync_WithFuncT_BasedOnValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Only update if name starts with "J"
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				u => new { u.Id, Name = "Updated" },
				userId,
				when: u => u.Name.StartsWith('J'))
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("Updated"); // Updated because name started with "J"
	}

	#endregion

	#region DbResult<T> - ThenDeleteIfAsync

	[Fact]
	public async Task DbResultT_ThenDeleteIfAsync_WhenTrue_ExecutesDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 100.0 });

		// Act
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.GetAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
				new { Id = orderId },
				orderId)
			.ThenDeleteIfAsync(
				"DELETE FROM Orders WHERE Id = @Id",
				o => new { o.Id },
				orderId,
				when: _ => true)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value!.Id.Should().Be(orderId); // Value passed through
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L); // Deleted
	}

	[Fact]
	public async Task DbResultT_ThenDeleteIfAsync_WhenFalse_SkipsDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 100.0 });

		// Act
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.GetAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
				new { Id = orderId },
				orderId)
			.ThenDeleteIfAsync(
				"DELETE FROM Orders WHERE Id = @Id",
				o => new { o.Id },
				orderId,
				when: _ => false)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(1L); // NOT deleted
	}

	[Fact]
	public async Task DbResultT_ThenDeleteIfAsync_WithFuncT_BasedOnValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 100.0 });

		// Act - Only delete if amount > 50
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.GetAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
				new { Id = orderId },
				orderId)
			.ThenDeleteIfAsync(
				"DELETE FROM Orders WHERE Id = @Id",
				o => new { o.Id },
				orderId,
				when: o => o.Amount > 50)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L); // Deleted because Amount > 50
	}

	#endregion

	#region ThenUpdateIfAndReturnAsync / ThenDeleteIfAndReturnAsync

	[Fact]
	public async Task DbResultT_ThenUpdateIfAndReturnAsync_WhenTrue_UpdatesAndReturns() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				u => new { u.Id, Name = "Jane" },
				userId,
				when: _ => true,
				resultSelector: u => $"Updated {u.Name}")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value.Should().Be("Updated John");
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("Jane");
	}

	[Fact]
	public async Task DbResultT_ThenUpdateIfAndReturnAsync_WhenFalse_SkipsButStillReturns() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				u => new { u.Id, Name = "Jane" },
				userId,
				when: _ => false,
				resultSelector: u => $"Skipped {u.Name}")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value.Should().Be("Skipped John"); // resultSelector still called
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("John"); // NOT updated
	}

	[Fact]
	public async Task DbResultT_ThenDeleteIfAndReturnAsync_WhenTrue_DeletesAndReturns() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 100.0 });

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.GetAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
				new { Id = orderId },
				orderId)
			.ThenDeleteIfAndReturnAsync(
				"DELETE FROM Orders WHERE Id = @Id",
				o => new { o.Id },
				orderId,
				when: _ => true,
				resultSelector: o => $"Deleted order {o.Amount}")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value.Should().Be("Deleted order 100");
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		orderCount.Should().Be(0L);
	}

	#endregion

	#region Entity Shortcut Overloads - DbContext.InsertAndReturnAsync<TEntity>(sql, entity)

	[Fact]
	public async Task DbContext_InsertAndReturnAsync_EntityShortcut_UsesEntityAsParamsAndReturn() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var user = new User(Guid.NewGuid().ToString(), "John", "john@test.com");

		// Act
		var result = await conn.ExecuteTransactionAsync<User>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				user)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeSameAs(user); // Same instance returned
		var dbUser = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { user.Id });
		dbUser.Should().NotBeNull();
		dbUser.Name.Should().Be("John");
	}

	[Fact]
	public async Task DbContext_InsertAndReturnAsync_EntityShortcut_HandlesConstraintViolation() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var user = new User(Guid.NewGuid().ToString(), "John", "john@test.com");
		// Insert first
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { user.Id, user.Name, user.Email });

		// Act - Try to insert with same email (unique constraint)
		var duplicateUser = new User(Guid.NewGuid().ToString(), "Jane", "john@test.com");
		var result = await conn.ExecuteTransactionAsync<User>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				duplicateUser)
		, CancellationToken.None);

		// Assert - Constraint violations are translated to AlreadyExistsException regardless of database provider
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<AlreadyExistsException>();
	}

	#endregion

	#region Entity Shortcut Overloads - DbResult.ThenInsertAndReturnAsync<TEntity>(sql, entity)

	[Fact]
	public async Task DbResult_ThenInsertAndReturnAsync_EntityShortcut_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var order = new OrderDto(Guid.NewGuid().ToString(), userId, 150.0);

		// Act
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				order)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeSameAs(order);
		var dbOrder = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { order.Id });
		dbOrder.Should().NotBeNull();
		dbOrder.Amount.Should().Be(150.0);
	}

	#endregion

	#region Entity Shortcut Overloads - DbResult.ThenInsertIfAndReturnAsync<TEntity>

	[Fact]
	public async Task DbResult_ThenInsertIfAndReturnAsync_EntityShortcut_WhenTrue_Inserts() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var order = new OrderDto(Guid.NewGuid().ToString(), userId, 150.0);

		// Act
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				order,
				when: true)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeSameAs(order);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task DbResult_ThenInsertIfAndReturnAsync_EntityShortcut_WhenFalse_SkipsButReturnsEntity() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var order = new OrderDto(Guid.NewGuid().ToString(), userId, 150.0);

		// Act
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				order,
				when: false)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeSameAs(order); // Entity still returned
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		orderCount.Should().Be(0L); // But not inserted
	}

	[Fact]
	public async Task DbResult_ThenInsertIfAndReturnAsync_EntityShortcut_WithFuncBool() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var order = new OrderDto(Guid.NewGuid().ToString(), userId, 150.0);
		var shouldInsert = true;

		// Act
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				order,
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeSameAs(order);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		orderCount.Should().Be(1L);
	}

	#endregion

	#region Entity Shortcut Overloads - DbResult<T>.ThenInsertIfAndReturnAsync<TEntity>

	[Fact]
	public async Task DbResultT_ThenInsertIfAndReturnAsync_EntityShortcut_WithFuncT() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email, IsActive) VALUES (@Id, @Name, @Email, @IsActive)",
			new { Id = userId, Name = "John", Email = "john@test.com", IsActive = 1 });
		var order = new OrderDto(Guid.NewGuid().ToString(), userId, 150.0);

		// Act - Insert order only if user is active
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.GetAsync<UserWithActiveDto>(
				"SELECT Id, Name, Email, IsActive FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				order,
				when: u => u.IsActive == 1)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value.Should().BeSameAs(order);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task DbResultT_ThenInsertIfAndReturnAsync_EntityShortcut_WithFuncT_WhenFalse() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email, IsActive) VALUES (@Id, @Name, @Email, @IsActive)",
			new { Id = userId, Name = "John", Email = "john@test.com", IsActive = 0 }); // Inactive
		var order = new OrderDto(Guid.NewGuid().ToString(), userId, 150.0);

		// Act - Insert order only if user is active
		var result = await conn.ExecuteTransactionAsync<OrderDto>(ctx =>
			ctx.GetAsync<UserWithActiveDto>(
				"SELECT Id, Name, Email, IsActive FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				order,
				when: u => u.IsActive == 1)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue($"Expected success but got failure: {result.Error?.Message}");
		result.Value.Should().BeSameAs(order); // Entity still returned
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		orderCount.Should().Be(0L); // But not inserted because user is inactive
	}

	#endregion
}
