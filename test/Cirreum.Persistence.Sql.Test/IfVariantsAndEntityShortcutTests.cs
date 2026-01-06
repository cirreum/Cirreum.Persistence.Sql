namespace Cirreum.Persistence.Sql.Test;

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
[TestClass]
public sealed class IfVariantsAndEntityShortcutTests {

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

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(1L, orderCount);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount); // Order should NOT be inserted
	}

	#endregion

	#region DbResult (non-generic) - ThenInsertIfAsync with Func<bool>

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(1L, orderCount);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount);
	}

	#endregion

	#region DbResult (non-generic) - ThenInsertIfAndReturnAsync

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(orderId, result.Value);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(1L, orderCount);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(orderId, result.Value); // Still returns the value
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount); // But no insert happened
	}

	#endregion

	#region DbResult<T> - ThenInsertIfAsync with bool

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("John", result.Value.Name); // Pass-through
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(1L, orderCount);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("John", result.Value.Name); // Pass-through still works
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount); // No insert
	}

	#endregion

	#region DbResult<T> - ThenInsertIfAsync with Func<T, bool>

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("John", result.Value.Name);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(1L, orderCount);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("John", result.Value.Name); // Pass-through still works
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount); // No insert because user is inactive
	}

	#endregion

	#region DbResult<T> - ThenUpdateIfAsync

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("John", result.Value.Name); // Original value passed through
		var updatedName = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		Assert.AreEqual("Jane", updatedName); // But DB was updated
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("John", result.Value.Name);
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		Assert.AreEqual("John", name); // NOT updated
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		Assert.AreEqual("Updated", name); // Updated because name started with "J"
	}

	#endregion

	#region DbResult<T> - ThenDeleteIfAsync

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual(orderId, result.Value.Id); // Value passed through
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount); // Deleted
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(1L, orderCount); // NOT deleted
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount); // Deleted because Amount > 50
	}

	#endregion

	#region ThenUpdateIfAndReturnAsync / ThenDeleteIfAndReturnAsync

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("Updated John", result.Value);
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		Assert.AreEqual("Jane", name);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("Skipped John", result.Value); // resultSelector still called
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		Assert.AreEqual("John", name); // NOT updated
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreEqual("Deleted order 100", result.Value);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		Assert.AreEqual(0L, orderCount);
	}

	#endregion

	#region Entity Shortcut Overloads - DbContext.InsertAndReturnAsync<TEntity>(sql, entity)

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreSame(user, result.Value); // Same instance returned
		var dbUser = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { user.Id });
		Assert.IsNotNull(dbUser);
		Assert.AreEqual("John", dbUser.Name);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert - Constraint violations are translated to AlreadyExistsException regardless of database provider
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<AlreadyExistsException>(result.Error);
	}

	#endregion

	#region Entity Shortcut Overloads - DbResult.ThenInsertAndReturnAsync<TEntity>(sql, entity)

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreSame(order, result.Value);
		var dbOrder = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { order.Id });
		Assert.IsNotNull(dbOrder);
		Assert.AreEqual(150.0, dbOrder.Amount);
	}

	#endregion

	#region Entity Shortcut Overloads - DbResult.ThenInsertIfAndReturnAsync<TEntity>

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreSame(order, result.Value);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		Assert.AreEqual(1L, orderCount);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreSame(order, result.Value); // Entity still returned
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		Assert.AreEqual(0L, orderCount); // But not inserted
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreSame(order, result.Value);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		Assert.AreEqual(1L, orderCount);
	}

	#endregion

	#region Entity Shortcut Overloads - DbResult<T>.ThenInsertIfAndReturnAsync<TEntity>

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreSame(order, result.Value);
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		Assert.AreEqual(1L, orderCount);
	}

	[TestMethod]
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
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess, $"Expected success but got failure: {result.Error?.Message}");
		Assert.AreSame(order, result.Value); // Entity still returned
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { order.Id });
		Assert.AreEqual(0L, orderCount); // But not inserted because user is inactive
	}

	#endregion
}
