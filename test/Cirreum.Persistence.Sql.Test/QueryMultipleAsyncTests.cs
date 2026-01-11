namespace Cirreum.Persistence.Sql.Test;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;

[TestClass]
public sealed class QueryMultipleAsyncTests {

	private static (ISqlConnection conn, SqliteConnection rawConn) CreateConnection() {
		var connection = new SqliteConnection("Data Source=:memory:");
		connection.Open();
		return (new SqliteSqlConnection(connection), connection);
	}

	private static void CreateTestSchema(SqliteConnection connection) {
		connection.Execute("""
			CREATE TABLE Users (
				Id TEXT PRIMARY KEY,
				Name TEXT NOT NULL,
				Email TEXT UNIQUE NOT NULL
			);

			CREATE TABLE Orders (
				Id TEXT PRIMARY KEY,
				UserId TEXT NOT NULL,
				Amount REAL NOT NULL,
				FOREIGN KEY (UserId) REFERENCES Users(Id)
			);
			""");
	}

	private sealed record UserDto(string Id, string Name, string Email);
	private sealed record OrderDto(string Id, string UserId, double Amount);
	private sealed record UserWithOrders(UserDto User, IReadOnlyList<OrderDto> Orders);

	#region MultipleGetAsync

	[TestMethod]
	public async Task MultipleGetAsync_ReturnsValue_WhenMapperReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act
		var result = await conn.MultipleGetAsync(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			[userId],
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}

				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return new UserWithOrders(user, orders);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.User.Name);
		Assert.HasCount(2, result.Value.Orders);
		Assert.AreEqual(300.0, result.Value.Orders.Sum(o => o.Amount));
	}

	[TestMethod]
	public async Task MultipleGetAsync_ReturnsNotFound_WhenMapperReturnsNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.MultipleGetAsync(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			[userId],
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}

				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return new UserWithOrders(user, orders);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
	}

	[TestMethod]
	public async Task MultipleGetAsync_NoParameters_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Jane", Email = "jane@test.com" });

		// Act
		var result = await conn.MultipleGetAsync(
			"SELECT COUNT(*) FROM Users; SELECT COUNT(*) FROM Orders;",
			["all"],
			async reader => {
				var userCount = await reader.ReadSingleOrDefaultAsync<int>();
				var orderCount = await reader.ReadSingleOrDefaultAsync<int>();
				return userCount + orderCount;
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(1, result.Value);
	}

	[TestMethod]
	public async Task MultipleGetAsync_MapperThrows_ReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.MultipleGetAsync<UserDto>(
			"SELECT 1;",
			["test"],
			async _ => {
				await Task.CompletedTask;
				throw new InvalidOperationException("Mapper error");
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<InvalidOperationException>(result.Error);
		Assert.AreEqual("Mapper error", result.Error!.Message);
	}

	#endregion

	#region MultipleGetOptionalAsync

	[TestMethod]
	public async Task MultipleGetOptionalAsync_ReturnsOptionalWithValue_WhenMapperReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.MultipleGetOptionalAsync(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
			new { Id = userId },
			async reader => await reader.ReadSingleOrDefaultAsync<UserDto>(), cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("John", result.Value.Value.Name);
	}

	[TestMethod]
	public async Task MultipleGetOptionalAsync_ReturnsEmptyOptional_WhenMapperReturnsNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.MultipleGetOptionalAsync(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
			new { Id = userId },
			async reader => await reader.ReadSingleOrDefaultAsync<UserDto>(), cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsFalse(result.Value.HasValue);
	}

	[TestMethod]
	public async Task MultipleGetOptionalAsync_NoParameters_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.MultipleGetOptionalAsync(
			"SELECT COUNT(*) FROM Users;",
			async reader => await reader.ReadSingleOrDefaultAsync<int>(), cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual(0, result.Value.Value);
	}

	#endregion

	#region MultipleQueryAnyAsync

	[TestMethod]
	public async Task MultipleQueryAnyAsync_ReturnsList_WhenMapperReturnsList() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act
		var result = await conn.MultipleQueryAnyAsync<OrderDto>(
			"""
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId;
			""",
			new { UserId = userId },
			async reader => {
				var orders = await reader.ReadAsync<OrderDto>();
				return [.. orders];
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(2, result.Value);
	}

	[TestMethod]
	public async Task MultipleQueryAnyAsync_ReturnsEmptyList_WhenNoResults() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.MultipleQueryAnyAsync<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId;",
			new { UserId = userId },
			async reader => {
				var orders = await reader.ReadAsync<OrderDto>();
				return [.. orders];
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsEmpty(result.Value);
	}

	[TestMethod]
	public async Task MultipleQueryAnyAsync_ReturnsEmptyList_WhenMapperReturnsNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.MultipleQueryAnyAsync<OrderDto>(
			"SELECT 1;",
			async _ => {
				await Task.CompletedTask;
				return null;
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsEmpty(result.Value);
	}

	[TestMethod]
	public async Task MultipleQueryAnyAsync_NoParameters_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" });

		// Act
		var result = await conn.MultipleQueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users;",
			async reader => [.. (await reader.ReadAsync<UserDto>())], cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(2, result.Value);
	}

	#endregion

	#region DbContext Integration

	[TestMethod]
	public async Task DbContext_MultipleGetAsync_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteAsync<UserDto>(ctx =>
			ctx.MultipleGetAsync(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
				new { Id = userId },
				[userId],
				async reader => await reader.ReadSingleOrDefaultAsync<UserDto>()), this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Name);
	}

	[TestMethod]
	public async Task DbContext_ThenMultipleGetAsync_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });

		// Act
		var result = await conn.ExecuteAsync<UserWithOrders>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenMultipleGetAsync(
				"""
				SELECT Id, Name, Email FROM Users WHERE Id = @Id;
				SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
				""",
				user => new { user.Id },
				[userId],
				async (_, reader) => {
					var u = await reader.ReadSingleOrDefaultAsync<UserDto>();
					if (u is null) {
						return null;
					}

					var orders = (await reader.ReadAsync<OrderDto>()).ToList();
					return new UserWithOrders(u, orders);
				}), this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.User.Name);
		Assert.HasCount(1, result.Value.Orders);
	}

	[TestMethod]
	public async Task DbContext_ThenMultipleGetOptionalAsync_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteAsync<Optional<UserDto>>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenMultipleGetOptionalAsync(
				"SELECT Id, Name, Email FROM Users WHERE Email = @Email;",
				user => new { user.Email },
				async (_, reader) => await reader.ReadSingleOrDefaultAsync<UserDto>()), this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("John", result.Value.Value.Name);
	}

	[TestMethod]
	public async Task DbContext_ThenMultipleQueryAnyAsync_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act
		var result = await conn.ExecuteAsync<IReadOnlyList<OrderDto>>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenMultipleQueryAnyAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId;",
				user => new { UserId = user.Id },
				async (_, reader) => [.. (await reader.ReadAsync<OrderDto>())]), this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(2, result.Value);
	}

	#endregion

	#region Transaction Integration

	[TestMethod]
	public async Task MultipleGetAsync_WorksInTransaction() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenMultipleGetAsync(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
				new { Id = userId },
				[userId],
				async reader => await reader.ReadSingleOrDefaultAsync<UserDto>()), this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Name);
	}

	public TestContext TestContext { get; set; }

	#endregion

	#region MultipleGetAsync Tuple Overloads

	[TestMethod]
	public async Task MultipleGetAsync_T1T2_ReturnsValue_WhenMapperReturnsTuple() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 });

		// Act
		var result = await conn.MultipleGetAsync<UserDto, IReadOnlyList<OrderDto>>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			[userId],
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}
				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return (user, orders);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Item1.Name);
		Assert.HasCount(2, result.Value.Item2);
		Assert.AreEqual(300.0, result.Value.Item2.Sum(o => o.Amount));
	}

	[TestMethod]
	public async Task MultipleGetAsync_T1T2_ReturnsNotFound_WhenMapperReturnsNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.MultipleGetAsync<UserDto, IReadOnlyList<OrderDto>>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			[userId],
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}
				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return (user, orders);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
	}

	[TestMethod]
	public async Task MultipleGetAsync_T1T2T3_ReturnsValue_WhenMapperReturnsTuple() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });

		// Act
		var result = await conn.MultipleGetAsync(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			SELECT COUNT(*) FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			[userId],
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}
				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				var count = await reader.ReadSingleOrDefaultAsync<int>();
				return ((UserDto, IReadOnlyList<OrderDto>, int)?)(user, orders, count);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Item1.Name);
		Assert.HasCount(1, result.Value.Item2);
		Assert.AreEqual(1, result.Value.Item3);
	}

	[TestMethod]
	public async Task MultipleGetAsync_T1T2_NoParameters_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" });

		// Act
		var result = await conn.MultipleGetAsync(
			"SELECT COUNT(*) FROM Users; SELECT COUNT(*) FROM Orders;",
			["all"],
			async reader => {
				var userCount = await reader.ReadSingleOrDefaultAsync<int>();
				var orderCount = await reader.ReadSingleOrDefaultAsync<int>();
				return ((int, int)?)(userCount, orderCount);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(2, result.Value.Item1);
		Assert.AreEqual(0, result.Value.Item2);
	}

	#endregion

	#region MultipleGetOptionalAsync Tuple Overloads

	[TestMethod]
	public async Task MultipleGetOptionalAsync_T1T2_ReturnsOptionalWithValue_WhenMapperReturnsTuple() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });

		// Act
		var result = await conn.MultipleGetOptionalAsync<UserDto, IReadOnlyList<OrderDto>>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}

				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return ((UserDto, IReadOnlyList<OrderDto>)?)(user, orders);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("John", result.Value.Value.Item1.Name);
		Assert.HasCount(1, result.Value.Value.Item2);
	}

	[TestMethod]
	public async Task MultipleGetOptionalAsync_T1T2_ReturnsEmptyOptional_WhenMapperReturnsNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.MultipleGetOptionalAsync<UserDto, IReadOnlyList<OrderDto>>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}

				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return ((UserDto, IReadOnlyList<OrderDto>)?)(user, orders);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert - Success with empty Optional (NOT a failure like MultipleGetAsync)
		Assert.IsTrue(result.IsSuccess);
		Assert.IsFalse(result.Value.HasValue);
	}

	[TestMethod]
	public async Task MultipleGetOptionalAsync_T1T2T3_ReturnsOptionalWithValue_WhenMapperReturnsTuple() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });

		// Act
		var result = await conn.MultipleGetOptionalAsync<UserDto, IReadOnlyList<OrderDto>, int>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			SELECT COUNT(*) FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) {
					return null;
				}

				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				var count = await reader.ReadSingleOrDefaultAsync<int>();
				return ((UserDto, IReadOnlyList<OrderDto>, int)?)(user, orders, count);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("John", result.Value.Value.Item1.Name);
		Assert.HasCount(1, result.Value.Value.Item2);
		Assert.AreEqual(1, result.Value.Value.Item3);
	}

	[TestMethod]
	public async Task MultipleGetOptionalAsync_T1T2_NoParameters_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });

		// Act
		var result = await conn.MultipleGetOptionalAsync<int, int>(
			"SELECT COUNT(*) FROM Users; SELECT COUNT(*) FROM Orders;",
			async reader => {
				var userCount = await reader.ReadSingleOrDefaultAsync<int>();
				var orderCount = await reader.ReadSingleOrDefaultAsync<int>();
				return ((int, int)?)(userCount, orderCount);
			}, cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual(1, result.Value.Value.Item1);
		Assert.AreEqual(0, result.Value.Value.Item2);
	}

	#endregion
}
