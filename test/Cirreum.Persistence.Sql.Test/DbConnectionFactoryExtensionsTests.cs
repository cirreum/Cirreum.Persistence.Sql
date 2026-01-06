namespace Cirreum.Persistence.Sql.Test;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;

[TestClass]
public sealed class DbConnectionFactoryExtensionsTests {

	/// <summary>
	/// A test connection factory that uses a shared in-memory SQLite database.
	/// The "Mode=Memory;Cache=Shared" connection string ensures all connections
	/// created by this factory share the same in-memory database.
	/// </summary>
	private sealed class TestConnectionFactory : ISqlConnectionFactory, IDisposable {
		private readonly string _connectionString;
		private readonly SqliteConnection _keepAliveConnection;
		private bool _schemaCreated;

		public TestConnectionFactory() {
			// Use a unique database name per instance to avoid test interference
			var dbName = $"test_{Guid.NewGuid():N}";
			this._connectionString = $"Data Source={dbName};Mode=Memory;Cache=Shared";

			// Keep one connection open to prevent the shared in-memory database from being destroyed
			this._keepAliveConnection = new SqliteConnection(this._connectionString);
			this._keepAliveConnection.Open();
		}

		public int CommandTimeoutSeconds => 30;

		public async Task<ISqlConnection> CreateConnectionAsync(CancellationToken cancellationToken = default) {
			var connection = new SqliteConnection(this._connectionString);
			await connection.OpenAsync(cancellationToken);

			if (!this._schemaCreated) {
				CreateTestSchema(connection);
				this._schemaCreated = true;
			}

			return new SqliteSqlConnection(connection, this.CommandTimeoutSeconds);
		}

		private static void CreateTestSchema(SqliteConnection connection) {
			connection.Execute("""
				CREATE TABLE IF NOT EXISTS Users (
					Id TEXT PRIMARY KEY,
					Name TEXT NOT NULL,
					Email TEXT UNIQUE NOT NULL
				);

				CREATE TABLE IF NOT EXISTS Orders (
					Id TEXT PRIMARY KEY,
					UserId TEXT NOT NULL,
					Amount REAL NOT NULL,
					FOREIGN KEY (UserId) REFERENCES Users(Id)
				);
				""");
		}

		public void Dispose() {
			this._keepAliveConnection.Dispose();
		}
	}

	private TestConnectionFactory _factory = null!;

	[TestInitialize]
	public void TestInitialize() {
		this._factory = new TestConnectionFactory();
	}

	[TestCleanup]
	public void TestCleanup() {
		this._factory?.Dispose();
	}

	#region ExecuteAsync

	[TestMethod]
	public async Task ExecuteAsync_NonGeneric_ExecutesAction() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.ExecuteAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
	}

	[TestMethod]
	public async Task ExecuteAsync_Generic_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.ExecuteAsync(async ctx => {
			await ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" });
			return await ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId);
		}, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Name);
	}

	#endregion

	#region ExecuteTransactionAsync

	[TestMethod]
	public async Task ExecuteTransactionAsync_NonGeneric_CommitsOnSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);

		// Verify commit by reading from a new connection
		var verifyResult = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			this.TestContext.CancellationToken);
		Assert.IsTrue(verifyResult.IsSuccess);
	}

	[TestMethod]
	public async Task ExecuteTransactionAsync_Generic_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(userId, result.Value);
	}

	[TestMethod]
	public async Task ExecuteTransactionAsync_RollsBackOnFailure() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act - Insert then fail on an ensure clause
		var result = await this._factory.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "Invalid", Email = "invalid@test.com" },
				() => new UserDto(userId, "Invalid", "invalid@test.com"))
			.EnsureAsync(u => u.Name.StartsWith("Valid"), new BadRequestException("Must be valid"))
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);

		// Verify rollback - user should not exist
		var verifyResult = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			this.TestContext.CancellationToken);
		Assert.IsTrue(verifyResult.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(verifyResult.Error);
	}

	#endregion

	#region GetAsync

	[TestMethod]
	public async Task GetAsync_WhenRecordExists_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.Name);
	}

	[TestMethod]
	public async Task GetAsync_WhenRecordDoesNotExist_ReturnsNotFound() {
		// Act
		var result = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			"nonexistent",
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
	}

	[TestMethod]
	public async Task GetAsync_WithMapper_TransformsResult() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Jane", Email = "jane@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.GetAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			dto => new User(dto.Id, dto.Name.ToUpperInvariant(), dto.Email),
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("JANE", result.Value.Name);
	}

	[TestMethod]
	public async Task GetAsync_WithoutParameters_Works() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Solo", Email = "solo@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act - Use overload without parameters
		var result = await this._factory.GetAsync<UserDto>(
			$"SELECT Id, Name, Email FROM Users WHERE Id = '{userId}'",
			userId,
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("Solo", result.Value.Name);
	}

	#endregion

	#region GetScalarAsync

	[TestMethod]
	public async Task GetScalarAsync_ReturnsScalarValue() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.GetScalarAsync<long>(
			"SELECT COUNT(*) FROM Users",
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(1L, result.Value);
	}

	[TestMethod]
	public async Task GetScalarAsync_WithParameters_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.GetScalarAsync<long>(
			"SELECT COUNT(*) FROM Users WHERE Name = @Name",
			new { Name = "John" },
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(1L, result.Value);
	}

	[TestMethod]
	public async Task GetScalarAsync_WithMapper_TransformsValue() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.GetScalarAsync<long, string>(
			"SELECT COUNT(*) FROM Users",
			count => $"Total: {count}",
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("Total: 2", result.Value);
	}

	[TestMethod]
	public async Task GetScalarAsync_WhenNull_ReturnsFailure() {
		// Act - Empty table, MAX returns NULL
		var result = await this._factory.GetScalarAsync<string>(
			"SELECT MAX(Name) FROM Users",
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<InvalidOperationException>(result.Error);
	}

	[TestMethod]
	public async Task GetScalarAsync_WithMapper_WhenNull_MapperReceivesNull() {
		// Act
		var result = await this._factory.GetScalarAsync<string?, string>(
			"SELECT MAX(Name) FROM Users",
			value => value ?? "Empty",
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("Empty", result.Value);
	}

	#endregion

	#region QueryAnyAsync

	[TestMethod]
	public async Task QueryAnyAsync_ReturnsAllRecords() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.QueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users ORDER BY Name",
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(2, result.Value);
		Assert.AreEqual("User1", result.Value[0].Name);
		Assert.AreEqual("User2", result.Value[1].Name);
	}

	[TestMethod]
	public async Task QueryAnyAsync_WhenEmpty_ReturnsEmptyList() {
		// Act
		var result = await this._factory.QueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users",
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsEmpty(result.Value);
	}

	[TestMethod]
	public async Task QueryAnyAsync_WithParameters_FiltersResults() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "jane@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.QueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Name = @Name",
			new { Name = "John" },
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(1, result.Value);
		Assert.AreEqual("John", result.Value[0].Name);
	}

	[TestMethod]
	public async Task QueryAnyAsync_WithMapper_TransformsResults() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "john", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.QueryAnyAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users",
			dto => new User(dto.Id, dto.Name.ToUpperInvariant(), dto.Email),
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(1, result.Value);
		Assert.AreEqual("JOHN", result.Value[0].Name);
	}

	#endregion

	#region InsertAsync

	[TestMethod]
	public async Task InsertAsync_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
	}

	[TestMethod]
	public async Task InsertAsync_WithResultSelector_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.InsertAndReturnAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			() => userId,
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(userId, result.Value);
	}

	#endregion

	#region UpdateAsync

	[TestMethod]
	public async Task UpdateAsync_WhenRowsAffected_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.UpdateAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = userId, Name = "Jane" },
			userId,
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
	}

	[TestMethod]
	public async Task UpdateAsync_WhenNoRowsAffected_ReturnsNotFound() {
		// Act
		var result = await this._factory.UpdateAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = "nonexistent", Name = "Jane" },
			"nonexistent",
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
	}

	[TestMethod]
	public async Task UpdateAsync_WithResultSelector_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.UpdateAndReturnAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = userId, Name = "Jane" },
			userId,
			() => "Updated",
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("Updated", result.Value);
	}

	#endregion

	#region DeleteAsync

	[TestMethod]
	public async Task DeleteAsync_WhenRowsAffected_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.DeleteAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
	}

	[TestMethod]
	public async Task DeleteAsync_WhenNoRowsAffected_ReturnsNotFound() {
		// Act
		var result = await this._factory.DeleteAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			"nonexistent",
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
	}

	#endregion

	#region Fluent Chaining via Factory

	[TestMethod]
	public async Task Factory_ExecuteTransactionAsync_FluentChaining_Works() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act - Full fluent chain through factory
		var result = await this._factory.ExecuteTransactionAsync<OrderDto>(ctx => ctx
			.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				id => new { Id = orderId, UserId = id, Amount = 100.0 },
				_ => orderId)
			.ThenGetAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
				id => new { Id = id },
				orderId)
		, this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(orderId, result.Value.Id);
		Assert.AreEqual(userId, result.Value.UserId);
		Assert.AreEqual(100.0, result.Value.Amount);
	}

	[TestMethod]
	public async Task Factory_ExecuteTransactionAsync_FluentChaining_WhereFailure_RollsBack() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act - Insert user, insert order, then fail validation on order amount
		var result = await this._factory.ExecuteTransactionAsync<OrderDto>(ctx => ctx
			.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				id => new { Id = orderId, UserId = id, Amount = 50.0 },
				_ => new OrderDto(orderId, userId, 50.0))
			.EnsureAsync(
				order => order.Amount >= 100.0,
				new BadRequestException("Order amount must be at least 100"))
			.ThenGetAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
				order => new { order.Id },
				orderId)
		, this.TestContext.CancellationToken);

		// Assert - The chain should have failed at EnsureAsync
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<BadRequestException>(result.Error);
		Assert.AreEqual("Order amount must be at least 100", result.Error.Message);

		// Verify rollback - neither user nor order should exist
		var userResult = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			this.TestContext.CancellationToken);
		Assert.IsTrue(userResult.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(userResult.Error);

		var orderResult = await this._factory.GetAsync<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId },
			orderId,
			this.TestContext.CancellationToken);
		Assert.IsTrue(orderResult.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(orderResult.Error);
	}

	#endregion

	#region MultipleGetAsync

	[TestMethod]
	public async Task MultipleGetAsync_ReturnsValue_WhenMapperReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.MultipleGetAsync<UserWithOrders>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = userId },
			[userId],
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) return null;
				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return new UserWithOrders(user, orders);
			},
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual("John", result.Value.User.Name);
		Assert.HasCount(1, result.Value.Orders);
	}

	[TestMethod]
	public async Task MultipleGetAsync_ReturnsNotFound_WhenMapperReturnsNull() {
		// Act
		var result = await this._factory.MultipleGetAsync<UserWithOrders>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			SELECT Id, UserId, Amount FROM Orders WHERE UserId = @Id;
			""",
			new { Id = "nonexistent" },
			["nonexistent"],
			async reader => {
				var user = await reader.ReadSingleOrDefaultAsync<UserDto>();
				if (user is null) return null;
				var orders = (await reader.ReadAsync<OrderDto>()).ToList();
				return new UserWithOrders(user, orders);
			},
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsFailure);
		Assert.IsInstanceOfType<NotFoundException>(result.Error);
	}

	[TestMethod]
	public async Task MultipleGetAsync_WithoutParameters_Works() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.MultipleGetAsync<int>(
			"SELECT COUNT(*) FROM Users; SELECT COUNT(*) FROM Orders;",
			["counts"],
			async reader => {
				var userCount = await reader.ReadSingleOrDefaultAsync<int>();
				var orderCount = await reader.ReadSingleOrDefaultAsync<int>();
				return userCount + orderCount;
			},
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(1, result.Value);
	}

	#endregion

	#region MultipleGetOptionalAsync

	[TestMethod]
	public async Task MultipleGetOptionalAsync_ReturnsOptionalWithValue_WhenMapperReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.MultipleGetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
			new { Id = userId },
			async reader => await reader.ReadSingleOrDefaultAsync<UserDto>(),
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsTrue(result.Value.HasValue);
		Assert.AreEqual("John", result.Value.Value.Name);
	}

	[TestMethod]
	public async Task MultipleGetOptionalAsync_ReturnsEmptyOptional_WhenMapperReturnsNull() {
		// Act
		var result = await this._factory.MultipleGetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
			new { Id = "nonexistent" },
			async reader => await reader.ReadSingleOrDefaultAsync<UserDto>(),
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsFalse(result.Value.HasValue);
	}

	[TestMethod]
	public async Task MultipleGetOptionalAsync_WithoutParameters_Works() {
		// Act
		var result = await this._factory.MultipleGetOptionalAsync<int>(
			"SELECT COUNT(*) FROM Users;",
			async reader => await reader.ReadSingleOrDefaultAsync<int>(),
			this.TestContext.CancellationToken);

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
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.MultipleQueryAnyAsync<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId;",
			new { UserId = userId },
			async reader => [.. (await reader.ReadAsync<OrderDto>())],
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(2, result.Value);
	}

	[TestMethod]
	public async Task MultipleQueryAnyAsync_ReturnsEmptyList_WhenNoResults() {
		// Act
		var result = await this._factory.MultipleQueryAnyAsync<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId;",
			new { UserId = "nonexistent" },
			async reader => [.. (await reader.ReadAsync<OrderDto>())],
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.IsEmpty(result.Value);
	}

	[TestMethod]
	public async Task MultipleQueryAnyAsync_WithoutParameters_Works() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.MultipleQueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users;",
			async reader => [.. (await reader.ReadAsync<UserDto>())],
			this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.HasCount(2, result.Value);
	}

	#endregion

	#region InsertWithCountAsync

	[TestMethod]
	public async Task InsertWithCountAsync_ReturnsRowCount_WhenInsertSucceeds() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.InsertWithCountAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(1, result.Value);
	}

	[TestMethod]
	public async Task InsertWithCountAsync_ReturnsZero_WhenConditionalInsertMatchesNothing() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act - Try to insert only if email doesn't exist (it does, so 0 rows)
		var result = await this._factory.InsertWithCountAsync(
			"""
			INSERT INTO Users (Id, Name, Email)
			SELECT @Id, @Name, @Email
			WHERE NOT EXISTS (SELECT 1 FROM Users WHERE Email = @Email)
			""",
			new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(0, result.Value);
	}

	#endregion

	#region UpdateWithCountAsync

	[TestMethod]
	public async Task UpdateWithCountAsync_ReturnsRowCount_WhenUpdateSucceeds() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.UpdateWithCountAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = userId, Name = "Johnny" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(1, result.Value);
	}

	[TestMethod]
	public async Task UpdateWithCountAsync_ReturnsZero_WhenNoRowsMatch() {
		// Act
		var result = await this._factory.UpdateWithCountAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = Guid.NewGuid().ToString(), Name = "Johnny" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(0, result.Value);
	}

	[TestMethod]
	public async Task UpdateWithCountAsync_ReturnsMultiple_WhenMultipleRowsMatch() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john1@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john2@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.UpdateWithCountAsync(
			"UPDATE Users SET Name = @NewName WHERE Name = @OldName",
			new { OldName = "John", NewName = "Johnny" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(2, result.Value);
	}

	#endregion

	#region DeleteWithCountAsync

	[TestMethod]
	public async Task DeleteWithCountAsync_ReturnsRowCount_WhenDeleteSucceeds() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = userId },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(1, result.Value);
	}

	[TestMethod]
	public async Task DeleteWithCountAsync_ReturnsZero_WhenNoRowsMatch() {
		// Act
		var result = await this._factory.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = Guid.NewGuid().ToString() },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(0, result.Value);
	}

	[TestMethod]
	public async Task DeleteWithCountAsync_ReturnsMultiple_WhenMultipleRowsMatch() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john1@test.com" },
			cancellationToken: this.TestContext.CancellationToken);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john2@test.com" },
			cancellationToken: this.TestContext.CancellationToken);

		// Act
		var result = await this._factory.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Name = @Name",
			new { Name = "John" },
			cancellationToken: this.TestContext.CancellationToken);

		// Assert
		Assert.IsTrue(result.IsSuccess);
		Assert.AreEqual(2, result.Value);
	}

	#endregion

	#region Test DTOs

	private record UserDto(string Id, string Name, string Email);
	private record User(string Id, string Name, string Email);
	private record OrderDto(string Id, string UserId, double Amount);
	private record UserWithOrders(UserDto User, IReadOnlyList<OrderDto> Orders);

	public TestContext TestContext { get; set; }

	#endregion

}
