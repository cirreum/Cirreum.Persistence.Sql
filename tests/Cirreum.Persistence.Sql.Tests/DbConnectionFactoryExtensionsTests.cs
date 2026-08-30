namespace Cirreum.Persistence.Sql.Tests;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;

public sealed class DbConnectionFactoryExtensionsTests : IDisposable {

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

	private readonly TestConnectionFactory _factory = new();

	public void Dispose() {
		this._factory.Dispose();
	}

	#region ExecuteAsync

	[Fact]
	public async Task ExecuteAsync_NonGeneric_ExecutesAction() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.ExecuteAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
	}

	[Fact]
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
		}, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("John");
	}

	#endregion

	#region ExecuteTransactionAsync

	[Fact]
	public async Task ExecuteTransactionAsync_NonGeneric_CommitsOnSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();

		// Verify commit by reading from a new connection
		var verifyResult = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			CancellationToken.None);
		verifyResult.IsSuccess.Should().BeTrue();
	}

	[Fact]
	public async Task ExecuteTransactionAsync_Generic_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(userId);
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();

		// Verify rollback - user should not exist
		var verifyResult = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			CancellationToken.None);
		verifyResult.IsFailure.Should().BeTrue();
		verifyResult.Error.Should().BeOfType<NotFoundException>();
	}

	#endregion

	#region GetAsync

	[Fact]
	public async Task GetAsync_WhenRecordExists_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("John");
	}

	[Fact]
	public async Task GetAsync_WhenRecordDoesNotExist_ReturnsNotFound() {
		// Act
		var result = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			"nonexistent",
			CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	[Fact]
	public async Task GetAsync_WithMapper_TransformsResult() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Jane", Email = "jane@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.GetAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			dto => new User(dto.Id, dto.Name.ToUpperInvariant(), dto.Email),
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("JANE");
	}

	[Fact]
	public async Task GetAsync_WithoutParameters_Works() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Solo", Email = "solo@test.com" },
			cancellationToken: CancellationToken.None);

		// Act - Use overload without parameters
		var result = await this._factory.GetAsync<UserDto>(
			$"SELECT Id, Name, Email FROM Users WHERE Id = '{userId}'",
			userId,
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("Solo");
	}

	#endregion

	#region GetScalarAsync

	[Fact]
	public async Task GetScalarAsync_ReturnsScalarValue() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.GetScalarAsync<long>(
			"SELECT COUNT(*) FROM Users",
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1L);
	}

	[Fact]
	public async Task GetScalarAsync_WithParameters_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.GetScalarAsync<long>(
			"SELECT COUNT(*) FROM Users WHERE Name = @Name",
			new { Name = "John" },
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1L);
	}

	[Fact]
	public async Task GetScalarAsync_WithMapper_TransformsValue() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.GetScalarAsync<long, string>(
			"SELECT COUNT(*) FROM Users",
			count => $"Total: {count}",
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Total: 2");
	}

	[Fact]
	public async Task GetScalarAsync_WhenNull_ReturnsFailure() {
		// Act - Empty table, MAX returns NULL
		var result = await this._factory.GetScalarAsync<string>(
			"SELECT MAX(Name) FROM Users",
			CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidOperationException>();
	}

	[Fact]
	public async Task GetScalarAsync_WithMapper_WhenNull_MapperReceivesNull() {
		// Act
		var result = await this._factory.GetScalarAsync<string?, string>(
			"SELECT MAX(Name) FROM Users",
			value => value ?? "Empty",
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Empty");
	}

	#endregion

	#region QueryAnyAsync

	[Fact]
	public async Task QueryAnyAsync_ReturnsAllRecords() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.QueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users ORDER BY Name",
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(2);
		result.Value[0].Name.Should().Be("User1");
		result.Value[1].Name.Should().Be("User2");
	}

	[Fact]
	public async Task QueryAnyAsync_WhenEmpty_ReturnsEmptyList() {
		// Act
		var result = await this._factory.QueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users",
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeEmpty();
	}

	[Fact]
	public async Task QueryAnyAsync_WithParameters_FiltersResults() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "jane@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.QueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Name = @Name",
			new { Name = "John" },
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(1);
		result.Value[0].Name.Should().Be("John");
	}

	[Fact]
	public async Task QueryAnyAsync_WithMapper_TransformsResults() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "john", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.QueryAnyAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users",
			dto => new User(dto.Id, dto.Name.ToUpperInvariant(), dto.Email),
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(1);
		result.Value[0].Name.Should().Be("JOHN");
	}

	#endregion

	#region InsertAsync

	[Fact]
	public async Task InsertAsync_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
	}

	[Fact]
	public async Task InsertAsync_WithResultSelector_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.InsertAndReturnAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			() => userId,
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(userId);
	}

	#endregion

	#region UpdateAsync

	[Fact]
	public async Task UpdateAsync_WhenRowsAffected_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.UpdateAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = userId, Name = "Jane" },
			userId,
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
	}

	[Fact]
	public async Task UpdateAsync_WhenNoRowsAffected_ReturnsNotFound() {
		// Act
		var result = await this._factory.UpdateAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = "nonexistent", Name = "Jane" },
			"nonexistent",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	[Fact]
	public async Task UpdateAsync_WithResultSelector_ReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.UpdateAndReturnAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = userId, Name = "Jane" },
			userId,
			() => "Updated",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Updated");
	}

	#endregion

	#region DeleteAsync

	[Fact]
	public async Task DeleteAsync_WhenRowsAffected_ReturnsSuccess() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.DeleteAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
	}

	[Fact]
	public async Task DeleteAsync_WhenNoRowsAffected_ReturnsNotFound() {
		// Act
		var result = await this._factory.DeleteAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			"nonexistent",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	#endregion

	#region Fluent Chaining via Factory

	[Fact]
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
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Id.Should().Be(orderId);
		result.Value.UserId.Should().Be(userId);
		result.Value.Amount.Should().Be(100.0);
	}

	[Fact]
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
		, CancellationToken.None);

		// Assert - The chain should have failed at EnsureAsync
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<BadRequestException>();
		result.Error.Message.Should().Be("Order amount must be at least 100");

		// Verify rollback - neither user nor order should exist
		var userResult = await this._factory.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			CancellationToken.None);
		userResult.IsFailure.Should().BeTrue();
		userResult.Error.Should().BeOfType<NotFoundException>();

		var orderResult = await this._factory.GetAsync<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId },
			orderId,
			CancellationToken.None);
		orderResult.IsFailure.Should().BeTrue();
		orderResult.Error.Should().BeOfType<NotFoundException>();
	}

	#endregion

	#region MultipleGetAsync

	[Fact]
	public async Task MultipleGetAsync_ReturnsValue_WhenMapperReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 },
			cancellationToken: CancellationToken.None);

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
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.User.Name.Should().Be("John");
		result.Value.Orders.Should().HaveCount(1);
	}

	[Fact]
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
			CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	[Fact]
	public async Task MultipleGetAsync_WithoutParameters_Works() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.MultipleGetAsync<int>(
			"SELECT COUNT(*) FROM Users; SELECT COUNT(*) FROM Orders;",
			["counts"],
			async reader => {
				var userCount = await reader.ReadSingleOrDefaultAsync<int>();
				var orderCount = await reader.ReadSingleOrDefaultAsync<int>();
				return userCount + orderCount;
			},
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	#endregion

	#region MultipleGetOptionalAsync

	[Fact]
	public async Task MultipleGetOptionalAsync_ReturnsOptionalWithValue_WhenMapperReturnsValue() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.MultipleGetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
			new { Id = userId },
			async reader => await reader.ReadSingleOrDefaultAsync<UserDto>(),
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Name.Should().Be("John");
	}

	[Fact]
	public async Task MultipleGetOptionalAsync_ReturnsEmptyOptional_WhenMapperReturnsNull() {
		// Act
		var result = await this._factory.MultipleGetOptionalAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id;",
			new { Id = "nonexistent" },
			async reader => await reader.ReadSingleOrDefaultAsync<UserDto>(),
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeFalse();
	}

	[Fact]
	public async Task MultipleGetOptionalAsync_WithoutParameters_Works() {
		// Act
		var result = await this._factory.MultipleGetOptionalAsync<int>(
			"SELECT COUNT(*) FROM Users;",
			async reader => await reader.ReadSingleOrDefaultAsync<int>(),
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.HasValue.Should().BeTrue();
		result.Value.Value.Should().Be(0);
	}

	#endregion

	#region MultipleQueryAnyAsync

	[Fact]
	public async Task MultipleQueryAnyAsync_ReturnsList_WhenMapperReturnsList() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 200.0 },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.MultipleQueryAnyAsync<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId;",
			new { UserId = userId },
			async reader => [.. (await reader.ReadAsync<OrderDto>())],
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(2);
	}

	[Fact]
	public async Task MultipleQueryAnyAsync_ReturnsEmptyList_WhenNoResults() {
		// Act
		var result = await this._factory.MultipleQueryAnyAsync<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId;",
			new { UserId = "nonexistent" },
			async reader => [.. (await reader.ReadAsync<OrderDto>())],
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeEmpty();
	}

	[Fact]
	public async Task MultipleQueryAnyAsync_WithoutParameters_Works() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.MultipleQueryAnyAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users;",
			async reader => [.. (await reader.ReadAsync<UserDto>())],
			CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(2);
	}

	#endregion

	#region InsertWithCountAsync

	[Fact]
	public async Task InsertWithCountAsync_ReturnsRowCount_WhenInsertSucceeds() {
		// Arrange
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await this._factory.InsertWithCountAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task InsertWithCountAsync_ReturnsZero_WhenConditionalInsertMatchesNothing() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act - Try to insert only if email doesn't exist (it does, so 0 rows)
		var result = await this._factory.InsertWithCountAsync(
			"""
			INSERT INTO Users (Id, Name, Email)
			SELECT @Id, @Name, @Email
			WHERE NOT EXISTS (SELECT 1 FROM Users WHERE Email = @Email)
			""",
			new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(0);
	}

	#endregion

	#region UpdateWithCountAsync

	[Fact]
	public async Task UpdateWithCountAsync_ReturnsRowCount_WhenUpdateSucceeds() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.UpdateWithCountAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = userId, Name = "Johnny" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task UpdateWithCountAsync_ReturnsZero_WhenNoRowsMatch() {
		// Act
		var result = await this._factory.UpdateWithCountAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = Guid.NewGuid().ToString(), Name = "Johnny" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(0);
	}

	[Fact]
	public async Task UpdateWithCountAsync_ReturnsMultiple_WhenMultipleRowsMatch() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john1@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john2@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.UpdateWithCountAsync(
			"UPDATE Users SET Name = @NewName WHERE Name = @OldName",
			new { OldName = "John", NewName = "Johnny" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(2);
	}

	#endregion

	#region DeleteWithCountAsync

	[Fact]
	public async Task DeleteWithCountAsync_ReturnsRowCount_WhenDeleteSucceeds() {
		// Arrange
		var userId = Guid.NewGuid().ToString();
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = userId },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task DeleteWithCountAsync_ReturnsZero_WhenNoRowsMatch() {
		// Act
		var result = await this._factory.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = Guid.NewGuid().ToString() },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(0);
	}

	[Fact]
	public async Task DeleteWithCountAsync_ReturnsMultiple_WhenMultipleRowsMatch() {
		// Arrange
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john1@test.com" },
			cancellationToken: CancellationToken.None);
		await this._factory.InsertAsync(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john2@test.com" },
			cancellationToken: CancellationToken.None);

		// Act
		var result = await this._factory.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Name = @Name",
			new { Name = "John" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(2);
	}

	#endregion

	#region Test DTOs

	private record UserDto(string Id, string Name, string Email);
	private record User(string Id, string Name, string Email);
	private record OrderDto(string Id, string UserId, double Amount);
	private record UserWithOrders(UserDto User, IReadOnlyList<OrderDto> Orders);

	#endregion

}
