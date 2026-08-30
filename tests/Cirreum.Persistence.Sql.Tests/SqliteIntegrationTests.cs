namespace Cirreum.Persistence.Sql.Tests;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Data;

public sealed class SqliteIntegrationTests {

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

			CREATE TABLE Orders (
				Id TEXT PRIMARY KEY,
				UserId TEXT NOT NULL,
				Amount REAL NOT NULL,
				FOREIGN KEY (UserId) REFERENCES Users(Id)
			);
			""");
	}

	#region Query Methods

	[Fact]
	public async Task GetAsync_WhenRecordExists_ReturnsSuccess() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId, cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("John");
		result.Value.Email.Should().Be("john@test.com");
	}

	[Fact]
	public async Task GetAsync_WhenRecordDoesNotExist_ReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.GetAsync<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			"nonexistent", cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	[Fact]
	public async Task GetAsync_WithMapper_TransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Jane", Email = "jane@test.com" });

		// Act
		var result = await conn.GetAsync<UserDto, User>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId },
			userId,
			dto => new User(dto.Id, dto.Name, dto.Email), cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("Jane");
	}

	[Fact]
	public async Task QueryAnyAsync_ReturnsAllRecords() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" });

		// Act
		var result = await conn.QueryAnyAsync<UserDto>("SELECT Id, Name, Email FROM Users", cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(2);
	}

	[Fact]
	public async Task QueryAnyAsync_WhenEmpty_ReturnsEmptyList() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.QueryAnyAsync<UserDto>("SELECT Id, Name, Email FROM Users", cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().BeEmpty();
	}

	[Fact]
	public async Task GetScalarAsync_ReturnsScalarValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" });

		// Act
		var result = await conn.GetScalarAsync<long>("SELECT COUNT(*) FROM Users", cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(2L);
	}

	[Fact]
	public async Task GetScalarAsync_WithMapper_TransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });

		// Act
		var result = await conn.GetScalarAsync<long, string>(
			"SELECT COUNT(*) FROM Users",
			count => $"Total: {count}", cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Total: 1");
	}

	#endregion

	#region TransactionBuilder Basic Operations

	[Fact]
	public async Task TransactionBuilder_GetAsync_ReturnsDbResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Test", Email = "test@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("Test");
	}

	[Fact]
	public async Task TransactionBuilder_GetScalarAsync_ReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "Test", Email = "test@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync(async db => {
			return await db.GetScalarAsync<long>("SELECT COUNT(*) FROM Users");
		}, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1L);
	}

	[Fact]
	public async Task TransactionBuilder_QueryAnyAsync_ReturnsResults() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User1", Email = "user1@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "User2", Email = "user2@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync(async db => {
			return await db.QueryAnyAsync<UserDto>("SELECT Id, Name, Email FROM Users");
		}, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(2);
	}

	#endregion

	#region Fluent Chaining

	[Fact]
	public async Task FluentChaining_GetThenMap_Success() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<User>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.MapAsync(u => new User(u.Id, u.Name, u.Email))
, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("John");
	}

	[Fact]
	public async Task FluentChaining_GetThenWhere_PassesFilter() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "ValidUser", Email = "valid@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.EnsureAsync(u => u.Name.StartsWith("Valid"), new BadRequestException("Invalid user"))
, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("ValidUser");
	}

	[Fact]
	public async Task FluentChaining_GetThenEnsure_FailsValidation() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "InvalidUser", Email = "invalid@test.com" });
		var expectedError = new BadRequestException("User must start with 'Valid'");

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.EnsureAsync(u => u.Name.StartsWith("Valid"), expectedError)
, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(expectedError);
	}

	[Fact]
	public async Task FluentChaining_GetThenGetScalar_ChainsOperations() {
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
		var result = await conn.ExecuteTransactionAsync<long>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenGetScalarAsync<long>(
				"SELECT COUNT(*) FROM Orders WHERE UserId = @UserId",
				new { UserId = userId })
, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(2L);
	}

	[Fact]
	public async Task FluentChaining_GetThenQueryAny_ChainsOperations() {
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
		var result = await conn.ExecuteTransactionAsync<IReadOnlyList<OrderDto>>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenQueryAnyAsync<OrderDto>(
				"SELECT Id, UserId, Amount FROM Orders WHERE UserId = @UserId",
				new { UserId = userId })
, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(2);
		result.Value[0].Amount.Should().Be(100.0);
		result.Value[1].Amount.Should().Be(200.0);
	}

	[Fact]
	public async Task FluentChaining_NotFoundPropagates_ThroughChain() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var scalarExecuted = false;

		// Act
		var result = await conn.ExecuteTransactionAsync<long>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = "nonexistent" },
				"nonexistent")
			.ThenGetScalarAsync<long>("SELECT COUNT(*) FROM Orders")
			.MapAsync(count => {
				scalarExecuted = true;
				return count;
			})
, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
		scalarExecuted.Should().BeFalse("Subsequent operations should not execute after failure");
	}

	[Fact]
	public async Task FluentChaining_MultipleGetOperations() {
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
		var result = await conn.ExecuteTransactionAsync<UserDto>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user1Id },
				user1Id)
			.ThenGetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = user2Id },
				user2Id)
, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("User2"); // Second get result
	}

	#endregion

	#region Insert/Update/Delete Chaining

	[Fact]
	public async Task FluentChaining_GetThenInsert_ReturnsDbResultNonGeneric() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 150.0 })
			.ToResult()
, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		count.Should().Be(1L);
	}

	[Fact]
	public async Task FluentChaining_GetThenInsertWithParametersFactory_UsesPreviousValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act - use the fetched user's ID in the insert
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 250.0 })
			.ToResult()
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QueryFirstOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().NotBeNull();
		order.UserId.Should().Be(userId);
		order.Amount.Should().Be(250.0);
	}

	[Fact]
	public async Task FluentChaining_GetThenInsertWithResultSelector_ReturnsDbResultT() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 350.0 },
				_ => orderId)
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(orderId);
	}

	[Fact]
	public async Task FluentChaining_GetThenInsertWithFactoryAndResultSelector_ReturnsDbResultT() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<(string OrderId, string UserName)>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 450.0 },
				_ => (orderId, "John"))
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.OrderId.Should().Be(orderId);
		result.Value.UserName.Should().Be("John");
	}

	[Fact]
	public async Task FluentChaining_InsertThenGetScalar_ChainsFromNonGeneric() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act - Insert returns DbResultNonGeneric, then chain to ThenGetScalarAsync
		var result = await conn.ExecuteTransactionAsync<long>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 })
			.ThenGetScalarAsync<long>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId })
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1L);
	}

	[Fact]
	public async Task FluentChaining_InsertThenInsert_ChainsMultipleInserts() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId1 = Guid.NewGuid().ToString();
		var orderId2 = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId1, UserId = user.Id, Amount = 100.0 })
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId2, UserId = userId, Amount = 200.0 })
			.ToResult()
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId });
		count.Should().Be(2L);
	}

	[Fact]
	public async Task FluentChaining_GetThenUpdate_ReturnsDbResultNonGeneric() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "Jane" },
				userId)
			.ToResult()
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var updatedName = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		updatedName.Should().Be("Jane");
	}

	[Fact]
	public async Task FluentChaining_GetThenUpdateWithParametersFactory_UsesPreviousValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - update based on fetched user
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @NewName WHERE Id = @Id",
				user => new { user.Id, NewName = user.Name + " Updated" },
				userId)
			.ToResult()
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var updatedName = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		updatedName.Should().Be("John Updated");
	}

	[Fact]
	public async Task FluentChaining_GetThenUpdateWithResultSelector_ReturnsDbResultT() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "Jane" },
				userId,
				_ => "Updated successfully")
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Updated successfully");
	}

	[Fact]
	public async Task FluentChaining_GetThenDelete_ReturnsDbResultNonGeneric() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenDeleteAsync(
				"DELETE FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ToResult()
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		count.Should().Be(0L);
	}

	[Fact]
	public async Task FluentChaining_GetThenDeleteWithParametersFactory_UsesPreviousValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenDeleteAsync(
				"DELETE FROM Users WHERE Id = @Id",
				user => new { user.Id },
				userId)
			.ToResult()
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		count.Should().Be(0L);
	}

	[Fact]
	public async Task FluentChaining_FailurePropagates_ThroughInsertChain() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var insertExecuted = false;

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = "nonexistent" },
				"nonexistent")
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				_ => {
					insertExecuted = true;
					return new { Id = Guid.NewGuid().ToString(), UserId = "test", Amount = 100.0 };
				})
			.ToResult()
			, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
		insertExecuted.Should().BeFalse("Insert should not execute after failure");
	}

	[Fact]
	public async Task FluentChaining_ComplexChain_GetInsertUpdateGetScalar() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act - Complex chain: Get user -> Insert order -> Update user -> Get count
		var result = await conn.ExecuteTransactionAsync<long>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 500.0 })
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "John (with order)" },
				userId)
			.ThenGetScalarAsync<long>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId })
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1L);
		var updatedName = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		updatedName.Should().Be("John (with order)");
	}

	[Fact]
	public async Task FluentChaining_DbResultNonGeneric_ThenInsertWithResultSelector() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId1 = Guid.NewGuid().ToString();
		var orderId2 = Guid.NewGuid().ToString();

		// Act - Insert returning non-generic, then insert returning generic
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId1, UserId = userId, Amount = 100.0 })
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId2, UserId = userId, Amount = 200.0 },
				_ => orderId2)
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(orderId2);
	}

	#endregion

	#region TransactionBuilder Direct Insert/Update/Delete Chaining

	[Fact]
	public async Task TransactionBuilder_InsertThenUpdate_ChainsFromInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act - Start with InsertAsync directly on TransactionBuilder, then chain UpdateAsync
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "Jane" },
				userId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("Jane");
	}

	[Fact]
	public async Task TransactionBuilder_InsertThenInsert_ChainsMultipleInserts() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 100.0 })
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var userCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		var orderCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		userCount.Should().Be(1L);
		orderCount.Should().Be(1L);
	}

	[Fact]
	public async Task TransactionBuilder_InsertThenGetAsync_ChainsToQuery() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<UserDto>(db =>
			db.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenGetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("John");
		result.Value.Email.Should().Be("john@test.com");
	}

	[Fact]
	public async Task TransactionBuilder_InsertWithResultSelector_ThenUpdate_ChainsFromTypedInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act - InsertAsync<T> returns DbResult<T>, then chain to ThenUpdateAsync
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				id => new { Id = id, Name = "Updated via chain" },
				userId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("Updated via chain");
	}

	[Fact]
	public async Task TransactionBuilder_UpdateThenDelete_ChainsFromUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.UpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "Jane" },
				userId)
			.ThenDeleteAsync(
				"DELETE FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		count.Should().Be(0L);
	}

	[Fact]
	public async Task TransactionBuilder_DeleteThenInsert_ChainsFromDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var oldUserId = Guid.NewGuid().ToString();
		var newUserId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = oldUserId, Name = "OldUser", Email = "old@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync(db =>
			db.DeleteAsync(
				"DELETE FROM Users WHERE Id = @Id",
				new { Id = oldUserId },
				oldUserId)
			.ThenInsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = newUserId, Name = "NewUser", Email = "new@test.com" })
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var oldCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = oldUserId });
		var newCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = newUserId });
		oldCount.Should().Be(0L);
		newCount.Should().Be(1L);
	}

	[Fact]
	public async Task TransactionBuilder_InsertThenUpdateThenGetScalar_ComplexChain() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act - Complex chain starting from InsertAsync
		var result = await conn.ExecuteTransactionAsync<long>(db =>
			db.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 500.0 })
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "John (with order)" },
				userId)
			.ThenGetScalarAsync<long>("SELECT COUNT(*) FROM Orders")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1L);
		var userName = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		userName.Should().Be("John (with order)");
	}

	#endregion

	#region Complex Chaining Patterns: Mutator -> Query -> Mutator -> Query

	[Fact]
	public async Task Insert_ThenGet_ThenUpdate_ChainsWithParametersFactory() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act: Insert -> Get (using parametersFactory) -> Update (using parametersFactory)
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenGetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				id => new { Id = id },
				userId)
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = $"{user.Name} (verified)" },
				userId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("John (verified)");
	}

	[Fact]
	public async Task Insert_ThenGetScalar_ThenInsert_ChainsWithScalarValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act: Insert user -> Get scalar count -> Insert order using count in amount
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenGetScalarAsync<long>(
				"SELECT COUNT(*) FROM Users",
				_ => null)
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				count => new { Id = orderId, UserId = userId, Amount = (double)(count * 100) },
				_ => orderId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(orderId);
		var amount = rawConn.ExecuteScalar<double>("SELECT Amount FROM Orders WHERE Id = @Id", new { Id = orderId });
		amount.Should().Be(100.0); // 1 user * 100
	}

	[Fact]
	public async Task Insert_ThenWhere_ThenUpdate_ChainsWithValidation() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act: Insert -> Ensure (validate) -> Update
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => new UserDto(userId, "John", "john@test.com"))
			.EnsureAsync(user => user.Email.Contains('@'), new BadRequestException("Invalid email"))
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = $"{user.Name} (email verified)" },
				userId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("John (email verified)");
	}

	[Fact]
	public async Task Insert_ThenEnsure_Fails_ShortCircuits() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act: Insert -> Ensure (fails) -> Update (should be skipped)
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "invalid-email" },
				() => new UserDto(userId, "John", "invalid-email"))
			.EnsureAsync(user => user.Email.Contains('@'), new BadRequestException("Invalid email"))
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = "Should not reach here" },
				userId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<BadRequestException>();
		// Transaction was rolled back, so no user exists
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		count.Should().Be(0L); // Insert was rolled back
	}

	[Fact]
	public async Task Update_ThenGet_ThenDelete_ThenQueryAny() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId1 = Guid.NewGuid().ToString();
		var userId2 = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId1, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId2, Name = "Jane", Email = "jane@test.com" });

		// Act: Update -> Get -> Delete -> QueryAny
		var result = await conn.ExecuteTransactionAsync<IReadOnlyList<UserDto>>(db =>
			db.UpdateAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId1, Name = "John Updated" },
				userId1,
				() => userId1)
			.ThenGetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				id => new { Id = id },
				userId1)
			.ThenDeleteAsync(
				"DELETE FROM Users WHERE Id = @Id",
				user => new { user.Id },
				userId1)
			.ThenQueryAnyAsync<UserDto>("SELECT Id, Name, Email FROM Users")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().HaveCount(1); // Only Jane remains
		result.Value[0].Name.Should().Be("Jane");
	}

	[Fact]
	public async Task Insert_ThenQueryAny_ThenInsert_UsingQueryResults() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Pre-insert a user
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = "existing-user", Name = "Existing", Email = "existing@test.com" });

		// Act: Insert new user -> QueryAny all users -> Insert order for first user
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "New User", Email = "new@test.com" },
				() => userId)
			.ThenQueryAnyAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users ORDER BY Name",
				_ => null)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				users => new { Id = orderId, UserId = users[0].Id, Amount = users.Count * 50.0 })
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QueryFirst<OrderDto>("SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id", new { Id = orderId });
		order.Amount.Should().Be(100.0); // 2 users * 50
	}

	[Fact]
	public async Task Delete_ThenGetScalar_ThenUpdate_ConditionalUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId1 = Guid.NewGuid().ToString();
		var userId2 = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId1, Name = "ToDelete", Email = "delete@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId2, Name = "ToKeep", Email = "keep@test.com" });

		// Act: Delete -> GetScalar (remaining count) -> Update remaining user's name with count
		var result = await conn.ExecuteTransactionAsync(db =>
			db.DeleteAsync(
				"DELETE FROM Users WHERE Id = @Id",
				new { Id = userId1 },
				userId1)
			.ThenGetScalarAsync<long>("SELECT COUNT(*) FROM Users")
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				count => new { Id = userId2, Name = $"Last of {count}" },
				userId2)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId2 });
		name.Should().Be("Last of 1");
	}

	[Fact]
	public async Task Insert_ThenMap_ThenUpdate_TransformsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act: Insert -> Map to uppercase name -> Update with mapped value
		var result = await conn.ExecuteTransactionAsync(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "john doe", Email = "john@test.com" },
				() => new UserDto(userId, "john doe", "john@test.com"))
			.MapAsync(user => user with { Name = user.Name.ToUpperInvariant() })
			.ThenUpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, user.Name },
				userId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("JOHN DOE");
	}

	[Fact]
	public async Task Insert_ThenGet_ThenInsert_ThenGet_LongChain() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act: Insert user -> Get user -> Insert order -> Get order
		var result = await conn.ExecuteTransactionAsync<OrderDto>(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenGetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				id => new { Id = id },
				userId)
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 250.0 },
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
		result.Value.Amount.Should().Be(250.0);
	}

	[Fact]
	public async Task Update_ThenQueryAny_ThenDelete_BatchOperations() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId1 = Guid.NewGuid().ToString();
		var userId2 = Guid.NewGuid().ToString();
		var userId3 = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId1, Name = "Active1", Email = "active1@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId2, Name = "Inactive", Email = "inactive@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId3, Name = "Active2", Email = "active2@test.com" });

		// Act: Update one -> Query all -> Delete based on query (delete the first active)
		var result = await conn.ExecuteTransactionAsync(db =>
			db.UpdateAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId2, Name = "StillInactive" },
				userId2,
				() => userId2)
			.ThenQueryAnyAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Name LIKE 'Active%' ORDER BY Name",
				_ => null)
			.ThenDeleteAsync(
				"DELETE FROM Users WHERE Id = @Id",
				activeUsers => new { activeUsers[0].Id },
				"first-active")
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var remainingCount = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users");
		remainingCount.Should().Be(2L); // One was deleted
		var names = rawConn.Query<string>("SELECT Name FROM Users ORDER BY Name").ToList();
		names.Should().Contain("Active2");
		names.Should().Contain("StillInactive");
		names.Should().NotContain("Active1"); // First active was deleted
	}

	[Fact]
	public async Task InsertWithResultSelector_ThenGetScalar_ThenUpdate_ThenGetScalar_MultipleScalars() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act: Insert -> GetScalar (count before) -> Update -> GetScalar (count after, should be same)
		var result = await conn.ExecuteTransactionAsync<long>(db =>
			db.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "CountTest", Email = "count@test.com" },
				() => 0L) // Placeholder, we'll get actual count next
			.ThenGetScalarAsync<long>(
				"SELECT COUNT(*) FROM Users",
				_ => null)
			.ThenUpdateAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				count => new { Id = userId, Name = $"User #{count}" },
				userId,
				_ => userId)
			.ThenGetScalarAsync<long>(
				"SELECT COUNT(*) FROM Users",
				_ => null)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1L); // Still 1 user
		var name = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		name.Should().Be("User #1");
	}

	#endregion

	#region GetScalarAsync Null Handling

	[Fact]
	public async Task GetScalarAsync_WhenSqlReturnsNull_ReturnsFailureWithInvalidOperationException() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		// No users inserted, so MAX will return NULL

		// Act
		var result = await conn.GetScalarAsync<string>(
			"SELECT MAX(Name) FROM Users",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidOperationException>();
		result.Error.Message.ToLowerInvariant().Should().Contain("null");
	}

	[Fact]
	public async Task GetScalarAsync_WhenSqlReturnsNullableColumnWithValue_ReturnsSuccess() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.GetScalarAsync<string>(
			"SELECT MAX(Name) FROM Users",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("John");
	}

	[Fact]
	public async Task GetScalarAsync_WithCoalesceForNullColumn_ReturnsDefaultValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		// No users, MAX returns NULL, but COALESCE provides default

		// Act
		var result = await conn.GetScalarAsync<string>(
			"SELECT COALESCE(MAX(Name), 'No Users') FROM Users",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("No Users");
	}

	[Fact]
	public async Task GetScalarAsync_WithMapper_WhenSqlReturnsNull_MapperReceivesNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		// No users, MAX returns NULL
		var receivedValue = "not-null-sentinel";

		// Act
		var result = await conn.GetScalarAsync<string?, string>(
			"SELECT MAX(Name) FROM Users",
			value => {
				receivedValue = value;
				return value ?? "Default";
			},
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		receivedValue.Should().BeNull("Mapper should receive null when SQL returns NULL");
		result.Value.Should().Be("Default");
	}

	[Fact]
	public async Task GetScalarAsync_WithMapper_WhenMapperReturnsNull_ReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.GetScalarAsync<string?, string?>(
			"SELECT MAX(Name) FROM Users",
			_ => null!, // Mapper intentionally returns null
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidOperationException>();
		result.Error.Message.ToLowerInvariant().Should().Contain("mapper");
	}

	[Fact]
	public async Task GetScalarAsync_WithMapper_WhenSqlReturnsValue_MapperTransformsIt() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "jane@test.com" });

		// Act
		var result = await conn.GetScalarAsync<long, string>(
			"SELECT COUNT(*) FROM Users",
			count => $"Total users: {count}",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Total users: 2");
	}

	[Fact]
	public async Task GetScalarAsync_WithMapperAndParameters_WhenSqlReturnsNull_MapperReceivesNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		// Query for non-existent user, MAX returns NULL

		// Act
		var result = await conn.GetScalarAsync<string?, string>(
			"SELECT MAX(Name) FROM Users WHERE Id = @Id",
			new { Id = "nonexistent" },
			value => value ?? "Not Found",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Not Found");
	}

	#endregion

	#region GetScalarAsync Null Handling in Transactions

	[Fact]
	public async Task TransactionContext_GetScalarAsync_WhenSqlReturnsNull_ReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetScalarAsync<string>("SELECT MAX(Name) FROM Users")
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidOperationException>();
	}

	[Fact]
	public async Task TransactionContext_GetScalarAsync_WithMapper_MapperReceivesNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetScalarAsync<string?, string>(
				"SELECT MAX(Name) FROM Users",
				value => value ?? "Empty Table")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Empty Table");
	}

	[Fact]
	public async Task FluentChaining_ThenGetScalarAsync_WhenSqlReturnsNull_PropagatesFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Get user, then try to get MAX of a non-existent related table data
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenGetScalarAsync<string>(
				"SELECT MAX(Name) FROM Users WHERE Id = 'nonexistent'") // Returns NULL
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidOperationException>();
	}

	[Fact]
	public async Task FluentChaining_ThenGetScalarAsync_WithMapper_HandlesNull() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenGetScalarAsync<string?, string>(
				"SELECT MAX(Name) FROM Users WHERE Id = 'nonexistent'",
				value => value ?? "No matching users")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("No matching users");
	}

	#endregion

	#region Conditional Operations (when: parameter)

	[Fact]
	public async Task ThenInsertIfAsync_WithWhenTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 },
				when: _ => true)  // Always execute
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().NotBeNull();
		order.Amount.Should().Be(100.0);
	}

	[Fact]
	public async Task ThenInsertIfAsync_WithWhenFalse_SkipsInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 },
				when: _ => false)  // Never execute
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().BeNull("Insert should have been skipped");
	}

	[Fact]
	public async Task ThenInsertIfAsync_WithWhenBasedOnValue_ConditionallExecutes() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Only insert order if user name is "Admin"
		// Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 },
				when: user => user.Name == "Admin")  // Only for Admin users
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().BeNull("Insert should have been skipped because user is not Admin");
	}

	[Fact]
	public async Task ThenInsertIfAsync_Generic_WithWhenFalse_ReturnsResultSelectorValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - when: with resultSelector, resultSelector is Func<T, TResult>
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 },
				when: _ => false,  // Skip insert
				resultSelector: _ => orderId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(orderId, "Result selector should still be called");
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().BeNull("Insert should have been skipped");
	}

	[Fact]
	public async Task ThenInsertIfAsync_Generic_WithWhenTrue_ExecutesInsertAndTransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - when=true with resultSelector: insert executes AND result transforms
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 },
				when: _ => true,  // Execute insert
				resultSelector: user => $"Order for {user.Name}")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Order for John", "Result selector should transform using T value");
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().NotBeNull("Insert should have been executed");
		order.Amount.Should().Be(100.0);
	}

	[Fact]
	public async Task ThenInsertIfAsync_Generic_WithResultSelector_NextOperationReceivesTransformedType() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Chain: Get UserDto -> ThenInsertIfAsync transforms to string -> ThenUpdateAsync uses string
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 },
				when: _ => true,
				resultSelector: _ => orderId)
			.ThenUpdateAsync(  // This should receive string (orderId), not UserDto
				"UPDATE Orders SET Amount = @Amount WHERE Id = @Id",
				oid => new { Id = oid, Amount = 200.0 },  // oid is string
				orderId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingle<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Amount.Should().Be(200.0, "Update should have used the transformed orderId");
	}

	[Fact]
	public async Task ThenInsertIfAsync_Generic_WithResultSelector_WhenFalse_NextOperationReceivesTransformedType() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		// Pre-create the order so we can update it
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 50.0 });

		// Act - when=false: insert skipped, but resultSelector still transforms type for next operation
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = Guid.NewGuid().ToString(), UserId = user.Id, Amount = 999.0 },
				when: _ => false,  // Skip insert
				resultSelector: _ => orderId)
			.ThenUpdateAsync(  // This should still receive string (orderId)
				"UPDATE Orders SET Amount = @Amount WHERE Id = @Id",
				oid => new { Id = oid, Amount = 300.0 },
				orderId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingle<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Amount.Should().Be(300.0, "Update should have used the transformed orderId even when insert was skipped");
	}

	[Fact]
	public async Task ThenUpdateIfAsync_WithWhenFalse_SkipsUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = "UpdatedName" },
				userId,
				when: _ => false)  // Never execute
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("John", "Update should have been skipped");
	}

	[Fact]
	public async Task ThenUpdateIfAsync_WithWhenAndResultSelectorFalse_SkipsUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });


		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserLight>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = "UpdatedName" },
				userId,
				when: _ => false,  // Never execute
				resultSelector: user => new UserLight(user.Id, user.Name))
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingle<UserLight>(
			"SELECT Id, Name FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("John", "Update should have been skipped");
	}

	[Fact]
	public async Task ThenUpdateIfAsync_WithWhenTrue_ExecutesUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = "UpdatedName" },
				userId,
				when: _ => true)  // Always execute
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("UpdatedName");
	}

	[Fact]
	public async Task ThenUpdateIfAsync_WithWhenAndResultSelectorTrue_ExecutesUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserLight>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = "UpdatedName" },
				userId,
				when: _ => true,  // Always execute
				resultSelector: user => new UserLight(user.Id, user.Name))
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingle<UserLight>(
			"SELECT Id, Name FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("UpdatedName");
	}

	[Fact]
	public async Task ThenDeleteIfAsync_WithWhenFalse_SkipsDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenDeleteIfAsync(
				"DELETE FROM Users WHERE Id = @Id",
				user => new { user.Id },
				userId,
				when: _ => false)  // Never execute
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().NotBeNull("Delete should have been skipped");
	}

	[Fact]
	public async Task ThenDeleteIfAsync_WithWhenTrue_ExecutesDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Using generic ExecuteTransactionAsync because when: returns DbResult<T> (pass-through)
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenDeleteIfAsync(
				"DELETE FROM Users WHERE Id = @Id",
				user => new { user.Id },
				userId,
				when: _ => true)  // Always execute
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull("User should be deleted");
	}

	[Fact]
	public async Task ConditionalOperations_ChainedWithMultipleConditions_ExecutesCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId1 = Guid.NewGuid().ToString();
		var orderId2 = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "Admin", Email = "admin@test.com" });

		// Act - Insert first order only for Admin, skip second order
		// With when:, the current T value passes through automatically - no resultSelector needed
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = orderId1, UserId = user.Id, Amount = 100.0 },
				when: user => user.Name == "Admin")  // Should execute, UserDto passes through
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				_ => new { Id = orderId2, UserId = userId, Amount = 200.0 },
				when: _ => false)  // Should skip, UserDto passes through
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();

		var order1 = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId1 });
		order1.Should().NotBeNull("First order should be inserted (Admin user)");

		var order2 = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId2 });
		order2.Should().BeNull("Second order should be skipped");
	}

	#endregion

	#region Non-Generic Conditional Operations (when: Func<bool>)

	[Fact]
	public async Task NonGenericDbResult_ThenInsertIfAsync_WithWhenTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var shouldInsert = true;

		// Act - Start with insert (returns DbResult), then chain another insert with when:
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId });
		count.Should().Be(2L, "Both orders should be inserted");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenInsertIfAsync_WithWhenFalse_SkipsInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var shouldInsert = false;

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: () => shouldInsert)  // External value
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId });
		count.Should().Be(1L, "Only first order should be inserted");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenInsertIfAsyncWithResultSelector_WithWhenFalse_SkipsButReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var shouldInsert = false;

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: () => shouldInsert,
				resultSelector: () => orderId)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(orderId, "Result selector should still return value");
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().BeNull("Second insert should have been skipped");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenInsertIfAsyncWithResultSelector_WithWhenTrue_ExecutesInsertAndReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - when=true: insert executes AND resultSelector returns value
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenInsertIfAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = orderId, UserId = userId, Amount = 100.0 },
				when: () => true,
				resultSelector: () => $"Created order {orderId}")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be($"Created order {orderId}");
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().NotBeNull("Second insert should have been executed");
		order.Amount.Should().Be(100.0);
	}

	[Fact]
	public async Task NonGenericDbResult_ThenUpdateIfAsyncWithResultSelector_WithWhenFalse_SkipsButReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - when=false: update skipped, resultSelector still returns value
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "UpdatedName" },
				userId,
				when: () => false,
				resultSelector: () => $"User {userId}")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be($"User {userId}", "Result selector should return value even when skipped");
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("John", "Update should have been skipped");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenUpdateIfAsyncWithResultSelector_WithWhenTrue_ExecutesUpdateAndReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - when=true: update executes AND resultSelector returns value
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "UpdatedName" },
				userId,
				when: () => true,
				resultSelector: () => $"Updated user {userId}")
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be($"Updated user {userId}");
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("UpdatedName", "Update should have been executed");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenUpdateIfAsync_WithWhenFalse_SkipsUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var shouldUpdate = false;

		// Act - Start with insert (returns DbResult), then chain update with when:
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenUpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "UpdatedName" },
				userId,
				when: () => shouldUpdate)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("John", "Update should have been skipped");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenUpdateIfAsync_WithWhenTrue_ExecutesUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var shouldUpdate = true;

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenUpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "UpdatedName" },
				userId,
				when: () => shouldUpdate)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("UpdatedName");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenDeleteIfAsync_WithWhenFalse_SkipsDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var shouldDelete = false;

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 50.0 })
			.ThenDeleteIfAsync(
				"DELETE FROM Users WHERE Id = @Id",
				() => new { Id = userId },
				userId,
				when: () => shouldDelete)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().NotBeNull("Delete should have been skipped");
	}

	[Fact]
	public async Task NonGenericDbResult_ThenDeleteIfAsync_WithWhenTrue_ExecutesDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var shouldDelete = true;

		// Act - Insert order, then delete the order (not the user, to avoid FK constraint)
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 50.0 })
			.ThenDeleteIfAsync(
				"DELETE FROM Orders WHERE Id = @Id",
				() => new { Id = orderId },
				orderId,
				when: () => shouldDelete)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().BeNull("Order should be deleted");
	}

	[Fact]
	public async Task NonGenericDbResult_ChainedConditionalOperations_HandlerScenario() {
		// Arrange - This simulates the Handler scenario where incoming request has a bool
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		var auditId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Simulate incoming request with ShouldAudit property
		var request = new { ShouldAudit = true };

		// Act - Insert order, then conditionally insert audit log based on request property
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 100.0 })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = auditId, UserId = userId, Amount = 0.0 },  // Using Orders as "audit log" for simplicity
				when: () => request.ShouldAudit)  // Closure captures external value
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId });
		count.Should().Be(2L, "Both order and audit should be inserted");
	}

	[Fact]
	public async Task NonGenericDbResult_ChainedConditionalOperations_HandlerScenario_WhenFalse() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		var auditId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Simulate incoming request with ShouldAudit = false
		var request = new { ShouldAudit = false };

		// Act
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 100.0 })
			.ThenInsertIfAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				() => new { Id = auditId, UserId = userId, Amount = 0.0 },
				when: () => request.ShouldAudit)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId });
		count.Should().Be(1L, "Only order should be inserted, audit skipped");
	}

	[Fact]
	public async Task ThenUpdateIfAsync_Generic_WithResultSelector_WhenTrue_NextOperationReceivesTransformedType() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 50.0 });

		// Act - Chain: Get UserDto -> ThenUpdateIfAndReturnAsync transforms to string -> ThenUpdateAsync uses string
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Orders SET Amount = @Amount WHERE UserId = @UserId",
				user => new { UserId = user.Id, Amount = 150.0 },
				orderId,
				when: _ => true,
				resultSelector: user => orderId)
			.ThenUpdateAsync(  // This receives string (orderId)
				"UPDATE Orders SET Amount = @Amount WHERE Id = @Id",
				oid => new { Id = oid, Amount = 600.0 },
				orderId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingle<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Amount.Should().Be(600.0, "Second update should have used the transformed orderId");
	}

	[Fact]
	public async Task ThenUpdateIfAsync_Generic_WithResultSelector_WhenFalse_NextOperationReceivesTransformedType() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 50.0 });

		// Act - when=false: first update skipped, resultSelector still transforms type for next operation
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateIfAndReturnAsync(
				"UPDATE Orders SET Amount = @Amount WHERE UserId = @UserId",
				user => new { UserId = user.Id, Amount = 999.0 },  // Would set to 999 but will be skipped
				orderId,
				when: _ => false,  // Skip this update
				resultSelector: user => orderId)  // Transform UserDto -> string (orderId)
			.ThenUpdateAsync(  // This should still receive string (orderId)
				"UPDATE Orders SET Amount = @Amount WHERE Id = @Id",
				oid => new { Id = oid, Amount = 700.0 },
				orderId)
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var order = rawConn.QuerySingle<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Amount.Should().Be(700.0, "Second update should have used the transformed orderId even when first update was skipped");
	}

	#endregion

	#region TransactionContext CUDIfAsync Methods

	[Fact]
	public async Task InsertIfAsync_WithWhenTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var shouldInsert = true;

		// Act - Start chain with conditional insert
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertIfAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				() => new { Id = userId, Name = "John", Email = "john@test.com" },
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().NotBeNull();
		user.Name.Should().Be("John");
	}

	[Fact]
	public async Task InsertIfAsync_WithWhenFalse_SkipsInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var shouldInsert = false;

		// Act - Start chain with conditional insert that should be skipped
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertIfAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				() => new { Id = userId, Name = "John", Email = "john@test.com" },
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue("Result should be successful even when insert is skipped");
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull("Insert should have been skipped");
	}

	[Fact]
	public async Task InsertIfAsync_WithoutParameters_WithWhenTrue_ExecutesInsert() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var shouldInsert = true;

		// Act - Start chain with conditional insert (no parameters overload)
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertIfAsync(
				$"INSERT INTO Users (Id, Name, Email) VALUES ('{userId}', 'John', 'john@test.com')",
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().NotBeNull();
	}

	[Fact]
	public async Task InsertIfAndReturnAsync_WithWhenTrue_ExecutesInsertAndReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var shouldInsert = true;

		// Act - Start chain with conditional insert that returns a value
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertIfAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				() => new { Id = userId, Name = "John", Email = "john@test.com" },
				resultSelector: () => userId,
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(userId);
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().NotBeNull();
	}

	[Fact]
	public async Task InsertIfAndReturnAsync_WithWhenFalse_SkipsInsertButReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var shouldInsert = false;

		// Act - Start chain with conditional insert that should be skipped
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertIfAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				() => new { Id = userId, Name = "John", Email = "john@test.com" },
				resultSelector: () => "skipped",
				when: () => shouldInsert)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("skipped", "Result selector should still be called when insert is skipped");
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull("Insert should have been skipped");
	}

	[Fact]
	public async Task UpdateIfAsync_WithWhenTrue_ExecutesUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		var shouldUpdate = true;

		// Act - Start chain with conditional update
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.UpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "Jane" },
				key: userId,
				when: () => shouldUpdate)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("Jane");
	}

	[Fact]
	public async Task UpdateIfAsync_WithWhenFalse_SkipsUpdate() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		var shouldUpdate = false;

		// Act - Start chain with conditional update that should be skipped
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.UpdateIfAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "Jane" },
				key: userId,
				when: () => shouldUpdate)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue("Result should be successful even when update is skipped");
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("John", "Name should not have changed");
	}

	[Fact]
	public async Task UpdateIfAndReturnAsync_WithWhenTrue_ExecutesUpdateAndReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		var shouldUpdate = true;

		// Act - Start chain with conditional update that returns a value
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.UpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "Jane" },
				key: userId,
				resultSelector: () => "updated",
				when: () => shouldUpdate)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("updated");
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("Jane");
	}

	[Fact]
	public async Task UpdateIfAndReturnAsync_WithWhenFalse_SkipsUpdateButReturnsValue() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		var shouldUpdate = false;

		// Act - Start chain with conditional update that should be skipped
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.UpdateIfAndReturnAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				() => new { Id = userId, Name = "Jane" },
				key: userId,
				resultSelector: () => "skipped",
				when: () => shouldUpdate)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("skipped", "Result selector should still be called when update is skipped");
		var user = rawConn.QuerySingle<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Name.Should().Be("John", "Name should not have changed");
	}

	[Fact]
	public async Task DeleteIfAsync_WithWhenTrue_ExecutesDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		var shouldDelete = true;

		// Act - Start chain with conditional delete
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.DeleteIfAsync(
				"DELETE FROM Users WHERE Id = @Id",
				() => new { Id = userId },
				key: userId,
				when: () => shouldDelete)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull("User should have been deleted");
	}

	[Fact]
	public async Task DeleteIfAsync_WithWhenFalse_SkipsDelete() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		var shouldDelete = false;

		// Act - Start chain with conditional delete that should be skipped
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.DeleteIfAsync(
				"DELETE FROM Users WHERE Id = @Id",
				() => new { Id = userId },
				key: userId,
				when: () => shouldDelete)
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue("Result should be successful even when delete is skipped");
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().NotBeNull("User should still exist");
	}

	[Fact]
	public async Task InsertIfAsync_CanChainWithOtherOperations() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		var shouldInsertUser = true;

		// Act - Start with conditional insert, then chain to unconditional insert
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertIfAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				() => new { Id = userId, Name = "John", Email = "john@test.com" },
				when: () => shouldInsertUser)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 100.0 })
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().NotBeNull();
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().NotBeNull();
	}

	[Fact]
	public async Task InsertIfAsync_WhenSkipped_CanStillChainWithOtherOperations() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		var shouldInsertUser = false;

		// First insert the user directly so the order insert can succeed
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Start with conditional insert (skipped), then chain to unconditional insert
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertIfAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				() => new { Id = "different-id", Name = "Jane", Email = "jane@test.com" },
				when: () => shouldInsertUser)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = orderId, UserId = userId, Amount = 100.0 })
		, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		// The conditional user should not exist
		var jane = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Name = 'Jane'");
		jane.Should().BeNull("Conditional insert should have been skipped");
		// But the order should exist
		var order = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId });
		order.Should().NotBeNull("Chain should continue after skipped insert");
	}

	#endregion

	#region InsertAndGet

	[Fact]
	public async Task InsertAndGetAsync_WhenSuccessful_ReturnsInsertedRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.InsertAndGetAsync<UserDto>(
			"""
			INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email);
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Id.Should().Be(userId);
		result.Value.Name.Should().Be("John");
		result.Value.Email.Should().Be("john@test.com");
	}

	[Fact]
	public async Task InsertAndGetAsync_WithMapper_TransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.InsertAndGetAsync<UserDto, User>(
			"""
			INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email);
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = userId, Name = "Jane", Email = "jane@test.com" },
			dto => new User(dto.Id, dto.Name, dto.Email),
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("Jane");
	}

	[Fact]
	public async Task InsertAndGetAsync_WhenUniqueConstraintViolation_ReturnsAlreadyExists() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - insert with same email (unique constraint)
		var result = await conn.InsertAndGetAsync<UserDto>(
			"""
			INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email);
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "john@test.com" },
			uniqueConstraintMessage: "User with this email already exists",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<AlreadyExistsException>();
		result.Error.Message.Should().Be("User with this email already exists");
	}

	[Fact]
	public async Task InsertAndGetOptionalAsync_WhenConditionalInsertSucceeds_ReturnsRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act - conditional insert that succeeds (no existing row)
		var result = await conn.InsertAndGetOptionalAsync<UserDto, string>(
			"""
			INSERT INTO Users (Id, Name, Email)
			SELECT @Id, @Name, @Email
			WHERE NOT EXISTS (SELECT 1 FROM Users WHERE Email = @Email);
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = userId, Name = "John", Email = "john@test.com" },
			opt => opt.HasValue ? $"Created: {opt.Value.Name}" : "Already exists",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Created: John");
	}

	[Fact]
	public async Task InsertAndGetOptionalAsync_WhenConditionalInsertSkipped_ReturnsNone() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var existingUserId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = existingUserId, Name = "John", Email = "john@test.com" });

		var newUserId = Guid.NewGuid().ToString();

		// Act - conditional insert that is skipped (row exists with same email)
		var result = await conn.InsertAndGetOptionalAsync<UserDto, string>(
			"""
			INSERT INTO Users (Id, Name, Email)
			SELECT @Id, @Name, @Email
			WHERE NOT EXISTS (SELECT 1 FROM Users WHERE Email = @Email);
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = newUserId, Name = "Jane", Email = "john@test.com" },
			opt => opt.HasValue ? $"Created: {opt.Value.Name}" : "Already exists",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Already exists");
	}

	#endregion

	#region UpdateAndGet

	[Fact]
	public async Task UpdateAndGetAsync_WhenSuccessful_ReturnsUpdatedRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.UpdateAndGetAsync<UserDto>(
			"""
			UPDATE Users SET Name = @Name WHERE Id = @Id;
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = userId, Name = "Johnny" },
			key: userId,
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Id.Should().Be(userId);
		result.Value.Name.Should().Be("Johnny");
		result.Value.Email.Should().Be("john@test.com");
	}

	[Fact]
	public async Task UpdateAndGetAsync_WhenRecordNotFound_ReturnsNotFound() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var nonExistentId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.UpdateAndGetAsync<UserDto>(
			"""
			UPDATE Users SET Name = @Name WHERE Id = @Id;
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = nonExistentId, Name = "Johnny" },
			key: nonExistentId,
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	[Fact]
	public async Task UpdateAndGetAsync_WithMapper_TransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.UpdateAndGetAsync<UserDto, UserLight>(
			"""
			UPDATE Users SET Name = @Name WHERE Id = @Id;
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = userId, Name = "Johnny" },
			key: userId,
			dto => new UserLight(dto.Id, dto.Name),
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Name.Should().Be("Johnny");
	}

	[Fact]
	public async Task UpdateAndGetAsync_WhenUniqueConstraintViolation_ReturnsAlreadyExists() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId1 = Guid.NewGuid().ToString();
		var userId2 = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId1, Name = "John", Email = "john@test.com" });
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId2, Name = "Jane", Email = "jane@test.com" });

		// Act - try to update user2's email to user1's email (unique constraint)
		var result = await conn.UpdateAndGetAsync<UserDto>(
			"""
			UPDATE Users SET Email = @Email WHERE Id = @Id;
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = userId2, Email = "john@test.com" },
			key: userId2,
			uniqueConstraintMessage: "Email already in use",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<AlreadyExistsException>();
		result.Error.Message.Should().Be("Email already in use");
	}

	[Fact]
	public async Task UpdateAndGetOptionalAsync_WhenConditionalUpdateSucceeds_ReturnsRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.UpdateAndGetOptionalAsync<UserDto, string>(
			"""
			UPDATE Users SET Name = @Name WHERE Id = @Id AND Name != @Name;
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			""",
			new { Id = userId, Name = "Johnny" },
			opt => opt.HasValue ? $"Updated: {opt.Value.Name}" : "Not updated",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Updated: Johnny");
	}

	#endregion

	#region DeleteAndGet

	[Fact]
	public async Task DeleteAndGetAsync_WhenSuccessful_ReturnsDeletedRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - SQLite doesn't support OUTPUT, so we SELECT first then DELETE
		var result = await conn.DeleteAndGetAsync<UserDto>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			DELETE FROM Users WHERE Id = @Id;
			""",
			new { Id = userId },
			key: userId,
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Id.Should().Be(userId);
		result.Value.Name.Should().Be("John");

		// Verify deleted
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		count.Should().Be(0L);
	}

	[Fact]
	public async Task DeleteAndGetAsync_WhenRecordNotFound_ReturnsNotFound() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var nonExistentId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.DeleteAndGetAsync<UserDto>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			DELETE FROM Users WHERE Id = @Id;
			""",
			new { Id = nonExistentId },
			key: nonExistentId,
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();
	}

	[Fact]
	public async Task DeleteAndGetAsync_WithMapper_TransformsResult() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.DeleteAndGetAsync<UserDto, string>(
			"""
			SELECT Id, Name, Email FROM Users WHERE Id = @Id;
			DELETE FROM Users WHERE Id = @Id;
			""",
			new { Id = userId },
			key: userId,
			dto => $"Deleted user: {dto.Name}",
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Deleted user: John");
	}

	#endregion

	#region InsertAndGet/UpdateAndGet/DeleteAndGet Chaining

	[Fact]
	public async Task FluentChaining_GetThenInsertAndGet_ReturnsInsertedRow() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<OrderDto>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAndGetAsync<OrderDto>(
				"""
				INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount);
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				user => new { Id = orderId, UserId = user.Id, Amount = 99.99 })
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Id.Should().Be(orderId);
		result.Value.UserId.Should().Be(userId);
		result.Value.Amount.Should().Be(99.99);
	}

	[Fact]
	public async Task FluentChaining_InsertAndGetThenUpdateAndGet_ChainsOperations() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act - insert an order, then update its amount
		var result = await conn.ExecuteTransactionAsync<OrderDto>(db =>
			db.InsertAndGetAsync<OrderDto>(
				"""
				INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount);
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				new { Id = orderId, UserId = userId, Amount = 50.0 })
			.ThenUpdateAndGetAsync<OrderDto>(
				"""
				UPDATE Orders SET Amount = @Amount WHERE Id = @Id;
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				order => new { Id = order.Id, Amount = order.Amount * 2 },
				order => order.Id)
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Id.Should().Be(orderId);
		result.Value.Amount.Should().Be(100.0); // 50 * 2
	}

	[Fact]
	public async Task FluentChaining_GetThenInsertAndGetThenDelete_FullChain() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();

		// Act - Get user, insert order, then delete the order
		var result = await conn.ExecuteTransactionAsync<OrderDto>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAndGetAsync<OrderDto>(
				"""
				INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount);
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				user => new { Id = orderId, UserId = user.Id, Amount = 75.0 })
			.ThenDeleteAndGetAsync<OrderDto>(
				"""
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				DELETE FROM Orders WHERE Id = @Id;
				""",
				order => new { Id = order.Id },
				order => order.Id)
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value!.Id.Should().Be(orderId);
		result.Value.Amount.Should().Be(75.0);

		// Verify deleted
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		count.Should().Be(0L);
	}

	[Fact]
	public async Task FluentChaining_WhenFirstStepFails_SubsequentStepsSkipped() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var nonExistentUserId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act - Get non-existent user, then try to insert order (should be skipped)
		var result = await conn.ExecuteTransactionAsync<OrderDto>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = nonExistentUserId },
				nonExistentUserId)
			.ThenInsertAndGetAsync<OrderDto>(
				"""
				INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount);
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				user => new { Id = orderId, UserId = user.Id, Amount = 100.0 })
			, CancellationToken.None);

		// Assert - Should fail with NotFoundException from the first step
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();

		// Verify order was NOT inserted (chain short-circuited)
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		count.Should().Be(0L);
	}

	[Fact]
	public async Task FluentChaining_WhenMiddleStepFails_TransactionRolledBack() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		var orderId = Guid.NewGuid().ToString();
		var nonExistentOrderId = Guid.NewGuid().ToString();

		// Act - Insert order (succeeds), then update non-existent order (fails)
		var result = await conn.ExecuteTransactionAsync<OrderDto>(db =>
			db.InsertAndGetAsync<OrderDto>(
				"""
				INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount);
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				new { Id = orderId, UserId = userId, Amount = 50.0 })
			.ThenUpdateAndGetAsync<OrderDto>(
				"""
				UPDATE Orders SET Amount = @Amount WHERE Id = @Id;
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				_ => new { Id = nonExistentOrderId, Amount = 999.0 },
				_ => nonExistentOrderId)
			, CancellationToken.None);

		// Assert - Should fail with NotFoundException from the second step
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotFoundException>();

		// Verify order was rolled back (transaction failed)
		var count = rawConn.ExecuteScalar<long>("SELECT COUNT(*) FROM Orders WHERE Id = @Id", new { Id = orderId });
		count.Should().Be(0L);
	}

	[Fact]
	public async Task FluentChaining_ThenInsertAndGetOptional_HandlesEmptyCase() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Insert an existing order
		var existingOrderId = Guid.NewGuid().ToString();
		rawConn.Execute("INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = existingOrderId, UserId = userId, Amount = 100.0 });

		var newOrderId = Guid.NewGuid().ToString();

		// Act - conditional insert that is skipped
		var result = await conn.ExecuteTransactionAsync<string>(db =>
			db.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertAndGetOptionalAsync<OrderDto, string>(
				"""
				INSERT INTO Orders (Id, UserId, Amount)
				SELECT @Id, @UserId, @Amount
				WHERE NOT EXISTS (SELECT 1 FROM Orders WHERE UserId = @UserId);
				SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id;
				""",
				user => new { Id = newOrderId, UserId = user.Id, Amount = 200.0 },
				opt => opt.HasValue ? $"Created order: {opt.Value.Id}" : "User already has an order")
			, CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("User already has an order");
	}

	#endregion

	#region Test DTOs

	private record UserDto(string Id, string Name, string Email);
	private record User(string Id, string Name, string Email);
	private record UserLight(string Id, string Name);
	private record OrderDto(string Id, string UserId, double Amount);

	#endregion

}
