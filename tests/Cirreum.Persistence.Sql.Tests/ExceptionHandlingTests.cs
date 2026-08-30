namespace Cirreum.Persistence.Sql.Tests;

using Cirreum;
using Cirreum.Exceptions;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;
using System.Data;

/// <summary>
/// Tests that verify exception handling behavior in database operations.
/// Specifically tests that exceptions in ExecuteTransactionAsync are captured
/// and properly rolled back, converting exceptions to Result failures.
/// </summary>
/// <remarks>
/// The library uses database-agnostic exception handling via DbExceptionExtensions.
/// Constraint violations are translated to appropriate exception types (AlreadyExistsException,
/// BadRequestException, ConflictException) regardless of the underlying database provider.
/// </remarks>
public sealed class ExceptionHandlingTests {

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

	#region Transaction Exception Handling - General Exceptions

	[Fact]
	public async Task ExecuteTransactionAsync_WhenActionThrowsException_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync(async ctx => {
			await ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" });

			throw new ApplicationException("Something went wrong mid-transaction!");
#pragma warning disable CS0162 // Unreachable code detected
			return Result.Success;
#pragma warning restore CS0162
		}, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<ApplicationException>();
		result.Error.Message.Should().Be("Something went wrong mid-transaction!");

		// Verify rollback - user should not exist
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull();
	}

	[Fact]
	public async Task ExecuteTransactionAsync_Generic_WhenActionThrowsException_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<string>(async ctx => {
			await ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" });

			throw new InvalidOperationException("Cannot complete transaction!");
#pragma warning disable CS0162 // Unreachable code detected
			return userId;
#pragma warning restore CS0162
		}, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidOperationException>();
		result.Error.Message.Should().Be("Cannot complete transaction!");

		// Verify rollback - user should not exist
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull();
	}

	[Fact]
	public async Task ExecuteTransactionAsync_WhenMapperThrowsInChain_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act - Insert then map throws
		var result = await conn.ExecuteTransactionAsync<User>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => new UserDto(userId, "John", "john@test.com"))
			.MapAsync(_ => {
				static User ThrowingMapper() => throw new FormatException("Bad format!");
				return ThrowingMapper();
			})
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<FormatException>();
		result.Error.Message.Should().Be("Bad format!");

		// Verify rollback - user should not exist
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull();
	}

	[Fact]
	public async Task ExecuteTransactionAsync_WhenWherePredicateThrows_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act - Insert then ensure predicate throws
		var result = await conn.ExecuteTransactionAsync<UserDto>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => new UserDto(userId, "John", "john@test.com"))
			.EnsureAsync(
				_ => {
					static bool ThrowingPredicate() => throw new ArithmeticException("Cannot evaluate predicate!");
					return ThrowingPredicate();
				},
				new BadRequestException("Validation failed"))
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<ArithmeticException>();
		result.Error.Message.Should().Be("Cannot evaluate predicate!");

		// Verify rollback - user should not exist
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull();
	}

	[Fact]
	public async Task ExecuteTransactionAsync_WhenParametersFactoryThrows_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();

		// Act - Insert user, then parameters factory for order throws
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				_ => throw new ArgumentNullException("userId", "Parameters factory failed!"))
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<ArgumentNullException>();

		// Verify rollback - user should not exist
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull();
	}

	[Fact]
	public async Task ExecuteTransactionAsync_WhenResultSelectorThrows_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act - Insert with result selector that throws
		var result = await conn.ExecuteTransactionAsync<string>(ctx =>
			ctx.InsertAndReturnAsync<string>(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => throw new InvalidCastException("Result selector failed!"))
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<InvalidCastException>();
		result.Error.Message.Should().Be("Result selector failed!");

		// Verify rollback - user should not exist
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull();
	}

	#endregion

	#region Transaction SQL Error Handling

	[Fact]
	public async Task ExecuteTransactionAsync_WhenSqlSyntaxError_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();

		// Act - First insert succeeds, second has syntax error
		var result = await conn.ExecuteTransactionAsync(async ctx => {
			await ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" });

			// Syntax error in second insert
			return await ctx.InsertAsync(
				"INSRT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)", // INSRT instead of INSERT
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 });
		}, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<SqliteException>();

		// Verify rollback - first user insert should be rolled back too
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull();
	}

	[Fact]
	public async Task ExecuteTransactionAsync_WhenConstraintViolation_RollsBackAndReturnsFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId1 = Guid.NewGuid().ToString();
		var userId2 = Guid.NewGuid().ToString();

		rawConn.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId1, Name = "Existing", Email = "existing@test.com" });

		// Act - Insert user, then try to update to conflict with existing email
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId2, Name = "New", Email = "new@test.com" })
			.ThenUpdateAsync(
				"UPDATE Users SET Email = @Email WHERE Id = @Id",
				new { Id = userId2, Email = "existing@test.com" }, // Conflicts with existing user
				userId2)
		, CancellationToken.None);

		// Assert - Constraint violations are translated to AlreadyExistsException regardless of database provider
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<AlreadyExistsException>();

		// Verify rollback - new user should not exist
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId2 });
		user.Should().BeNull();
	}

	#endregion

	#region Chained Operations Exception Propagation

	[Fact]
	public async Task FluentChain_WhenMiddleOperationThrows_PropagatesFailureAndStopsChain() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var finalOperationExecuted = false;

		// Act
		var result = await conn.ExecuteTransactionAsync<long>(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => new UserDto(userId, "John", "john@test.com"))
			.MapAsync(_ => {
				static User ThrowingMapper() => throw new NotImplementedException("Middle mapper failed!");
				return ThrowingMapper();
			})
			.ThenGetScalarAsync<long>("SELECT COUNT(*) FROM Users")
			.MapAsync(count => {
				finalOperationExecuted = true;
				return count;
			})
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<NotImplementedException>();
		result.Error.Message.Should().Be("Middle mapper failed!");
		finalOperationExecuted.Should().BeFalse("Operations after failure should not execute");
	}

	[Fact]
	public async Task FluentChain_ExceptionInFirstOperation_PropagatesAndRollsBack() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var orderId = Guid.NewGuid().ToString();
		var orderInsertExecuted = false;

		// Act - First operation's result selector throws, second should not run
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAndReturnAsync<UserDto>(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john@test.com" },
				() => throw new AccessViolationException("First operation failed!"))
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => {
					orderInsertExecuted = true;
					return new { Id = orderId, UserId = user.Id, Amount = 100.0 };
				})
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeOfType<AccessViolationException>();
		orderInsertExecuted.Should().BeFalse("Second operation should not execute after first fails");
	}

	[Fact]
	public async Task FluentChain_MultipleInsertsWithExceptionInMiddle_RollsBackAll() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);
		var userId = Guid.NewGuid().ToString();
		var orderId1 = Guid.NewGuid().ToString();
		var orderId2 = Guid.NewGuid().ToString();

		// Act - Insert user, insert order 1, throw, insert order 2 (should never run)
		var result = await conn.ExecuteTransactionAsync(ctx =>
			ctx.InsertAndReturnAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" },
				() => userId)
			.ThenInsertAndReturnAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				_ => new { Id = orderId1, UserId = userId, Amount = 100.0 },
				_ => orderId1)
			.MapAsync(_ => {
				static string Throw() => throw new Exception("Boom!");
				return Throw();
			})
			.ThenInsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				_ => new { Id = orderId2, UserId = userId, Amount = 200.0 })
			.ToResult()
		, CancellationToken.None);

		// Assert
		result.IsFailure.Should().BeTrue();

		// Verify ALL operations were rolled back
		var user = rawConn.QuerySingleOrDefault<UserDto>(
			"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
			new { Id = userId });
		user.Should().BeNull("User should be rolled back");

		var order1 = rawConn.QuerySingleOrDefault<OrderDto>(
			"SELECT Id, UserId, Amount FROM Orders WHERE Id = @Id",
			new { Id = orderId1 });
		order1.Should().BeNull("Order 1 should be rolled back");
	}

	#endregion

	#region Test DTOs

	private record UserDto(string Id, string Name, string Email);
	private record User(string Id, string Name, string Email);
	private record OrderDto(string Id, string UserId, double Amount);

	#endregion

}
