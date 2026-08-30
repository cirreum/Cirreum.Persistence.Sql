namespace Cirreum.Persistence.Sql.Tests;

using Cirreum;
using Cirreum.Persistence;
using Dapper;
using Microsoft.Data.Sqlite;

public sealed class CommandWithCountAsyncTests {

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

	#region InsertWithCountAsync

	[Fact]
	public async Task InsertWithCountAsync_ReturnsRowCount_WhenInsertSucceeds() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.InsertWithCountAsync(
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
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Insert a user first
		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Try to insert only if email doesn't exist (it does, so 0 rows)
		var result = await conn.InsertWithCountAsync(
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

	[Fact]
	public async Task InsertWithCountAsync_ReturnsOne_WhenConditionalInsertSucceeds() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act - Conditional insert when email doesn't exist
		var result = await conn.InsertWithCountAsync(
			"""
			INSERT INTO Users (Id, Name, Email)
			SELECT @Id, @Name, @Email
			WHERE NOT EXISTS (SELECT 1 FROM Users WHERE Email = @Email)
			""",
			new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "jane@test.com" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	#endregion

	#region UpdateWithCountAsync

	[Fact]
	public async Task UpdateWithCountAsync_ReturnsRowCount_WhenUpdateSucceeds() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.UpdateWithCountAsync(
			"UPDATE Users SET Name = @Name WHERE Id = @Id",
			new { Id = userId, Name = "Johnny" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task UpdateWithCountAsync_ReturnsZero_WhenNoRowsMatch() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act - Update non-existent user
		var result = await conn.UpdateWithCountAsync(
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
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john1@test.com" });
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john2@test.com" });

		// Act - Update all users named John
		var result = await conn.UpdateWithCountAsync(
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
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Id = @Id",
			new { Id = userId },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task DeleteWithCountAsync_ReturnsZero_WhenNoRowsMatch() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		// Act - Delete non-existent user
		var result = await conn.DeleteWithCountAsync(
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
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john1@test.com" });
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = Guid.NewGuid().ToString(), Name = "John", Email = "john2@test.com" });

		// Act - Delete all users named John
		var result = await conn.DeleteWithCountAsync(
			"DELETE FROM Users WHERE Name = @Name",
			new { Name = "John" },
			cancellationToken: CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(2);
	}

	#endregion

	#region DbContext Integration

	[Fact]
	public async Task DbContext_InsertWithCountAsync_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.InsertWithCountAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task DbContext_UpdateWithCountAsync_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.UpdateWithCountAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "Johnny" }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task DbContext_DeleteWithCountAsync_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.DeleteWithCountAsync(
				"DELETE FROM Users WHERE Id = @Id",
				new { Id = userId }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	#endregion

	#region Chaining (ThenXxxWithCountAsync)

	[Fact]
	public async Task ThenInsertWithCountAsync_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Get user, then insert an order
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenInsertWithCountAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				user => new { Id = Guid.NewGuid().ToString(), UserId = user.Id, Amount = 100.0 }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);

		// Verify order was inserted
		var orderCount = rawConn.ExecuteScalar<int>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId });
		orderCount.Should().Be(1);
	}

	[Fact]
	public async Task ThenUpdateWithCountAsync_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Get user, then update their name
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateWithCountAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				user => new { user.Id, Name = "Johnny" }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);

		// Verify update
		var updatedName = rawConn.ExecuteScalar<string>("SELECT Name FROM Users WHERE Id = @Id", new { Id = userId });
		updatedName.Should().Be("Johnny");
	}

	[Fact]
	public async Task ThenDeleteWithCountAsync_ChainsCorrectly() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 100.0 });

		// Act - Get user, then delete their orders
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenDeleteWithCountAsync(
				"DELETE FROM Orders WHERE UserId = @UserId",
				user => new { UserId = user.Id }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);

		// Verify deletion
		var orderCount = rawConn.ExecuteScalar<int>("SELECT COUNT(*) FROM Orders WHERE UserId = @UserId", new { UserId = userId });
		orderCount.Should().Be(0);
	}

	[Fact]
	public async Task ThenUpdateWithCountAsync_ReturnsZero_WhenNoRowsMatch() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Get user, then try to update a non-existent user
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.GetAsync<UserDto>(
				"SELECT Id, Name, Email FROM Users WHERE Id = @Id",
				new { Id = userId },
				userId)
			.ThenUpdateWithCountAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @NonExistentId",
				new { NonExistentId = Guid.NewGuid().ToString(), Name = "Johnny" }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(0);
	}

	#endregion

	#region Transaction Integration

	[Fact]
	public async Task InsertWithCountAsync_WorksInTransaction() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act
		var result = await conn.ExecuteTransactionAsync<int>(ctx =>
			ctx.InsertWithCountAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);

		// Verify committed
		var count = rawConn.ExecuteScalar<int>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		count.Should().Be(1);
	}

	[Fact]
	public async Task ChainedWithCountAsync_RollsBackOnFailure() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act - Insert user, then try to insert with duplicate email (should fail and rollback)
		var result = await conn.ExecuteTransactionAsync<int>(ctx =>
			ctx.InsertWithCountAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertWithCountAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = Guid.NewGuid().ToString(), Name = "Jane", Email = "john@test.com" }), CancellationToken.None); // Same email!

		// Assert
		result.IsFailure.Should().BeTrue();

		// Verify rollback - first user should NOT exist
		var count = rawConn.ExecuteScalar<int>("SELECT COUNT(*) FROM Users WHERE Id = @Id", new { Id = userId });
		count.Should().Be(0);
	}

	#endregion

	#region Non-generic DbResult chaining

	[Fact]
	public async Task NonGenericDbResult_ThenInsertWithCountAsync_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();

		// Act - Insert user (non-generic), then insert order with count
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
				new { Id = userId, Name = "John", Email = "john@test.com" })
			.ThenInsertWithCountAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task NonGenericDbResult_ThenUpdateWithCountAsync_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });

		// Act - Insert order (non-generic), then update user with count
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.InsertAsync(
				"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
				new { Id = Guid.NewGuid().ToString(), UserId = userId, Amount = 100.0 })
			.ThenUpdateWithCountAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "Johnny" }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	[Fact]
	public async Task NonGenericDbResult_ThenDeleteWithCountAsync_Works() {
		// Arrange
		var (conn, rawConn) = CreateConnection();
		await using var _ = conn;
		CreateTestSchema(rawConn);

		var userId = Guid.NewGuid().ToString();
		var orderId = Guid.NewGuid().ToString();
		rawConn.Execute(
			"INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)",
			new { Id = userId, Name = "John", Email = "john@test.com" });
		rawConn.Execute(
			"INSERT INTO Orders (Id, UserId, Amount) VALUES (@Id, @UserId, @Amount)",
			new { Id = orderId, UserId = userId, Amount = 100.0 });

		// Act - Update user (non-generic), then delete orders with count
		var result = await conn.ExecuteAsync<int>(ctx =>
			ctx.UpdateAsync(
				"UPDATE Users SET Name = @Name WHERE Id = @Id",
				new { Id = userId, Name = "Johnny" },
				userId)
			.ThenDeleteWithCountAsync(
				"DELETE FROM Orders WHERE UserId = @UserId",
				new { UserId = userId }), CancellationToken.None);

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(1);
	}

	#endregion

}
