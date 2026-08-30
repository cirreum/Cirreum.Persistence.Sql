namespace Cirreum.Persistence.Sql.Tests;

using Cirreum;
using Cirreum.Exceptions;

public sealed class DbResultTests {

	#region Error Propagation

	[Fact]
	public async Task DbResult_WhenFailure_PropagatesErrorThroughMapAsync() {
		// Arrange
		var error = new NotFoundException("Not found");
		var failedResult = Task.FromResult(Result.Fail<int>(error));
		var dbResult = new DbResult<int>(default, failedResult);

		// Act
		var mapped = dbResult.MapAsync(x => x * 2);
		var result = await mapped;

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(error);
	}

	[Fact]
	public async Task DbResult_WhenFailure_PropagatesErrorThroughEnsureAsync() {
		// Arrange
		var error = new BadRequestException("Bad request");
		var failedResult = Task.FromResult(Result.Fail<string>(error));
		var dbResult = new DbResult<string>(default, failedResult);

		// Act
		var filtered = dbResult.EnsureAsync(x => x.Length > 5, new NotFoundException("Should not see this"));
		var result = await filtered;

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(error);
	}

	[Fact]
	public async Task DbResult_WhenSuccess_MapAsyncTransformsValue() {
		// Arrange
		var successResult = Task.FromResult(Result.From(42));
		var dbResult = new DbResult<int>(default, successResult);

		// Act
		var mapped = dbResult.MapAsync(x => x * 2);
		var result = await mapped;

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(84);
	}

	[Fact]
	public async Task DbResult_WhenSuccess_AsyncMapAsyncTransformsValue() {
		// Arrange
		var successResult = Task.FromResult(Result.From(42));
		var dbResult = new DbResult<int>(default, successResult);

		// Act
		var mapped = dbResult.MapAsync((Func<int, Task<int>>)(async x => {
			await Task.Delay(1, CancellationToken.None);
			return x * 2;
		}));
		var result = await mapped;

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(84);
	}

	#endregion

	#region EnsureAsync

	[Fact]
	public async Task DbResult_EnsureAsync_WhenPredicateTrue_ReturnsSuccess() {
		// Arrange
		var successResult = Task.FromResult(Result.From("Hello"));
		var dbResult = new DbResult<string>(default, successResult);

		// Act
		var filtered = dbResult.EnsureAsync(x => x.Length == 5, new BadRequestException("Wrong length"));
		var result = await filtered;

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Hello");
	}

	[Fact]
	public async Task DbResult_EnsureAsync_WhenPredicateFalse_ReturnsProvidedError() {
		// Arrange
		var successResult = Task.FromResult(Result.From("Hi"));
		var dbResult = new DbResult<string>(default, successResult);
		var expectedError = new BadRequestException("Wrong length");

		// Act
		var filtered = dbResult.EnsureAsync(x => x.Length == 5, expectedError);
		var result = await filtered;

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(expectedError);
	}

	[Fact]
	public async Task DbResult_EnsureAsync_WithAsyncPredicate_WhenTrue_ReturnsSuccess() {
		// Arrange
		var successResult = Task.FromResult(Result.From(10));
		var dbResult = new DbResult<int>(default, successResult);

		// Act
		var filtered = dbResult.EnsureAsync(
			async x => {
				await Task.Delay(1, CancellationToken.None);
				return x > 5;
			},
			new BadRequestException("Too small"));
		var result = await filtered;

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(10);
	}

	[Fact]
	public async Task DbResult_EnsureAsync_WithAsyncPredicate_WhenFalse_ReturnsError() {
		// Arrange
		var successResult = Task.FromResult(Result.From(3));
		var dbResult = new DbResult<int>(default, successResult);
		var expectedError = new BadRequestException("Too small");

		// Act
		var filtered = dbResult.EnsureAsync(
			async x => {
				await Task.Delay(1, CancellationToken.None);
				return x > 5;
			},
			expectedError);
		var result = await filtered;

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(expectedError);
	}

	#endregion

	#region Implicit Conversion

	[Fact]
	public async Task DbResult_ImplicitlyConvertsToTaskOfResult() {
		// Arrange
		var successResult = Task.FromResult(Result.From(42));
		var dbResult = new DbResult<int>(default, successResult);

		// Act
		Task<Result<int>> task = dbResult; // implicit conversion
		var result = await task;

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(42);
	}

	[Fact]
	public async Task DbResult_CanBeAwaitedDirectly() {
		// Arrange
		var successResult = Task.FromResult(Result.From("test"));
		var dbResult = new DbResult<string>(default, successResult);

		// Act
		var result = await dbResult; // uses GetAwaiter

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("test");
	}

	#endregion

	#region Chaining Multiple Operations

	[Fact]
	public async Task DbResult_ChainingMapAndEnsure_Success() {
		// Arrange
		var successResult = Task.FromResult(Result.From(10));
		var dbResult = new DbResult<int>(default, successResult);

		// Act
		var result = await dbResult
			.MapAsync(x => x * 2)
			.EnsureAsync(x => x > 15, new BadRequestException("Too small"))
			.MapAsync(x => $"Value: {x}");

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Value: 20");
	}

	[Fact]
	public async Task DbResult_ChainingMapAndEnsure_FailsAtEnsure() {
		// Arrange
		var successResult = Task.FromResult(Result.From(5));
		var dbResult = new DbResult<int>(default, successResult);
		var expectedError = new BadRequestException("Too small");

		// Act
		var result = await dbResult
			.MapAsync(x => x * 2)  // 5 * 2 = 10
			.EnsureAsync(x => x > 15, expectedError) // 10 > 15 is false
			.MapAsync(x => $"Value: {x}"); // should not execute

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(expectedError);
	}

	[Fact]
	public async Task DbResult_ChainingWithInitialFailure_PropagatesError() {
		// Arrange
		var initialError = new NotFoundException("Initial failure");
		var failedResult = Task.FromResult(Result.Fail<int>(initialError));
		var dbResult = new DbResult<int>(default, failedResult);
		var mapperCalled = false;
		var predicateCalled = false;

		// Act
		var result = await dbResult
			.MapAsync(x => {
				mapperCalled = true;
				return x * 2;
			})
			.EnsureAsync(x => {
				predicateCalled = true;
				return x > 15;
			}, new BadRequestException("Too small"))
			.MapAsync(x => $"Value: {x}");

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(initialError);
		mapperCalled.Should().BeFalse("Mapper should not be called when initial result is failure");
		predicateCalled.Should().BeFalse("Predicate should not be called when initial result is failure");
	}

	#endregion

	#region ThenAsync (to non-generic Result)

	[Fact]
	public async Task DbResult_ThenAsync_WhenSuccess_ExecutesNext() {
		// Arrange
		var successResult = Task.FromResult(Result.From(42));
		var dbResult = new DbResult<int>(default, successResult);
		var nextExecuted = false;

		// Act
		var result = await dbResult.ThenAsync(async x => {
			await Task.Delay(1, CancellationToken.None);
			nextExecuted = true;
			x.Should().Be(42);
			return Result.Success;
		});

		// Assert
		result.IsSuccess.Should().BeTrue();
		nextExecuted.Should().BeTrue();
	}

	[Fact]
	public async Task DbResult_ThenAsync_WhenFailure_DoesNotExecuteNext() {
		// Arrange
		var error = new NotFoundException("Not found");
		var failedResult = Task.FromResult(Result.Fail<int>(error));
		var dbResult = new DbResult<int>(default, failedResult);
		var nextExecuted = false;

		// Act
		var result = await dbResult.ThenAsync(async _ => {
			await Task.Delay(1, CancellationToken.None);
			nextExecuted = true;
			return Result.Success;
		});

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(error);
		nextExecuted.Should().BeFalse();
	}

	#endregion

	#region ThenAsync (to generic Result<TResult>)

	[Fact]
	public async Task DbResult_ThenAsyncGeneric_WhenSuccess_ExecutesNextAndReturnsValue() {
		// Arrange
		var successResult = Task.FromResult(Result.From(42));
		var dbResult = new DbResult<int>(default, successResult);

		// Act
		var result = await dbResult.ThenAsync(async x => {
			await Task.Delay(1, CancellationToken.None);
			return Result.From($"Value was {x}");
		});

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be("Value was 42");
	}

	[Fact]
	public async Task DbResult_ThenAsyncGeneric_WhenFailure_DoesNotExecuteNext() {
		// Arrange
		var error = new NotFoundException("Not found");
		var failedResult = Task.FromResult(Result.Fail<int>(error));
		var dbResult = new DbResult<int>(default, failedResult);
		var nextExecuted = false;

		// Act
		var result = await dbResult.ThenAsync(async _ => {
			await Task.Delay(1, CancellationToken.None);
			nextExecuted = true;
			return Result.From("Should not reach");
		});

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(error);
		nextExecuted.Should().BeFalse();
	}

	[Fact]
	public async Task DbResult_ThenAsyncGeneric_WhenNextFails_PropagatesError() {
		// Arrange
		var successResult = Task.FromResult(Result.From(42));
		var dbResult = new DbResult<int>(default, successResult);
		var nextError = new BadRequestException("Next failed");

		// Act
		var result = await dbResult.ThenAsync(async _ => {
			await Task.Delay(1, CancellationToken.None);
			return Result.Fail<string>(nextError);
		});

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(nextError);
	}

	#endregion

}

public sealed class DbResultNonGenericTests {

	#region Implicit Conversion

	[Fact]
	public async Task DbResultNonGeneric_ImplicitlyConvertsToTaskOfResult() {
		// Arrange
		var successResult = Task.FromResult(Result.Success);
		var dbResult = new DbResult(default, successResult);

		// Act
		Task<Result> task = dbResult; // implicit conversion
		var result = await task;

		// Assert
		result.IsSuccess.Should().BeTrue();
	}

	[Fact]
	public async Task DbResultNonGeneric_CanBeAwaitedDirectly() {
		// Arrange
		var successResult = Task.FromResult(Result.Success);
		var dbResult = new DbResult(default, successResult);

		// Act
		var result = await dbResult; // uses GetAwaiter

		// Assert
		result.IsSuccess.Should().BeTrue();
	}

	[Fact]
	public async Task DbResultNonGeneric_WithFailure_PropagatesError() {
		// Arrange
		var error = new BadRequestException("Something went wrong");
		var failedResult = Task.FromResult(Result.Fail(error));
		var dbResult = new DbResult(default, failedResult);

		// Act
		var result = await dbResult;

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(error);
	}

	#endregion

	#region ThenAsync (to non-generic Result)

	[Fact]
	public async Task DbResultNonGeneric_ThenAsync_WhenSuccess_ExecutesNext() {
		// Arrange
		var successResult = Task.FromResult(Result.Success);
		var dbResult = new DbResult(default, successResult);
		var nextExecuted = false;

		// Act
		var result = await dbResult.ThenAsync(async () => {
			await Task.Delay(1, CancellationToken.None);
			nextExecuted = true;
			return Result.Success;
		});

		// Assert
		result.IsSuccess.Should().BeTrue();
		nextExecuted.Should().BeTrue();
	}

	[Fact]
	public async Task DbResultNonGeneric_ThenAsync_WhenFailure_DoesNotExecuteNext() {
		// Arrange
		var error = new NotFoundException("Not found");
		var failedResult = Task.FromResult(Result.Fail(error));
		var dbResult = new DbResult(default, failedResult);
		var nextExecuted = false;

		// Act
		var result = await dbResult.ThenAsync(async () => {
			await Task.Delay(1, CancellationToken.None);
			nextExecuted = true;
			return Result.Success;
		});

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(error);
		nextExecuted.Should().BeFalse();
	}

	[Fact]
	public async Task DbResultNonGeneric_ThenAsync_WhenNextFails_PropagatesError() {
		// Arrange
		var successResult = Task.FromResult(Result.Success);
		var dbResult = new DbResult(default, successResult);
		var nextError = new BadRequestException("Next failed");

		// Act
		var result = await dbResult.ThenAsync(async () => {
			await Task.Delay(1, CancellationToken.None);
			return Result.Fail(nextError);
		});

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(nextError);
	}

	#endregion

	#region ThenAsync (to generic Result<T>)

	[Fact]
	public async Task DbResultNonGeneric_ThenAsyncGeneric_WhenSuccess_ExecutesNextAndReturnsValue() {
		// Arrange
		var successResult = Task.FromResult(Result.Success);
		var dbResult = new DbResult(default, successResult);

		// Act
		var result = await dbResult.ThenAsync(async () => {
			await Task.Delay(1, CancellationToken.None);
			return Result.From(42);
		});

		// Assert
		result.IsSuccess.Should().BeTrue();
		result.Value.Should().Be(42);
	}

	[Fact]
	public async Task DbResultNonGeneric_ThenAsyncGeneric_WhenFailure_DoesNotExecuteNext() {
		// Arrange
		var error = new NotFoundException("Not found");
		var failedResult = Task.FromResult(Result.Fail(error));
		var dbResult = new DbResult(default, failedResult);
		var nextExecuted = false;

		// Act
		var result = await dbResult.ThenAsync(async () => {
			await Task.Delay(1, CancellationToken.None);
			nextExecuted = true;
			return Result.From(42);
		});

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(error);
		nextExecuted.Should().BeFalse();
	}

	[Fact]
	public async Task DbResultNonGeneric_ThenAsyncGeneric_WhenNextFails_PropagatesError() {
		// Arrange
		var successResult = Task.FromResult(Result.Success);
		var dbResult = new DbResult(default, successResult);
		var nextError = new BadRequestException("Next failed");

		// Act
		var result = await dbResult.ThenAsync(async () => {
			await Task.Delay(1, CancellationToken.None);
			return Result.Fail<int>(nextError);
		});

		// Assert
		result.IsFailure.Should().BeTrue();
		result.Error.Should().BeSameAs(nextError);
	}

	#endregion

}
