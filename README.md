# Cirreum.Persistence.Sql

[![NuGet Version](https://img.shields.io/nuget/v/Cirreum.Persistence.Sql.svg?style=flat-square&labelColor=1F1F1F&color=003D8F)](https://www.nuget.org/packages/Cirreum.Persistence.Sql/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Cirreum.Persistence.Sql.svg?style=flat-square&labelColor=1F1F1F&color=003D8F)](https://www.nuget.org/packages/Cirreum.Persistence.Sql/)
[![GitHub Release](https://img.shields.io/github/v/release/cirreum/Cirreum.Persistence.Sql?style=flat-square&labelColor=1F1F1F&color=FF3B2E)](https://github.com/cirreum/Cirreum.Persistence.Sql/releases)
[![License](https://img.shields.io/github/license/cirreum/Cirreum.Persistence.Sql?style=flat-square&labelColor=1F1F1F&color=F2F2F2)](https://github.com/cirreum/Cirreum.Persistence.Sql/blob/main/LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-003D8F?style=flat-square&labelColor=1F1F1F)](https://dotnet.microsoft.com/)

**Database-agnostic SQL persistence layer for .NET applications**

## Overview

**Cirreum.Persistence.Sql** provides a streamlined, database-agnostic SQL persistence layer built on ADO.NET. Designed to integrate seamlessly with the Cirreum Foundation Framework, it works with any ADO.NET provider including SQL Server, SQLite, PostgreSQL, and MySQL.

The library offers Result-oriented extension methods for common data access patterns, fluent transaction chaining, pagination (offset, cursor, and slice), and automatic SQL constraint violation handling that works transparently across database providers.

## Key Features

- **Database Agnostic** - Works with SQL Server, SQLite, PostgreSQL, MySQL, and any ADO.NET provider
- **Connection Factory Pattern** - Clean `ISqlConnectionFactory` abstraction for database connections
- **Result Integration** - Extension methods that return `Result<T>` for railway-oriented programming
- **Pagination Support** - Built-in support for offset (`PagedResult<T>`), cursor (`CursorResult<T>`), and slice (`SliceResult<T>`) pagination
- **Constraint Handling** - Automatic detection and conversion of constraint violations to typed Result failures across all supported databases
- **Fluent Transactions** - Chain multiple database operations in a single transaction with railway-oriented error handling
- **Multiple Result Sets** - Support for queries returning multiple result sets via `IMultipleResult`
- **Data Mapping** - Built-in mapper overloads for transforming data types during queries

## Core Interfaces

### ISqlConnectionFactory

The factory interface for creating database connections:

```csharp
public interface ISqlConnectionFactory
{
    int CommandTimeoutSeconds { get; }
    Task<ISqlConnection> CreateConnectionAsync(CancellationToken cancellationToken = default);
}
```

### ISqlConnection

The abstraction that wraps ADO.NET connections:

```csharp
public interface ISqlConnection : IAsyncDisposable
{
    Task<T?> QuerySingleOrDefaultAsync<T>(string sql, object? parameters,
        IDbTransaction? transaction, CancellationToken cancellationToken);
    Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object? parameters,
        IDbTransaction? transaction, CancellationToken cancellationToken);
    Task<IEnumerable<T>> QueryAsync<T>(string sql, object? parameters,
        IDbTransaction? transaction, CancellationToken cancellationToken);
    Task<T?> ExecuteScalarAsync<T>(string sql, object? parameters,
        IDbTransaction? transaction, CancellationToken cancellationToken);
    Task<int> ExecuteAsync(string sql, object? parameters,
        IDbTransaction? transaction, CancellationToken cancellationToken);
    Task<IMultipleResult> QueryMultipleAsync(string sql, object? parameters,
        IDbTransaction? transaction, CancellationToken cancellationToken);
    IDbTransaction BeginTransaction();
}
```

### IMultipleResult

Interface for reading multiple result sets from a single query:

```csharp
public interface IMultipleResult
{
    bool IsConsumed { get; }
    Task<T?> ReadSingleOrDefaultAsync<T>();
    Task<T?> ReadFirstOrDefaultAsync<T>();
    Task<IEnumerable<T>> ReadAsync<T>(bool buffered = true);
}
```

## Database Support

Constraint violation detection works automatically via reflection for:

| Database | Provider Package | Unique Constraint | FK Violation |
|----------|-----------------|-------------------|--------------|
| SQL Server | `Microsoft.Data.SqlClient` | Error 2627, 2601 | Error 547 |
| SQLite | `Microsoft.Data.Sqlite` | Extended 1555, 2067 | Extended 787 |
| PostgreSQL | `Npgsql` | State 23505 | State 23503 |
| MySQL | `MySql.Data` / `MySqlConnector` | Error 1062 | Error 1452 |

## Quick Start

```csharp
// Use a provider-specific package (recommended) or implement ISqlConnectionFactory
// Provider packages handle connection creation, pooling, and authentication

// Register in DI (example using SQL Server provider package)
services.AddSingleton<ISqlConnectionFactory>(
    new SqlServerConnectionFactory(connectionString));

// Or for SQLite
services.AddSingleton<ISqlConnectionFactory>(
    new SqliteConnectionFactory("Data Source=mydb.db"));
```

## Query Extensions

All query extensions return `Result<T>` and integrate with the Cirreum Result monad. Extensions are available on both `ISqlConnection` (for manual connection management) and `ISqlConnectionFactory` (for automatic connection handling).

### Single Record Queries
```csharp
public async Task<Result<Order>> GetOrderAsync(Guid orderId, CancellationToken ct)
{
    // Using factory extension (recommended - handles connection lifecycle)
    return await db.GetAsync<Order>(
        "SELECT * FROM Orders WHERE OrderId = @OrderId",
        new { OrderId = orderId },
        key: orderId,  // Used for NotFoundException if not found
        ct);
}

// With mapper for data transformation
public async Task<Result<OrderSummary>> GetOrderSummaryAsync(Guid orderId, CancellationToken ct)
{
    return await db.GetAsync<OrderDto, OrderSummary>(
        "SELECT * FROM Orders WHERE OrderId = @OrderId",
        new { OrderId = orderId },
        key: orderId,
        dto => new OrderSummary(dto.Id, dto.Total),
        ct);
}
```

### Collection Queries
```csharp
public async Task<Result<IReadOnlyList<Order>>> GetOrdersAsync(Guid customerId, CancellationToken ct)
{
    return await db.QueryAnyAsync<Order>(
        "SELECT * FROM Orders WHERE CustomerId = @CustomerId",
        new { CustomerId = customerId },
        ct);
}

// With mapper
public async Task<Result<IReadOnlyList<OrderSummary>>> GetOrderSummariesAsync(
    Guid customerId, CancellationToken ct)
{
    return await db.QueryAnyAsync<OrderDto, OrderSummary>(
        "SELECT * FROM Orders WHERE CustomerId = @CustomerId",
        new { CustomerId = customerId },
        dto => new OrderSummary(dto.Id, dto.Total),
        ct);
}
```

### Scalar Queries
```csharp
public async Task<Result<int>> GetOrderCountAsync(Guid customerId, CancellationToken ct)
{
    return await db.GetScalarAsync<int>(
        "SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId",
        new { CustomerId = customerId },
        ct);
}
```

### Optional Queries

Use optional queries when a record may or may not exist and you want to handle absence without exceptions.

**`GetOptionalAsync`** - Strict single-row query (fails if multiple rows):

```csharp
public async Task<Result<Optional<UserPrefs>>> GetUserPrefsAsync(Guid userId, CancellationToken ct)
{
    return await db.GetOptionalAsync<UserPrefs>(
        "SELECT * FROM UserPrefs WHERE UserId = @UserId",
        new { UserId = userId },
        ct);
}
```

**`QueryOptionalAsync`** - Returns first row if multiple exist:

```csharp
public async Task<Result<Optional<Order>>> GetLatestOrderAsync(Guid customerId, CancellationToken ct)
{
    return await db.QueryOptionalAsync<Order>(
        "SELECT * FROM Orders WHERE CustomerId = @CustomerId ORDER BY CreatedAt DESC LIMIT 1",
        new { CustomerId = customerId },
        ct);
}
```

| Method | No rows | One row | Multiple rows |
|--------|---------|---------|---------------|
| `GetOptionalAsync` | `Optional.Empty` | `Optional.Some(value)` | **Failure** |
| `QueryOptionalAsync` | `Optional.Empty` | `Optional.Some(value)` | Returns first row |

```csharp
var result = await GetUserPrefsAsync(userId, ct);
if (result.IsSuccess && result.Value.HasValue) {
    var prefs = result.Value.Value;
    // Use prefs
}
```

### Multiple Result Sets

Use multiple result queries when your SQL returns multiple result sets that need to be combined:

```csharp
public async Task<Result<OrderWithItems>> GetOrderWithItemsAsync(Guid orderId, CancellationToken ct)
{
    await using var conn = await db.CreateConnectionAsync(ct);

    return await conn.MultipleGetAsync<OrderWithItems>(
        """
        SELECT * FROM Orders WHERE OrderId = @OrderId;
        SELECT * FROM OrderItems WHERE OrderId = @OrderId;
        """,
        new { OrderId = orderId },
        keys: [orderId],
        async reader => {
            var order = await reader.ReadSingleOrDefaultAsync<OrderDto>();
            if (order is null) return null;

            var items = await reader.ReadAsync<OrderItemDto>();
            return new OrderWithItems(order, items.ToList());
        },
        ct);
}
```

## Pagination

### Offset Pagination (PagedResult)

Best for smaller datasets with "Page X of Y" UI requirements.
```csharp
public async Task<Result<PagedResult<Order>>> GetOrdersPagedAsync(
    Guid customerId, int pageSize, int pageNumber, CancellationToken ct)
{
    var offset = (pageNumber - 1) * pageSize;

    // Query 1: Get total count
    var totalCountResult = await db.GetScalarAsync<int>(
        "SELECT COUNT(*) FROM Orders WHERE CustomerId = @CustomerId",
        new { CustomerId = customerId },
        ct);

    if (totalCountResult.IsFailure)
        return totalCountResult.Error;

    // Query 2: Get page data (syntax varies by database)
    return await db.QueryPagedAsync<Order>(
        """
        SELECT * FROM Orders
        WHERE CustomerId = @CustomerId
        ORDER BY CreatedAt DESC
        LIMIT @PageSize OFFSET @Offset
        """,
        new { CustomerId = customerId, Offset = offset, PageSize = pageSize },
        totalCountResult.Value, pageSize, pageNumber, ct);
}
```

### Cursor Pagination (CursorResult)

Best for large datasets, infinite scroll, and real-time data where consistency matters.
```csharp
public async Task<Result<CursorResult<Order>>> GetOrdersCursorAsync(
    Guid customerId, int pageSize, string? cursor, CancellationToken ct)
{
    var decoded = Cursor.Decode<DateTime>(cursor);

    var sql = decoded is null
        ? """
          SELECT * FROM Orders
          WHERE CustomerId = @CustomerId
          ORDER BY CreatedAt DESC, OrderId DESC
          LIMIT @PageSize
          """
        : """
          SELECT * FROM Orders
          WHERE CustomerId = @CustomerId
            AND (CreatedAt < @Column OR (CreatedAt = @Column AND OrderId < @Id))
          ORDER BY CreatedAt DESC, OrderId DESC
          LIMIT @PageSize
          """;

    return await db.QueryCursorAsync<Order, DateTime>(
        sql,
        new { CustomerId = customerId, decoded?.Column, decoded?.Id, PageSize = pageSize },
        pageSize,
        o => (o.CreatedAt, o.OrderId),  // Cursor selector
        ct);
}
```

### Slice Queries (SliceResult)

For "preview with expand" scenarios - load an initial batch and indicate if more exist. Not for pagination.
```csharp
public async Task<Result<SliceResult<Order>>> GetRecentOrdersAsync(
    Guid customerId, CancellationToken ct)
{
    return await db.QuerySliceAsync<Order>(
        """
        SELECT * FROM Orders
        WHERE CustomerId = @CustomerId
        ORDER BY CreatedAt DESC
        LIMIT @Limit
        """,
        new { CustomerId = customerId, Limit = 6 },  // Fetch pageSize + 1 to detect HasMore
        pageSize: 5,
        ct);
}
```
```razor
@foreach (var order in slice.Items) { ... }

@if (slice.HasMore) {
    <a href="/orders">View All Orders</a>
}
```

**Use cases:**
- Dashboard widgets showing recent items with "View All" link
- Preview cards with "Show More" expansion
- Batch processing where you grab N items at a time

**Not for:**
- Paginating through results (use `PagedResult` or `CursorResult`)
- Infinite scroll (use `CursorResult`)

## Command Extensions

Insert, Update, and Delete extensions automatically handle SQL constraint violations and convert them to appropriate Result failures.

### Insert
```csharp
// Simple insert - returns Result (success/failure only)
public async Task<Result> CreateOrderAsync(CreateOrder command, CancellationToken ct)
{
    return await db.InsertAsync(
        """
        INSERT INTO Orders (OrderId, CustomerId, Amount, CreatedAt)
        VALUES (@OrderId, @CustomerId, @Amount, @CreatedAt)
        """,
        new { command.OrderId, command.CustomerId, command.Amount, CreatedAt = DateTime.UtcNow },
        ct);
}

// Insert with return value - returns Result<T> with the generated ID
public async Task<Result<Guid>> CreateOrderAsync(CreateOrder command, CancellationToken ct)
{
    var orderId = Guid.CreateVersion7();

    return await db.InsertAndReturnAsync(
        """
        INSERT INTO Orders (OrderId, CustomerId, Amount, CreatedAt)
        VALUES (@OrderId, @CustomerId, @Amount, @CreatedAt)
        """,
        new { OrderId = orderId, command.CustomerId, command.Amount, CreatedAt = DateTime.UtcNow },
        () => orderId,  // Return the generated ID on success
        ct);
}
```

### Update
```csharp
// Simple update - returns Result (success/failure only)
public async Task<Result> UpdateOrderAsync(UpdateOrder command, CancellationToken ct)
{
    return await db.UpdateAsync(
        "UPDATE Orders SET Amount = @Amount WHERE OrderId = @OrderId",
        new { command.OrderId, command.Amount },
        key: command.OrderId,  // Returns NotFound if 0 rows affected
        ct);
}

// Update with return value - returns Result<T>
public async Task<Result<Guid>> UpdateOrderAsync(UpdateOrder command, CancellationToken ct)
{
    return await db.UpdateAndReturnAsync(
        "UPDATE Orders SET Amount = @Amount WHERE OrderId = @OrderId",
        new { command.OrderId, command.Amount },
        key: command.OrderId,
        () => command.OrderId,  // Return the ID on success
        ct);
}
```

### Delete
```csharp
public async Task<Result> DeleteOrderAsync(Guid orderId, CancellationToken ct)
{
    return await db.DeleteAsync(
        "DELETE FROM Orders WHERE OrderId = @OrderId",
        new { OrderId = orderId },
        key: orderId,  // Returns NotFound if 0 rows affected
        ct);
}
```

### Row Count Variants

Use the `*WithCountAsync` variants when you need the number of affected rows and zero rows should not be treated as a failure:

```csharp
// Update and get affected row count
public async Task<Result<int>> MarkOrdersShippedAsync(Guid customerId, CancellationToken ct)
{
    await using var conn = await db.CreateConnectionAsync(ct);

    return await conn.UpdateWithCountAsync(
        """
        UPDATE Orders
        SET Status = 'Shipped', ShippedAt = @ShippedAt
        WHERE CustomerId = @CustomerId AND Status = 'Pending'
        """,
        new { CustomerId = customerId, ShippedAt = DateTime.UtcNow },
        ct);
}

// Delete and get affected row count
public async Task<Result<int>> PurgeOldOrdersAsync(DateTime cutoff, CancellationToken ct)
{
    await using var conn = await db.CreateConnectionAsync(ct);

    return await conn.DeleteWithCountAsync(
        "DELETE FROM Orders WHERE CreatedAt < @Cutoff AND Status = 'Completed'",
        new { Cutoff = cutoff },
        ct);
}
```

**Behavior difference:**

| Method | 0 rows affected |
|--------|-----------------|
| `InsertAsync` | Returns `InvalidOperationException` |
| `UpdateAsync` / `DeleteAsync` | Returns `NotFoundException` |
| `*WithCountAsync` variants | Returns the count (not a failure) |

Use `*WithCountAsync` when you need the affected row count, or when zero affected rows is a valid outcome.

### Constraint Handling Summary

| Operation | Constraint | Result | HTTP |
|-----------|------------|--------|------|
| INSERT | No rows affected | `InvalidOperationException` | 500 |
| INSERT | Unique violation | `AlreadyExistsException` | 409 |
| INSERT | FK violation | `BadRequestException` | 400 |
| UPDATE | No rows affected | `NotFoundException` | 404 |
| UPDATE | Unique violation | `AlreadyExistsException` | 409 |
| UPDATE | FK violation | `BadRequestException` | 400 |
| DELETE | No rows affected | `NotFoundException` | 404 |
| DELETE | FK violation | `ConflictException` | 409 |

## Fluent Transaction Chaining

Chain multiple database operations in a single transaction with railway-oriented error handling. If any operation fails, subsequent operations are skipped and the error propagates.

The library provides two result wrapper types that enable fluent chaining within transactions:
- **`DbResult`** - Non-generic result for operations that don't return a value
- **`DbResult<T>`** - Generic result that carries a value through the chain

These types wrap `Result`/`Result<T>` along with the `DbContext`, enabling method chaining while preserving transaction scope. They function similarly to a Reader monad, threading the transaction context through each operation.

### Basic Chaining
```csharp
public async Task<Result<OrderDto>> CreateOrderWithItemsAsync(
    CreateOrder command, CancellationToken ct)
{
    await using var conn = await db.CreateConnectionAsync(ct);

    return await conn.ExecuteTransactionAsync(ctx =>
        ctx.GetAsync<CustomerDto>(
            "SELECT * FROM Customers WHERE CustomerId = @Id",
            new { Id = command.CustomerId },
            key: command.CustomerId)
        .ThenInsertAsync(
            "INSERT INTO Orders (OrderId, CustomerId, CreatedAt) VALUES (@OrderId, @CustomerId, @CreatedAt)",
            customer => new { OrderId = command.OrderId, customer.CustomerId, CreatedAt = DateTime.UtcNow })
        .ThenGetAsync<OrderDto>(
            "SELECT * FROM Orders WHERE OrderId = @Id",
            new { Id = command.OrderId },
            key: command.OrderId)
    , ct);
}
```

### Available Chain Methods

**From `DbContext` (starting point):**

*Query Methods:*
- `GetAsync<T>(...)` - Query single record (fails if not found)
- `GetAsync<TData, TModel>(...)` - Query single record with mapper
- `GetOptionalAsync<T>(...)` - Query optional record (fails if multiple rows)
- `QueryOptionalAsync<T>(...)` - Query optional record (returns first if multiple)
- `GetScalarAsync<T>(...)` - Query scalar value
- `QueryAnyAsync<T>(...)` - Query collection
- `QueryPagedAsync<T>(...)` - Query with offset pagination
- `QueryCursorAsync<T, TColumn>(...)` - Query with cursor pagination
- `QuerySliceAsync<T>(...)` - Query slice with "has more" indicator
- `MultipleGetAsync<T>(...)` - Query multiple result sets (fails if not found)
- `MultipleGetOptionalAsync<T>(...)` - Query multiple result sets (optional)
- `MultipleQueryAnyAsync<T>(...)` - Query multiple result sets (collection)

*Insert Methods:*
- `InsertAsync(...)` - Insert, returns `DbResult`
- `InsertAndReturnAsync<T>(...)` - Insert, returns `DbResult<T>`
- `InsertAndReturnAsync<TEntity>(sql, entity)` - Entity shortcut
- `InsertWithCountAsync(...)` - Insert, returns `DbResult<int>` (rows affected)
- `InsertIfAsync(..., when)` - Conditional insert, returns `DbResult`
- `InsertIfAndReturnAsync<T>(..., when)` - Conditional insert, returns `DbResult<T>`

*Update Methods:*
- `UpdateAsync(...)` - Update, returns `DbResult`
- `UpdateAndReturnAsync<T>(...)` - Update, returns `DbResult<T>`
- `UpdateWithCountAsync(...)` - Update, returns `DbResult<int>` (rows affected)
- `UpdateIfAsync(..., when)` - Conditional update, returns `DbResult`
- `UpdateIfAndReturnAsync<T>(..., when)` - Conditional update, returns `DbResult<T>`

*Delete Methods:*
- `DeleteAsync(...)` - Delete, returns `DbResult`
- `DeleteWithCountAsync(...)` - Delete, returns `DbResult<int>` (rows affected)
- `DeleteIfAsync(..., when)` - Conditional delete, returns `DbResult`

**From `DbResult<T>` (typed result):**

*Transform/Validate:*
- `MapAsync(Func<T, TResult>)` - Transform the value
- `EnsureAsync(Func<T, bool>, Exception)` - Validate with predicate
- `ToResult()` - Convert `DbResult<T>` to `DbResult` (discard value)

*Escape Hatch:*
- `ThenAsync(Func<T, Task<Result>>)` - External async operations
- `ThenAsync<TResult>(Func<T, Task<Result<TResult>>>)` - External async that transforms type

*Query Methods:*
- `ThenGetAsync<TResult>(...)` - Query single record (fails if not found)
- `ThenGetOptionalAsync<TResult>(...)` - Query optional record (returns `Optional<TResult>`)
- `ThenGetScalarAsync<TResult>(...)` - Query scalar value
- `ThenQueryAnyAsync<TResult>(...)` - Query collection
- `ThenMultipleGetAsync<TResult>(...)` - Query multiple result sets
- `ThenMultipleGetOptionalAsync<TResult>(...)` - Query multiple result sets (optional)
- `ThenMultipleQueryAnyAsync<TResult>(...)` - Query multiple result sets (collection)

*Insert Methods:*
- `ThenInsertAsync(...)` - Insert, returns `DbResult<T>` (pass-through)
- `ThenInsertAndReturnAsync<TResult>(...)` - Insert, returns `DbResult<TResult>` (transform)
- `ThenInsertIfAsync(..., when)` - Conditional insert, pass-through
- `ThenInsertIfAndReturnAsync<TResult>(..., when)` - Conditional insert with transform

*Update Methods:*
- `ThenUpdateAsync(...)` - Update, returns `DbResult<T>` (pass-through)
- `ThenUpdateAndReturnAsync<TResult>(...)` - Update, returns `DbResult<TResult>` (transform)
- `ThenUpdateWithCountAsync(...)` - Update, returns `DbResult<int>` (rows affected)
- `ThenUpdateIfAsync(..., when)` - Conditional update, pass-through
- `ThenUpdateIfAndReturnAsync<TResult>(..., when)` - Conditional update with transform

*Delete Methods:*
- `ThenDeleteAsync(...)` - Delete, returns `DbResult<T>` (pass-through)
- `ThenDeleteAndReturnAsync<TResult>(...)` - Delete, returns `DbResult<TResult>` (transform)
- `ThenDeleteWithCountAsync(...)` - Delete, returns `DbResult<int>` (rows affected)
- `ThenDeleteIfAsync(..., when)` - Conditional delete, pass-through
- `ThenDeleteIfAndReturnAsync<TResult>(..., when)` - Conditional delete with transform

*Tuple Accumulation (see Tuple Accumulation section):*
- `AndGetAsync<TNew>(...)` - Accumulate required record into tuple
- `AndGetOptionalAsync<TNew>(...)` - Accumulate optional record (fails if multiple)
- `AndQueryOptionalAsync<TNew>(...)` - Accumulate optional record (returns first if multiple)
- `AndQueryAnyAsync<TNew>(...)` - Accumulate collection into tuple
- `AndGetScalarAsync<TNew>(...)` - Accumulate scalar into tuple

**From `DbResult` (void result):**

*Escape Hatch:*
- `ThenAsync(Func<Task<Result>>)` - External async operations
- `ThenAsync<T>(Func<Task<Result<T>>>)` - External async that produces typed result

*Query Methods:*
- `ThenGetAsync<TResult>(...)` - Query single record (fails if not found)
- `ThenGetOptionalAsync<TResult>(...)` - Query optional record (returns `Optional<TResult>`)
- `ThenGetScalarAsync<TResult>(...)` - Query scalar value
- `ThenQueryAnyAsync<TResult>(...)` - Query collection
- `ThenMultipleGetAsync<TResult>(...)` - Query multiple result sets
- `ThenMultipleGetOptionalAsync<TResult>(...)` - Query multiple result sets (optional)
- `ThenMultipleQueryAnyAsync<TResult>(...)` - Query multiple result sets (collection)

*Insert Methods:*
- `ThenInsertAsync(...)` - Insert, returns `DbResult`
- `ThenInsertAndReturnAsync<T>(...)` - Insert, returns `DbResult<T>`
- `ThenInsertAndReturnAsync<TEntity>(sql, entity)` - Entity shortcut
- `ThenInsertIfAsync(..., when)` - Conditional insert
- `ThenInsertIfAndReturnAsync<T>(..., when)` - Conditional insert with transform
- `ThenInsertIfAndReturnAsync<TEntity>(sql, entity, when)` - Conditional entity shortcut

*Update Methods:*
- `ThenUpdateAsync(...)` - Update, returns `DbResult`
- `ThenUpdateAndReturnAsync<T>(...)` - Update, returns `DbResult<T>`
- `ThenUpdateWithCountAsync(...)` - Update, returns `DbResult<int>` (rows affected)
- `ThenUpdateIfAsync(..., when)` - Conditional update
- `ThenUpdateIfAndReturnAsync<T>(..., when)` - Conditional update with transform

*Delete Methods:*
- `ThenDeleteAsync(...)` - Delete, returns `DbResult`
- `ThenDeleteAndReturnAsync<T>(...)` - Delete, returns `DbResult<T>`
- `ThenDeleteWithCountAsync(...)` - Delete, returns `DbResult<int>` (rows affected)
- `ThenDeleteIfAsync(..., when)` - Conditional delete
- `ThenDeleteIfAndReturnAsync<T>(..., when)` - Conditional delete with transform

### Using Previous Values

Insert, Update, and Delete methods provide overloads that access the previous result value:

```csharp
// Use customer data to build order parameters
.ThenInsertAsync(
    "INSERT INTO Orders (OrderId, CustomerId, Tier) VALUES (@OrderId, @CustomerId, @Tier)",
    customer => new { OrderId = orderId, customer.CustomerId, customer.Tier })
```

### Returning Values from Mutations

Use result selectors to return values from Insert/Update operations. The `AndReturn` variants transform the result type:

```csharp
var orderId = Guid.CreateVersion7();

return await conn.ExecuteTransactionAsync(ctx =>
    ctx.InsertAndReturnAsync(
        "INSERT INTO Orders (...) VALUES (...)",
        new { OrderId = orderId, ... },
        () => orderId)  // Returns the new order ID as DbResult<Guid>
    .ThenGetAsync<OrderDto>(
        "SELECT * FROM Orders WHERE OrderId = @Id",
        new { Id = orderId },
        key: orderId)
, ct);
```

From `DbResult<T>`, use `ThenInsertAndReturnAsync` to transform to a new type:

```csharp
return await conn.ExecuteTransactionAsync(ctx =>
    ctx.GetAsync<CustomerDto>(
        "SELECT * FROM Customers WHERE CustomerId = @Id",
        new { Id = customerId },
        customerId)
    .ThenInsertAndReturnAsync(
        "INSERT INTO Orders (...) VALUES (...)",
        c => new { OrderId = orderId, c.CustomerId, ... },
        c => orderId)  // Transforms CustomerDto -> Guid
    .ThenGetAsync<OrderDto>(
        "SELECT * FROM Orders WHERE OrderId = @Id",
        new { Id = orderId },
        orderId)
, ct);
```

### Conditional Operations

Conditional operations allow you to skip database commands based on a predicate. If `when` returns `false`, the operation is skipped and the chain continues with a successful result.

The `when` parameter supports multiple overloads:
- `bool when` - Simple boolean, evaluated immediately
- `Func<bool> when` - Deferred evaluation
- `Func<T, bool> when` - Predicate based on the previous result value (for `DbResult<T>` only)

**Starting with a conditional operation** - Use `InsertIfAsync`, `UpdateIfAsync`, or `DeleteIfAsync` on `DbContext`:

```csharp
var request = new { ShouldCreateOrder = true, ShouldNotify = false };

return await conn.ExecuteTransactionAsync(ctx =>
    ctx.InsertIfAsync(
        "INSERT INTO Orders (OrderId, CustomerId) VALUES (@OrderId, @CustomerId)",
        new { OrderId = orderId, CustomerId = customerId },
        when: () => request.ShouldCreateOrder)  // Only insert if flag is set
    .ThenInsertIfAsync(
        "INSERT INTO Notifications (...) VALUES (...)",
        new { ... },
        when: () => request.ShouldNotify)  // Skipped when false
, ct);
```

**From `DbResult<T>`** - The predicate receives the current value:

```csharp
return await conn.ExecuteTransactionAsync(ctx =>
    ctx.GetAsync<CustomerDto>(
        "SELECT * FROM Customers WHERE CustomerId = @Id",
        new { Id = customerId },
        customerId)
    .ThenInsertIfAsync(
        "INSERT INTO AuditLog (CustomerId, Action) VALUES (@CustomerId, @Action)",
        c => new { c.CustomerId, Action = "Accessed" },
        when: c => c.TrackActivity)  // Only insert if tracking enabled; CustomerDto passes through
    .ThenUpdateIfAsync(
        "UPDATE Customers SET LastAccessedAt = @Now WHERE CustomerId = @CustomerId",
        c => new { c.CustomerId, Now = DateTime.UtcNow },
        customerId,
        when: c => c.IsActive)  // Only update if active; CustomerDto passes through
, ct);
```

**From `DbResult`** - The predicate is a simple `Func<bool>`:

```csharp
var request = new { ShouldAudit = true };

return await conn.ExecuteTransactionAsync(ctx =>
    ctx.InsertAsync(
        "INSERT INTO Orders (...) VALUES (...)",
        new { OrderId = orderId, ... })
    .ThenInsertIfAsync(
        "INSERT INTO AuditLog (...) VALUES (...)",
        new { ... },
        when: () => request.ShouldAudit)  // Captures external value
, ct);
```

### Conditional Operations with Type Transformation

Use `ThenInsertIfAndReturnAsync`, `ThenUpdateIfAndReturnAsync`, etc. when you need the type to transform regardless of whether the operation executes:

**From `DbResult<T>` to `DbResult<TResult>`:**

```csharp
return await conn.ExecuteTransactionAsync(ctx =>
    ctx.GetAsync<CustomerDto>(
        "SELECT * FROM Customers WHERE CustomerId = @Id",
        new { Id = customerId },
        customerId)
    .ThenInsertIfAndReturnAsync(
        "INSERT INTO Orders (...) VALUES (...)",
        c => new { OrderId = orderId, c.CustomerId, ... },
        c => orderId,  // resultSelector: CustomerDto -> string (orderId)
        when: c => c.IsActive)
    .ThenUpdateAsync(  // Now receives string (orderId), not CustomerDto
        "UPDATE Orders SET Status = @Status WHERE OrderId = @Id",
        oid => new { Id = oid, Status = "Confirmed" },
        orderId)
    .ToResult()  // Convert to DbResult when value is no longer needed
, ct);
```

**From `DbResult` to `DbResult<T>`:**

```csharp
return await conn.ExecuteTransactionAsync<string>(ctx =>
    ctx.InsertAsync(
        "INSERT INTO Users (...) VALUES (...)",
        new { ... })
    .ThenInsertIfAsync(
        "INSERT INTO Orders (...) VALUES (...)",
        new { OrderId = orderId, ... },
        () => orderId,  // resultSelector: transforms DbResult -> DbResult<string>
        when: () => shouldCreateOrder)
    .ThenUpdateAsync(  // Receives string (orderId)
        "UPDATE Orders SET Amount = @Amount WHERE Id = @Id",
        oid => new { Id = oid, Amount = 100.0 },
        orderId)
, ct);
```

The key insight: **`resultSelector` always runs** (when the chain is successful), even if `when` returns `false` and the operation is skipped. This allows consistent type transformation for subsequent operations.

**Pass-through pattern:** If you want the original type to pass through (no transformation), use the non-`AndReturn` variants:

```csharp
return await conn.ExecuteTransactionAsync(ctx =>
    ctx.GetAsync<CustomerDto>(...)
    .ThenInsertIfAsync(
        "INSERT INTO PremiumCustomers (...) VALUES (...)",
        c => new { c.CustomerId, ... },
        when: c => c.IsPremium)  // CustomerDto passes through
    .MapAsync(c => new CustomerSummary(c.CustomerId, c.Name))  // Transform after
, ct);
```

### Entity Shortcuts

When inserting an entity where the entity itself serves as both the parameters and the return value, use the entity shortcut overloads:

```csharp
var order = new Order(Guid.CreateVersion7(), customerId, 100.0m);

// Instead of this...
ctx.InsertAndReturnAsync(
    "INSERT INTO Orders (...) VALUES (...)",
    new { order.Id, order.CustomerId, order.Amount },
    () => order)

// You can write this...
ctx.InsertAndReturnAsync(
    "INSERT INTO Orders (Id, CustomerId, Amount) VALUES (@Id, @CustomerId, @Amount)",
    order)  // Entity used as both params and return value
```

Available entity shortcut overloads:

**From `DbContext`:**
- `InsertAndReturnAsync<TEntity>(sql, entity)`

**From `DbResult` (non-generic):**
- `ThenInsertAndReturnAsync<TEntity>(sql, entity)`
- `ThenInsertIfAndReturnAsync<TEntity>(sql, entity, when)`

**From `DbResult<T>`:**
- `ThenInsertAndReturnAsync<TEntity>(sql, entity)`
- `ThenInsertIfAndReturnAsync<TEntity>(sql, entity, when)`

The entity is used as the Dapper parameters and returned on success. Constraint violations are translated to appropriate Result failures (e.g., `AlreadyExistsException` for unique violations).

### Escape Hatch: ThenAsync

The `ThenAsync` methods allow you to integrate external async operations that return `Result` types into the fluent chain. This is useful for calling external services, complex validation, or chaining to other repositories:

```csharp
return await conn.ExecuteTransactionAsync(ctx =>
    ctx.GetAsync<CustomerDto>(
        "SELECT * FROM Customers WHERE CustomerId = @Id",
        new { Id = customerId },
        customerId)
    .ThenAsync(async customer => {
        // Call external service - if it fails, transaction rolls back
        return await paymentService.ValidateCustomerAsync(customer.CustomerId);
    })
    .ThenInsertAsync(
        "INSERT INTO Orders (...) VALUES (...)",
        new { OrderId = orderId, CustomerId = customerId, ... })
, ct);
```

Use `ThenAsync<TResult>` when the external operation produces a value needed by subsequent operations:

```csharp
return await conn.ExecuteTransactionAsync(ctx =>
    ctx.GetAsync<CustomerDto>(...)
    .ThenAsync<PaymentToken>(async customer => {
        // External call returns a value for the chain
        return await paymentService.CreateTokenAsync(customer.CustomerId);
    })
    .ThenInsertAsync(
        "INSERT INTO Orders (...) VALUES (...)",
        token => new { OrderId = orderId, PaymentToken = token.Value, ... })
, ct);
```

### Error Short-Circuiting

Failures propagate without executing subsequent operations:

```csharp
await conn.ExecuteTransactionAsync(ctx =>
    ctx.GetAsync<CustomerDto>(...)         // Returns NotFound
    .ThenInsertAsync(...)                  // Skipped
    .ThenUpdateAsync(...)                  // Skipped
    .ThenGetAsync<OrderDto>(...)           // Skipped - returns original NotFound
, ct);
```

## Tuple Accumulation

When you need to fetch multiple independent records and combine them, use the `And*` accumulator methods. These build up tuples of results while preserving railway-oriented error handling.

### Basic Accumulation

```csharp
// Fetch user and their preferences, then map to a domain object
var result = await conn.ExecuteAsync(ctx =>
    ctx.GetAsync<UserDto>(
        "SELECT * FROM Users WHERE Id = @Id",
        new { Id = userId },
        userId)
    .AndGetOptionalAsync<UserDto, UserPrefsDto>(
        "SELECT * FROM UserPrefs WHERE UserId = @Id",
        new { Id = userId })
    .MapAsync((user, prefs) => new UserProfile(
        user.Name,
        prefs.HasValue ? prefs.Value.Theme : "default"))
, ct);

// Result is UserProfile
if (result.IsSuccess) {
    var profile = result.Value;
}
```

When you call `MapAsync` on a tuple result, C# automatically deconstructs the tuple parameters, giving you clean access to each accumulated value.

### Available Accumulator Methods

Accumulator methods chain from `DbResult<T>` or `DbResult<(T1, T2, ...)>`:

- `AndGetAsync<..., TNew>(...)` - Append a required record (fails if not found)
- `AndGetOptionalAsync<..., TNew>(...)` - Append an optional record (fails if multiple rows)
- `AndQueryOptionalAsync<..., TNew>(...)` - Append an optional record (returns first if multiple)
- `AndGetScalarAsync<..., TNew>(...)` - Append a scalar value

These methods support tuples up to 7 elements: `(T1)` → `(T1, T2)` → ... → `(T1, T2, T3, T4, T5, T6, T7)`.

### Multi-Record Example

```csharp
// Fetch user, order, and count, then map to a summary object
var result = await conn.ExecuteAsync(ctx =>
    ctx.GetAsync<UserDto>(userSql, new { Id = userId }, userId)
    .AndGetAsync<UserDto, OrderDto>(orderSql, new { Id = orderId }, orderId)
    .AndGetScalarAsync<UserDto, OrderDto, long>(
        "SELECT COUNT(*) FROM Orders WHERE UserId = @UserId",
        new { UserId = userId })
    .MapAsync((user, order, orderCount) => new OrderSummary(
        user.Name,
        order.Total,
        orderCount))
, ct);

// Result is OrderSummary
if (result.IsSuccess) {
    var summary = result.Value;
}
```

### Short-Circuit Behavior

If any query fails, the chain short-circuits and returns the first error:

```csharp
ctx.GetAsync<UserDto>(...)           // User found
   .AndGetAsync<OrderDto>(...)       // Order not found - returns NotFound
   .AndGetScalarAsync<long>(...)     // Skipped
```

## Non-Transactional Operations

Both `ExecuteAsync` and `ExecuteTransactionAsync` provide the same `DbContext` fluent chaining experience. The only difference is transaction scope:

```csharp
// ExecuteAsync - same DbContext fluent API, but no transaction (auto-commit per statement)
var result = await db.ExecuteAsync(ctx =>
    ctx.GetAsync<UserDto>(
        "SELECT * FROM Users WHERE Id = @Id",
        new { Id = userId },
        userId)
    .ThenInsertAsync(
        "INSERT INTO AuditLog (Id, Action, UserId) VALUES (@Id, @Action, @UserId)",
        user => new { Id = Guid.NewGuid(), Action = "UserViewed", UserId = user.Id })
, ct);
```

**When to use `ExecuteAsync` vs `ExecuteTransactionAsync`:**
- `ExecuteAsync` - Read-only operations, independent writes, audit logging
- `ExecuteTransactionAsync` - Multiple related writes that must succeed or fail together

Both support the full fluent API including `Then*`, `And*`, `Map`, and `Ensure` operations.

## Factory Extensions

`ISqlConnectionFactory` provides the same extension methods available on `ISqlConnection`, but handles connection management automatically. These are the preferred approach for most operations:

```csharp
// Preferred - factory handles connection lifecycle
return await db.GetAsync<OrderDto>(sql, parameters, key, ct);

// Only needed for advanced scenarios (manual transaction control, connection reuse)
await using var conn = await db.CreateConnectionAsync(ct);
return await conn.GetAsync<OrderDto>(sql, parameters, key, null, ct);
```

### Example Usage

```csharp
public class OrderRepository(IDbConnectionFactory db)
{
    public Task<Result<OrderDto>> GetOrderAsync(Guid orderId, CancellationToken ct)
        => db.GetAsync<OrderDto>(
            "SELECT * FROM Orders WHERE OrderId = @Id",
            new { Id = orderId },
            orderId,
            ct);

    public Task<Result<Guid>> CreateOrderAsync(CreateOrder cmd, CancellationToken ct)
    {
        var orderId = Guid.CreateVersion7();
        return db.InsertAndReturnAsync(
            "INSERT INTO Orders (OrderId, CustomerId, Amount) VALUES (@OrderId, @CustomerId, @Amount)",
            new { OrderId = orderId, cmd.CustomerId, cmd.Amount },
            () => orderId,
            ct);
    }

    public Task<Result<OrderDto>> CreateOrderWithValidationAsync(CreateOrder cmd, CancellationToken ct)
        => db.ExecuteTransactionAsync(ctx =>
            ctx.GetAsync<CustomerDto>(
                "SELECT * FROM Customers WHERE CustomerId = @Id",
                new { Id = cmd.CustomerId },
                cmd.CustomerId)
            .EnsureAsync(
                c => c.IsActive,
                new BadRequestException("Customer is not active"))
            .ThenInsertAndReturnAsync(
                "INSERT INTO Orders (...) VALUES (...)",
                c => new { OrderId = Guid.CreateVersion7(), c.CustomerId, cmd.Amount },
                c => new OrderDto(...))  // Transform CustomerDto -> OrderDto
        , ct);
}
```

## Provider-Specific Packages

For production use with specific databases, use the provider packages that include connection factories with authentication support:

- **`Cirreum.Persistence.Sql.SqlServer`** - SQL Server with Azure Entra ID authentication
- **`Cirreum.Persistence.Sql.Sqlite`** - SQLite (coming soon)
- **`Cirreum.Persistence.Sql.PostgreSql`** - PostgreSQL (coming soon)

## Contribution Guidelines

1. **Be conservative with new abstractions** - The API surface must remain stable and meaningful.
2. **Limit dependency expansion** - Only add foundational, version-stable dependencies.
3. **Favor additive, non-breaking changes** - Breaking changes ripple through the entire ecosystem.
4. **Include thorough unit tests** - All primitives and patterns should be independently testable.
5. **Document architectural decisions** - Context and reasoning should be clear for future maintainers.
6. **Follow .NET conventions** - Use established patterns from Microsoft.Extensions.* libraries.

## Versioning

Cirreum.Persistence.Sql follows [Semantic Versioning](https://semver.org/):

- **Major** - Breaking API changes
- **Minor** - New features, backward compatible
- **Patch** - Bug fixes, backward compatible

Given its foundational role, major version bumps are rare and carefully considered.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Cirreum Foundation Framework**
*Layered simplicity for modern .NET*
