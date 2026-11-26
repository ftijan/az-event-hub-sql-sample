using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Example.Common.Data;
using Microsoft.AspNetCore.Mvc;
using Scalar.AspNetCore;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add the DB context
builder.AddSqlServerDbContext<MarketDbContext>("marketdb");
builder.Services.AddScoped<StockTickRepository>();

var connectionString =
	builder.Configuration.GetConnectionString("market-eventhubs")
	?? builder.Configuration.GetConnectionString("stock-ticks");

if (string.IsNullOrWhiteSpace(connectionString))
{
	throw new InvalidOperationException(
		"No Event Hubs connection string found in ConnectionStrings:market-eventhubs or ConnectionStrings:stock-ticks.");
}

// Add Event Hubs
builder.AddAzureEventHubProducerClient("market-eventhubs", settings =>
{
	settings.ConnectionString ??= connectionString;
	settings.EventHubName = "stock-ticks";
});

// Add services to the container.
builder.Services.AddProblemDetails();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
	app.MapScalarApiReference(); // serves UI at /scalar
}

app.MapGet("/", () => "API service is running.");

app.MapPost("/ticks/generate", async (
	[FromServices] EventHubProducerClient producerClient,
	CancellationToken cancellationToken) =>
{
	var ticks = StockTickProbeGenerator.GenerateBatch();

	using EventDataBatch eventBatch = await producerClient.CreateBatchAsync(cancellationToken);

	foreach (var tick in ticks)
	{
		var payload = JsonSerializer.SerializeToUtf8Bytes(tick);
		var eventData = new EventData(payload);

		if (!eventBatch.TryAdd(eventData))
		{
			throw new InvalidOperationException($"Event for {tick.TickerSymbol} exceeded the Event Hubs batch size limit.");
		}
	}

	await producerClient.SendAsync(eventBatch, cancellationToken);

	return Results.Ok(ticks);
});

app.MapGet("/ticks/series", async (
	DateTimeOffset? since,
	[FromServices] StockTickRepository repository,
	CancellationToken cancellationToken) =>
{
	var series = await repository.GetPriceSeriesByTickerAsync(since, cancellationToken);
	return Results.Ok(series);
});

app.MapGet("/ticks/series/{ticker}", async (
	string ticker,
	DateTimeOffset? since,
	int? take,
	StockTickRepository repository,
	CancellationToken cancellationToken) =>
{
	var series = await repository.GetSeriesByTickerAsync(ticker, since, take, cancellationToken);
	return series is null ? Results.NotFound() : Results.Ok(series);
});

app.MapDefaultEndpoints();

app.Run();

public static class StockTickProbeGenerator
{
	public static readonly string[] Tickers = ["GOOGL", "MSFT", "META", "AMZN"];

	public static IReadOnlyList<StockTickDto> GenerateBatch()
	{
		var now = DateTimeOffset.UtcNow;
		var results = new List<StockTickDto>(Tickers.Length);

		foreach (var ticker in Tickers)
		{
			// Random value in [0, 100.00] rounded to 2 decimal places
			decimal rawPrice = (decimal)(Random.Shared.NextDouble() * 100.0);
			decimal price = Math.Round(rawPrice, 2, MidpointRounding.AwayFromZero);

			results.Add(new StockTickDto(ticker, price, now));
		}

		return results;
	}
}