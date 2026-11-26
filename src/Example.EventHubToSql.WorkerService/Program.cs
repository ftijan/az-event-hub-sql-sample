using Example.Common.Data;

namespace Example.EventHubToSql.WorkerService;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.AddServiceDefaults();

		var connectionString =
			builder.Configuration.GetConnectionString("market-eventhubs")
			?? builder.Configuration.GetConnectionString("stock-ticks");

		if (string.IsNullOrWhiteSpace(connectionString))
		{
			throw new InvalidOperationException(
				"No Event Hubs connection string found in ConnectionStrings:market-eventhubs or ConnectionStrings:stock-ticks.");
		}

		// Event Hubs consumer client bound to the "stock-ticks" resource
		builder.AddAzureEventHubConsumerClient("market-eventhubs", settings =>
		{
			settings.ConnectionString ??= connectionString;
			settings.EventHubName = "stock-ticks";
		});

		// SQL Server DbContext bound to the "marketdb" resource + repository
		builder.AddSqlServerDbContext<MarketDbContext>("marketdb");
		builder.Services.AddScoped<StockTickRepository>();

		// Background worker reading events and writing to SQL
		builder.Services.AddHostedService<StockTickConsumerWorker>();

		var host = builder.Build();

		// Ensure local SQL Server container schema exists before consuming
		using (var scope = host.Services.CreateScope())
		{
			var dbContext = scope.ServiceProvider.GetRequiredService<MarketDbContext>();
			await dbContext.Database.EnsureCreatedAsync();
		}

		host.Run();
    }
}
