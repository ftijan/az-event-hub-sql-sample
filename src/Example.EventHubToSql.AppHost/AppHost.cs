var builder = DistributedApplication.CreateBuilder(args);

// 1. Azure Event Hubs emulator + stock ticks hub & consumer group
var marketEventHubs = builder.AddAzureEventHubs("market-eventhubs")
	.RunAsEmulator();

var stockTicksHub = marketEventHubs.AddHub("stock-ticks");
stockTicksHub.AddConsumerGroup("quote-persistence");

// 2. Azure SQL Database modeled locally as a SQL Server container
var marketDb = builder.AddAzureSqlServer("market-sql")
	.RunAsContainer()
	.AddDatabase("marketdb");

// 3. Ingestion API (POST endpoint generating/publishing GOOGL, MSFT, META, AMZN ticks)
var apiService = builder.AddDotnetProject("apiservice", "../Example.EventHubToSql.ApiService/Example.EventHubToSql.ApiService.csproj")
	.WithHttpHealthCheck("/health")
	.WithReference(stockTicksHub)
	.WithReference(marketDb)
	.WaitFor(marketEventHubs)
	.WaitFor(marketDb);

// 4. Background worker consuming ticks from Event Hubs and persisting quotes to SQL
var backgroundWorker = builder.AddDotnetProject("backgroundworker", "../Example.EventHubToSql.WorkerService/Example.EventHubToSql.WorkerService.csproj")
	.WithReference(stockTicksHub)
	.WithReference(marketDb)
	.WaitFor(marketEventHubs)
	.WaitFor(marketDb);

builder.Build().Run();
