using Azure.Messaging.EventHubs.Consumer;
using Example.Common.Data;
using System.Text.Json;
using System.Threading.Channels;

namespace Example.EventHubToSql.WorkerService;

public sealed class StockTickConsumerWorker(
	EventHubConsumerClient consumerClient,
	IServiceScopeFactory scopeFactory,
	ILogger<StockTickConsumerWorker> logger) : BackgroundService
{
	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		var partitionIds = await consumerClient.GetPartitionIdsAsync(stoppingToken);

		var tasks = partitionIds.Select(partitionId =>
			ReadAndSavePartitionAsync(partitionId, stoppingToken));

		await Task.WhenAll(tasks);
	}

	private async Task ReadAndSavePartitionAsync(string partitionId, CancellationToken stoppingToken)
	{
		var readOptions = new ReadEventOptions();
		var startingPosition = EventPosition.Latest;

		await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsFromPartitionAsync(
			partitionId: partitionId,
			startingPosition: startingPosition,
			readOptions: readOptions,
			cancellationToken: stoppingToken))
		{
			if (partitionEvent.Data is null)
			{
				continue;
			}

			var dto = JsonSerializer.Deserialize<StockTickDto>(partitionEvent.Data.EventBody.ToMemory().Span);
			if (dto is null)
			{
				logger.LogWarning("Skipped null StockTickDto on partition {PartitionId}.", partitionId);
				continue;
			}

			using var scope = scopeFactory.CreateScope();
			var repository = scope.ServiceProvider.GetRequiredService<StockTickRepository>();

			await repository.AddTickAsync([new StockTickDto(dto.TickerSymbol, dto.Price, dto.Timestamp)], stoppingToken);

			logger.LogInformation(
				"Saved {Ticker} ({Price}) from partition {PartitionId}.",
				dto.TickerSymbol,
				dto.Price,
				partitionId);
		}
	}
}

//public sealed class StockTickConsumerWorker(
//	EventHubConsumerClient consumerClient,
//	IServiceScopeFactory scopeFactory,
//	ILogger<StockTickConsumerWorker> logger) : BackgroundService
//{
//	private const int BatchSize = 4;
//	private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(2);

//	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
//	{
//		var channel = Channel.CreateUnbounded<StockTickDto>(new UnboundedChannelOptions
//		{
//			SingleReader = true
//		});

//		var partitionIds = await consumerClient.GetPartitionIdsAsync(stoppingToken);

//		var readerTasks = partitionIds.Select(partitionId =>
//			ReadPartitionToChannelAsync(partitionId, channel.Writer, stoppingToken)).ToList();

//		var flusherTask = FlushChannelAsync(channel.Reader, stoppingToken);

//		try
//		{
//			await Task.WhenAll(readerTasks);
//		}
//		finally
//		{
//			channel.Writer.TryComplete();
//			await flusherTask;
//		}
//	}




//	private async Task ReadPartitionToChannelAsync(
//		string partitionId,
//		ChannelWriter<StockTickDto> writer,
//		CancellationToken stoppingToken)
//	{
//		var readOptions = new ReadEventOptions();
//		var startingPosition = EventPosition.Latest;

//		await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsFromPartitionAsync(
//			partitionId: partitionId,
//			startingPosition: startingPosition,
//			readOptions: readOptions,
//			cancellationToken: stoppingToken))
//		{
//			if (partitionEvent.Data is null)
//			{
//				continue;
//			}

//			var dto = JsonSerializer.Deserialize<StockTickDto>(partitionEvent.Data.EventBody.ToMemory().Span);
//			if (dto is not null)
//			{
//				await writer.WriteAsync(dto, stoppingToken);
//			}
//		}
//	}

//	private async Task FlushChannelAsync(
//		ChannelReader<StockTickDto> reader,
//		CancellationToken stoppingToken)
//	{
//		var batch = new List<StockTickDto>(BatchSize);
//		using var timer = new PeriodicTimer(FlushInterval);

//		while (!stoppingToken.IsCancellationRequested)
//		{
//			while (batch.Count < BatchSize && reader.TryRead(out var item))
//			{
//				batch.Add(item);
//			}

//			if (batch.Count >= BatchSize)
//			{
//				await PersistBatchAsync(batch, stoppingToken);
//				batch.Clear();
//				continue;
//			}

//			var waitForItem = reader.WaitToReadAsync(stoppingToken).AsTask();
//			var waitForTick = timer.WaitForNextTickAsync(stoppingToken).AsTask();

//			await Task.WhenAny(waitForItem, waitForTick);

//			while (batch.Count < BatchSize && reader.TryRead(out var nextItem))
//			{
//				batch.Add(nextItem);
//			}

//			if (batch.Count > 0 && (batch.Count >= BatchSize || waitForTick.IsCompletedSuccessfully))
//			{
//				await PersistBatchAsync(batch, stoppingToken);
//				batch.Clear();
//			}
//		}

//		while (reader.TryRead(out var remaining))
//		{
//			batch.Add(remaining);
//		}

//		if (batch.Count > 0)
//		{
//			await PersistBatchAsync(batch, CancellationToken.None);
//		}
//	}

//	private async Task PersistBatchAsync(List<StockTickDto> batch, CancellationToken cancellationToken)
//	{
//		using var scope = scopeFactory.CreateScope();
//		var repository = scope.ServiceProvider.GetRequiredService<StockTickRepository>();

//		await repository.AddTickAsync(batch, cancellationToken);

//		logger.LogInformation("Persisted {Count} stock ticks to SQL Server.", batch.Count);
//	}
//}



//public sealed class StockTickConsumerWorker(
//	EventHubConsumerClient consumerClient,
//	IServiceScopeFactory scopeFactory,
//	ILogger<StockTickConsumerWorker> logger) : BackgroundService
//{
//	private const int BatchSize = 4;
//	private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(2);

//	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
//	{
//		var readOptions = new ReadEventOptions();
//		var startingPosition = EventPosition.Latest;

//		var batch = new List<StockTickDto>(BatchSize);
//		var lastFlush = DateTimeOffset.UtcNow;

//		await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsFromPartitionAsync(
//			partitionId: "0",
//			startingPosition: startingPosition,
//			readOptions: readOptions,
//			cancellationToken: stoppingToken))
//		{
//			if (partitionEvent.Data is not null)
//			{
//				var dto = JsonSerializer.Deserialize<StockTickDto>(partitionEvent.Data.EventBody.ToMemory().Span);
//				if (dto is not null)
//				{
//					batch.Add(dto);
//				}
//			}

//			bool batchFull = batch.Count >= BatchSize;
//			bool intervalElapsed = batch.Count > 0 && (DateTimeOffset.UtcNow - lastFlush) >= FlushInterval;

//			if (batchFull || intervalElapsed)
//			{
//				await PersistBatchAsync(batch, stoppingToken);
//				batch.Clear();
//				lastFlush = DateTimeOffset.UtcNow;
//			}
//		}

//		if (batch.Count > 0)
//		{
//			await PersistBatchAsync(batch, CancellationToken.None);
//		}
//	}

//	private async Task PersistBatchAsync(List<StockTickDto> batch, CancellationToken cancellationToken)
//	{
//		using var scope = scopeFactory.CreateScope();
//		var repository = scope.ServiceProvider.GetRequiredService<StockTickRepository>();

//		await repository.AddTickAsync(batch, cancellationToken);

//		logger.LogInformation(
//			"Persisted {Count} stock ticks to SQL Server.",
//			batch.Count);
//	}
//}