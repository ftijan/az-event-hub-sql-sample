using Microsoft.EntityFrameworkCore;

namespace Example.Common.Data;

public sealed class StockTickRepository(MarketDbContext dbContext)
{
	public async Task AddTickAsync(
		IEnumerable<StockTickDto> ticks,
		CancellationToken cancellationToken = default)
	{
		ArgumentNullException.ThrowIfNull(ticks);

		var entities = ticks
			.Select(dto => new StockTick
			{
				TickerSymbol = dto.TickerSymbol.Trim().ToUpperInvariant(),
				Price = dto.Price,
				Timestamp = dto.Timestamp
			})
			.ToList();

		if (entities.Count == 0)
		{
			return;
		}

		await dbContext.StockTicks.AddRangeAsync(entities, cancellationToken);
		await dbContext.SaveChangesAsync(cancellationToken);
	}

	public async Task<IReadOnlyList<TickerPriceSeriesDto>> GetPriceSeriesByTickerAsync(
		DateTimeOffset? since = null,
		CancellationToken cancellationToken = default)
	{
		IQueryable<StockTick> query = dbContext.StockTicks.AsNoTracking();

		if (since.HasValue)
		{
			query = query.Where(x => x.Timestamp >= since.Value);
		}

		var rows = await query
			.OrderBy(x => x.TickerSymbol)
			.ThenBy(x => x.Timestamp)
			.ThenBy(x => x.Id)
			.Select(x => new
			{
				x.TickerSymbol,
				x.Timestamp,
				x.Price
			})
			.ToListAsync(cancellationToken);

		return [.. rows
			.GroupBy(x => x.TickerSymbol)
			.Select(g => new TickerPriceSeriesDto(
				g.Key,
				[.. g.Select(p => new TimestampPricePointDto(p.Timestamp, p.Price))]))];
	}

	public async Task<TickerPriceSeriesDto?> GetSeriesByTickerAsync(
		string tickerSymbol,
		DateTimeOffset? since = null,
		int? take = null,
		CancellationToken cancellationToken = default)
	{
		var normalizedTicker = tickerSymbol.Trim().ToUpperInvariant();
		if (string.IsNullOrWhiteSpace(normalizedTicker))
		{
			return null;
		}

		var query = dbContext.StockTicks
			.AsNoTracking()
			.Where(t => t.TickerSymbol == normalizedTicker);

		if (since.HasValue)
		{
			query = query.Where(t => t.Timestamp >= since.Value);
		}

		// Order chronologically (or take latest N then re-order ascending for charting)
		query = query.OrderBy(t => t.Timestamp);

		if (take.HasValue && take.Value > 0)
		{
			query = query
				.OrderByDescending(t => t.Timestamp)
				.Take(take.Value)
				.OrderBy(t => t.Timestamp);
		}

		var points = await query
			.Select(t => new TimestampPricePointDto(t.Timestamp, t.Price))
			.ToListAsync(cancellationToken);

		if (points.Count == 0)
		{
			return null;
		}

		return new TickerPriceSeriesDto(normalizedTicker, points);
	}
}