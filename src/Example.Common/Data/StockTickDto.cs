namespace Example.Common.Data;

public sealed record StockTickDto(
	string TickerSymbol,
	decimal Price,
	DateTimeOffset Timestamp);

public sealed record TimestampPricePointDto(
	DateTimeOffset Timestamp,
	decimal Price);

public sealed record TickerPriceSeriesDto(
	string TickerSymbol,
	IReadOnlyList<TimestampPricePointDto> Points);