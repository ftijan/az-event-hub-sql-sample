namespace Example.Common.Data;

public sealed class StockTick
{
	public long Id { get; set; }
	public required string TickerSymbol { get; set; }
	public decimal Price { get; set; }
	public DateTimeOffset Timestamp { get; set; }
}