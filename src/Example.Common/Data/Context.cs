using Microsoft.EntityFrameworkCore;

namespace Example.Common.Data;

public sealed class MarketDbContext(DbContextOptions<MarketDbContext> options) : DbContext(options)
{
	public DbSet<StockTick> StockTicks => Set<StockTick>();

	protected override void OnModelCreating(ModelBuilder modelBuilder)
	{
		modelBuilder.Entity<StockTick>(entity =>
		{
			entity.ToTable("StockTicks");
			entity.HasKey(x => x.Id);

			entity.Property(x => x.TickerSymbol)
				.HasMaxLength(16)
				.IsRequired();

			// Use decimal(18, 4) for financial prices rather than float/double
			entity.Property(x => x.Price)
				.HasPrecision(18, 4)
				.IsRequired();

			entity.Property(x => x.Timestamp)
				.IsRequired();

			// Supports queries by ticker and recent time windows
			entity.HasIndex(x => new { x.TickerSymbol, x.Timestamp });
		});
	}
}