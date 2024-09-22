import calendar


def generate_gcs_paths(symbol, start_year, end_year, start_month, end_month):
    try:
        gcs_paths = []
        for year in range(int(start_year), int(end_year) + 1):
            month_start = int(start_month) if year == int(start_year) else 1
            month_end = int(end_month) if year == int(end_year) else 12
            for month in range(month_start, month_end + 1):
                days_in_month = calendar.monthrange(year, month)[
                    1
                ]  # Get the number of days in the month
                for day in range(1, days_in_month + 1):
                    gcs_path = f"gs://production-trustia-raw-data/Raw/binance-data-vision/historical/{symbol}/futures/aggTrades/{year}/{month:02d}/{day:02d}/data.parquet"
                    gcs_paths.append((year, month, day, gcs_path))
        print(
            f"Generated {len(gcs_paths)} GCS paths for symbol '{symbol}' from {start_year}-{start_month} to {end_year}-{end_month}"
        )
        return gcs_paths
    except Exception as e:
        print(f"Failed to generate GCS paths: {e}")
        raise


gcs_paths = generate_gcs_paths("BTCUSDT", 2022, 2024, 1, 12)
