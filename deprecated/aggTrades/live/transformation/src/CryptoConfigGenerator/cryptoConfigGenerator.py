import requests


class CryptoConfigGenerator:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3/ticker/24hr"

    def fetch_market_data(self):
        try:
            response = requests.get(self.base_url, timeout=5)
            cryptos = response.json()
        except Exception as e:
            print(f"Error fetching data from Binance: {e}")
            return []
        return cryptos

    def sort_cryptos(self, cryptos, metrics, sort_orders):
        if len(metrics) != len(sort_orders):
            raise ValueError("Length of metrics and sort_orders should be equal.")
        # Sorting by multiple metrics using the tuple ordering feature in Python's sort
        return sorted(
            cryptos,
            key=lambda x: tuple(
                float(x[metric]) if metric in x else 0 for metric in metrics
            ),
            reverse=True,
        )

    def generate_config(self, sorted_cryptos, num_cryptos=50):
        streams = [
            "kline_1m",
            "kline_3m",
            "markPrice",
            "aggTrades",
            "ticker",
            "bookTicker",
            "depth@100ms",
        ]

        config = {"symbols": {}}
        count = 0  # Count of cryptos added to the config

        # Iterate over sorted cryptos and add if they match criteria until we reach num_cryptos
        for crypto in sorted_cryptos:
            if count >= num_cryptos:
                break  # Break if we've reached the desired number of cryptos
            symbol = crypto["symbol"]
            if symbol.endswith("USDT") and not symbol.startswith("USDC"):
                config["symbols"][symbol] = {"streams": streams}
                count += 1

        return config

    def get_custom_sorted_cryptos_config(self, metrics, sort_orders, num_cryptos=50):
        market_data = self.fetch_market_data()
        sorted_cryptos = self.sort_cryptos(market_data, metrics, sort_orders)
        config = self.generate_config(sorted_cryptos, num_cryptos)
        return config
