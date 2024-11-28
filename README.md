![workflow](https://github.com/Trustia-labs/Cicada-binance/actions/workflows/python-package.yml/badge.svg)

# WebSocket Client for Binance

This project includes a Python script (`ws.py`) that connects to the Binance WebSocket API to subscribe to various streams for specified cryptocurrency symbols. It processes and stores incoming WebSocket messages in JSON files, organized by event type and symbol.

## Features

- Subscribes to multiple Binance WebSocket streams for configured symbols.
- Processes and stores incoming messages in a structured format.
- Supports concurrent WebSocket connections using threading.
- Logs application activity for troubleshooting and monitoring.

## Requirements

- Python 3.9 or higher
- `websocket-client` package
- Docker (optional for containerized deployment)

## Setup

### Local Python Environment

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd your_project_directory
   ```

2. Install the required Python packages:

   ```bash
   pip install -r requirements-binance-api.txt
   ```

3. Update the `config.json` file with the symbols and streams you wish to subscribe to.

4. Run the script:

   ```bash
   python main_refactor.py
   ```

### Docker Deployment

1. Build the Docker image:

   ```bash
   docker-compose up --build -d
   ```

2. To stop and remove the containers created by Docker Compose:

   ```bash
   docker-compose down
   ```

## Configuration

### Manual Configuration

**Symbols and streams** can be manually configured in the `config.json` file. Update this file to change the subscription parameters. Below is an example of how to configure the `config.json` file:

```json
{
  "symbols": {
    "BTCUSDT": {
      "streams": [
        "kline_1m",
        "kline_3m",
        "markPrice",
        "aggTrades",
        "ticker",
        "bookTicker",
        "depth@100ms"
      ]
    },
    "ETHUSDT": {
      "streams": ["kline_1m", "kline_3m", "markPrice", "aggTrades"]
    },
    "SOLUSDT": {
      "streams": ["kline_1m", "ticker"]
    },
    "BNBUSDT": {
      "streams": ["kline_3m", "bookTicker"]
    },
    "LINKUSDT": {
      "streams": ["kline_1m", "depth@100ms"]
    }
  }
}
```

### Dynamic Configuration

To dynamically generate the configuration for the top 50 cryptocurrencies by volume on Binance, use the CryptoConfigGenerator class included in the project. This approach ensures that the symbols and streams in your configuration are always up to date with the market.

### Using CryptoConfigGenerator

Ensure you have the requests library installed in your Python environment.
Instantiate the CryptoConfigGenerator class and call the get_top_50_cryptos_config method. This method fetches the current top 50 cryptocurrencies by volume from Binance and generates a configuration object.

Example usage:

```
from crypto_config_generator import CryptoConfigGenerator

generator = CryptoConfigGenerator()
config = generator.get_top_50_cryptos_config()
print(config)

```

This dynamically generated configuration can then be used directly in your application or saved to a config.json file.

## Data Storage

- Incoming WebSocket messages are stored in the `json_messages` directory, organized by event type and symbol.
- Log files are generated in the `logs` directory, capturing the application's operational activity.

## Troubleshooting

- Check the log files in the `logs` directory for errors or warnings.
- Ensure all Python dependencies are installed as per `requirements.txt`.
- For Docker deployments, ensure Docker is correctly installed and running on your system.

## Contributing

- Contributions to this project are welcome. Please create a pull request or open an issue for bugs and feature requests.
