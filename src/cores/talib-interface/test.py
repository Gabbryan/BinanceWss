import json

import httpx
import yfinance as yf

# URL of your FastAPI application
FASTAPI_URL = "http://0.0.0.0:8000/compute_indicators/"


def fetch_yahoo_data(ticker):
    """
    Fetch historical stock data from Yahoo Finance.
    """
    stock_data = yf.download(ticker, period="1mo", interval="1d")
    stock_data = stock_data.reset_index()  # Reset index so that Date becomes a column
    return stock_data


def prepare_data_for_request(stock_data):
    """
    Prepare the Yahoo Finance data for the FastAPI request format.
    """
    # Convert the stock data to the format required by FastAPI
    data = {
        "Open": stock_data['Open'].tolist(),
        "High": stock_data['High'].tolist(),
        "Low": stock_data['Low'].tolist(),
        "Close": stock_data['Close'].tolist()
    }
    return data


def test_rsi_indicator():
    """
    Test the RSI indicator using Yahoo Finance data.
    """
    # Fetch data from Yahoo Finance (e.g., Apple Inc.)
    stock_data = fetch_yahoo_data('AAPL')
    data = prepare_data_for_request(stock_data)

    # Define the category and indicator for RSI
    categories = [
        {
            "name": "Momentum Indicator Functions",
            "indicators": [
                {
                    "name": "RSI",
                    "func": "RSI",
                    "args": ["Close"],
                    "params": {
                        "timeperiod": 14
                    }
                }
            ]
        }
    ]

    # Send the request to the FastAPI server
    with httpx.Client() as client:
        response = client.post(FASTAPI_URL, json={"data": data, "categories": categories})

    # Parse and print the response
    print("RSI Indicator Response:")
    print(json.dumps(response.json(), indent=4))
    assert response.status_code == 200  # Ensure the request was successful


def test_cdl2crows_pattern():
    """
    Test the CDL2CROWS candlestick pattern using Yahoo Finance data.
    """
    # Fetch data from Yahoo Finance (e.g., Apple Inc.)
    stock_data = fetch_yahoo_data('AAPL')
    data = prepare_data_for_request(stock_data)

    # Define the category and indicator for CDL2CROWS pattern
    categories = [
        {
            "name": "Pattern Recognition Functions",
            "indicators": [
                {
                    "name": "CDL2CROWS",
                    "func": "CDL2CROWS",
                    "args": ["Open", "High", "Low", "Close"],
                    "multiple": False
                }
            ]
        }
    ]

    # Send the request to the FastAPI server
    with httpx.Client() as client:
        response = client.post(FASTAPI_URL, json={"data": data, "categories": categories})

    # Parse and print the response
    print("CDL2CROWS Pattern Response:")
    print(json.dumps(response.json(), indent=4))
    assert response.status_code == 200  # Ensure the request was successful


# Running both tests
if __name__ == "__main__":
    test_rsi_indicator()
    test_cdl2crows_pattern()
