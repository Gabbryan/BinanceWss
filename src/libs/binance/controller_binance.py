import requests


class BinanceServer():
    def __init__(self):
        self.url = "https://api.binance.com/api/v3/ticker/24hr"
        self.headers = {
            "X-MBX-APIKEY": "YOUR_API_KEY",
            "Content-Type": "application/json"
        }

    def connect(self):
        response = requests.get(self.url, headers=self.headers)
        return response.json()
