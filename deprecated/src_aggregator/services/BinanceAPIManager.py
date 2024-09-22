import requests


class BinanceAPI:
    def __init__(self, base_url):
        self.base_url = base_url  # Base URL for the API endpoint

    def send_request(self, method, endpoint, data=None):
        url = f"{self.base_url}{endpoint}"  # Construct the full API endpoint URL
        headers = {"content-type": "application/json"}
        # Perform the appropriate HTTP request and return the JSON response
        if method.lower() == "post":
            response = requests.post(url, json=data, headers=headers, timeout=5)
        elif method.lower() == "get":
            response = requests.get(url, params=data, headers=headers, timeout=5)
        else:
            response = None

        response_data = response.json()
        return response_data["data"]
