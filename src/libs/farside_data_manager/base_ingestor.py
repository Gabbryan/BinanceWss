import requests
from bs4 import BeautifulSoup


class BaseIngestor:
    def __init__(self, url):
        self.url = url
        self.data = None

    def fetch_data(self):
        response = requests.get(self.url)
        self.soup = BeautifulSoup(response.content, "html.parser")

    def process_data(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def print_data(self):
        if self.data is not None:
            print(self.data)
        else:
            print("No data to print.")
