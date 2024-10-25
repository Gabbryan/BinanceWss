import requests
from bs4 import BeautifulSoup
from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController("BaseIngestor")

class BaseIngestor:
    def __init__(self, url):
        """
        Initialize the BaseIngestor with the provided URL.
        """
        self.url = url
        self.data = None
        logger.log_info(f"BaseIngestor initialized with URL: {url}", context={'mod': 'BaseIngestor', 'action': 'Initialize'})

    def fetch_data(self):
        """
        Fetch data from the URL and parse it with BeautifulSoup.
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Raise an error for bad responses
            self.soup = BeautifulSoup(response.content, "html.parser")
            logger.log_info("Data fetched successfully from URL.", context={'mod': 'BaseIngestor', 'action': 'FetchData'})
        except requests.RequestException as e:
            logger.log_error(f"Error fetching data: {e}", context={'mod': 'BaseIngestor', 'action': 'FetchDataError'})
            raise

    def process_data(self):
        """
        Process data - should be implemented by subclasses.
        """
        logger.log_warning("process_data method called on BaseIngestor. This should be implemented by subclasses.", context={'mod': 'BaseIngestor', 'action': 'ProcessDataNotImplemented'})
        raise NotImplementedError("Subclasses should implement this method.")

    def print_data(self):
        """
        Print the ingested data if available.
        """
        if self.data is not None:
            logger.log_info("Printing ingested data.", context={'mod': 'BaseIngestor', 'action': 'PrintData'})
            print(self.data)
        else:
            logger.log_warning("No data to print.", context={'mod': 'BaseIngestor', 'action': 'NoDataToPrint'})
            print("No data to print.")
