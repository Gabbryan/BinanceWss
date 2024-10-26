import pandas as pd

from ..base_ingestor import BaseIngestor
from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController("BitcoinETFIngestor")

class BitcoinETFIngestor(BaseIngestor):
    def __init__(self):
        """
        Initialize the BitcoinETFIngestor with the ETF data URL.
        """
        super().__init__("https://farside.co.uk/bitcoin-etf-flow-all-data/")
        logger.log_info("BitcoinETFIngestor initialized with ETF data URL.", context={'mod': 'BitcoinETFIngestor', 'action': 'Initialize'})

    def process_data(self):
        """
        Process data from the ETF page, parsing the table and converting date fields.
        """
        try:
            # Find the table in the page content
            table = self.soup.find("table", class_="etf")
            headers = [header.get_text(strip=True) for header in table.find_all("th")]

            # Parse each row and collect data
            rows = []
            for row in table.find_all("tr")[1:]:  # Skip the header row
                # Skip rows with specific styling
                if "background-color: #99ff99" in row.get("style", ""):
                    continue

                columns = row.find_all("td")
                if len(columns) == len(headers):
                    row_data = [column.get_text(strip=True) for column in columns]
                    rows.append(row_data)

            # Create DataFrame from parsed data
            self.data = pd.DataFrame(rows, columns=headers)
            logger.log_info("Data table parsed into DataFrame.", context={'mod': 'BitcoinETFIngestor', 'action': 'ParseData'})

            # Filter and parse dates
            self.data = self.data[self.data["Date"].str.match(r"^\d{2} \w{3} \d{4}$")]
            self.data["Date"] = pd.to_datetime(self.data["Date"], format="%d %b %Y", errors="coerce")

            # Drop rows with invalid dates
            self.data = self.data.dropna(subset=["Date"])
            logger.log_info(f"Data processed with {self.data.shape[0]} rows after date filtering and parsing.", context={'mod': 'BitcoinETFIngestor', 'action': 'ProcessData'})

        except Exception as e:
            logger.log_error(f"Error processing data: {e}", context={'mod': 'BitcoinETFIngestor', 'action': 'ProcessDataError'})
            raise
