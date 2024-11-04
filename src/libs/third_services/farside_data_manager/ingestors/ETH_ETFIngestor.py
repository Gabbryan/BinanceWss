import pandas as pd

from ..base_ingestor import BaseIngestor
from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController("EthereumETFIngestor")

class EthereumETFIngestor(BaseIngestor):
    def __init__(self):
        """
        Initialize the EthereumETFIngestor with the ETF data URL.
        """
        super().__init__("https://farside.co.uk/ethereum-etf-flow-all-data/")
        logger.log_info("EthereumETFIngestor initialized with ETF data URL.", context={'mod': 'EthereumETFIngestor', 'action': 'Initialize'})

    def process_data(self):
        """
        Process data from the ETF page, parsing headers and rows, handling date columns, and adding a total column.
        """
        try:
            # Find the table in the page content
            table = self.soup.find("table", class_="etf")

            # Extract headers
            header_rows = table.find_all("tr", bgcolor="#eaeffa")
            headers = [header.get_text(strip=True) for header in header_rows[1].find_all("th")[:-1]]
            headers[0] = "Date"
            logger.log_info(f"Extracted headers: {headers}", context={'mod': 'EthereumETFIngestor', 'action': 'ExtractHeaders'})

            # Process table rows
            rows = []
            for row in table.find_all("tr")[3:]:  # Skip the header rows
                # Skip rows with specific styling
                if "background-color: #99ff99" in row.get("style", ""):
                    continue

                columns = row.find_all("td")
                if len(columns) == len(headers) + 1:  # Check column length
                    row_data = [column.get_text(strip=True) for column in columns[:-1]]  # Skip the first and last columns
                    rows.append(row_data)

            # Create DataFrame from parsed data
            self.data = pd.DataFrame(rows, columns=headers)
            logger.log_debug("Initial DataFrame created from parsed rows.", context={'mod': 'EthereumETFIngestor', 'action': 'CreateDataFrame'})

            # Ensure the 'Date' column is parsed correctly
            if "Date" in self.data.columns:
                self.data = self.data[self.data["Date"].str.match(r"^\d{2} \w{3} \d{4}$")]
                self.data["Date"] = pd.to_datetime(self.data["Date"], format="%d %b %Y", errors="coerce")

                # Drop rows with invalid dates
                self.data = self.data.dropna(subset=["Date"])
                logger.log_info(f"Data processed with {self.data.shape[0]} rows after date filtering and parsing.", context={'mod': 'EthereumETFIngestor', 'action': 'ProcessData'})

                # Handle parentheses indicating negative values
                for col in self.data.columns[1:]:  # Skip the 'Date' column
                    # Replace parentheses with a minus sign and remove any remaining parentheses
                    self.data[col] = self.data[col].str.replace(r'\((.*)\)', r'-\1', regex=True)
                    # Convert to numeric
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')

                # Add a 'Total' column that sums each row's numeric values
                self.data['Total'] = self.data.iloc[:, 1:].sum(axis=1)
                logger.log_info("Total column added to DataFrame.", context={'mod': 'EthereumETFIngestor', 'action': 'AddTotalColumn'})

            else:
                logger.log_error("Column 'Date' not found in DataFrame.", context={'mod': 'EthereumETFIngestor', 'action': 'MissingDateColumn'})

        except Exception as e:
            logger.log_error(f"Error processing data: {e}", context={'mod': 'EthereumETFIngestor', 'action': 'ProcessDataError'})
            raise

