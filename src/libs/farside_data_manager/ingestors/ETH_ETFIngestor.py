import logging

import pandas as pd

from ..base_ingestor import BaseIngestor


class EthereumETFIngestor(BaseIngestor):
    def __init__(self):
        super().__init__("https://farside.co.uk/ethereum-etf-flow-all-data/")

    def process_data(self):
        # Find the table
        table = self.soup.find("table", class_="etf")

        # Extract headers
        header_rows = table.find_all("tr", bgcolor="#eaeffa")
        headers = [
            header.get_text(strip=True) for header in header_rows[1].find_all("th")[:-1]
        ]
        headers[0] = "Date"

        # Print headers for debugging
        logging.info(f"Extracted headers: {headers}")

        # Process table rows
        rows = []
        for row in table.find_all("tr")[3:]:  # Skip the header rows
            if "background-color: #99ff99" in row.get("style", ""):
                continue

            columns = row.find_all("td")
            if len(columns) == len(headers) + 1:  # Check column length
                row_data = [
                    column.get_text(strip=True) for column in columns[:-1]
                ]  # Skip the first and last columns
                rows.append(row_data)

        # Create DataFrame
        self.data = pd.DataFrame(rows, columns=headers)

        # Print DataFrame for debugging
        logging.debug(f"Initial DataFrame:\n{self.data.head()}")

        # Ensure the 'Date' column is parsed correctly
        if "Date" in self.data.columns:
            self.data = self.data[self.data["Date"].str.match(r"^\d{2} \w{3} \d{4}$")]
            self.data["Date"] = pd.to_datetime(
                self.data["Date"], format="%d %b %Y", errors="coerce"
            )
            # Drop rows with invalid dates
            self.data = self.data.dropna(subset=["Date"])
        else:
            logging.error("Column 'Date' not found in DataFrame.")

        logging.info(
            f"Data processed with {self.data.shape[0]} rows after date filtering and parsing."
        )
