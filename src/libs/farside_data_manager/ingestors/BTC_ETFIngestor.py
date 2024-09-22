import logging

import pandas as pd

from ..base_ingestor import BaseIngestor


class BitcoinETFIngestor(BaseIngestor):
    def __init__(self):
        super().__init__("https://farside.co.uk/bitcoin-etf-flow-all-data/")

    def process_data(self):
        table = self.soup.find("table", class_="etf")

        headers = [header.get_text(strip=True) for header in table.find_all("th")]

        rows = []
        for row in table.find_all("tr")[1:]:  # Skip the header row
            if "background-color: #99ff99" in row.get("style", ""):
                continue

            columns = row.find_all("td")
            if len(columns) == len(headers):
                row_data = [column.get_text(strip=True) for column in columns]
                rows.append(row_data)

        self.data = pd.DataFrame(rows, columns=headers)

        # Ensure the 'Date' column is parsed correctly
        self.data = self.data[
            self.data["Date"].str.match(r"^\d{2} \w{3} \d{4}$")
        ]  # Keep rows with valid date format
        self.data["Date"] = pd.to_datetime(
            self.data["Date"], format="%d %b %Y", errors="coerce"
        )

        # Drop rows with invalid dates
        self.data = self.data.dropna(subset=["Date"])

        logging.info(
            f"Data processed with {self.data.shape[0]} rows after date filtering and parsing."
        )
