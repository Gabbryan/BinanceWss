from src.libs.utils.pandas.io.csv_handler import CSVHandler
from src.libs.utils.pandas.io.excel_handler import ExcelHandler
from src.libs.utils.pandas.io.json_handler import JSONHandler
from src.libs.utils.pandas.io.parquet_handler import ParquetHandler


class IOController:
    """
    Unified interface for handling various I/O operations such as CSV, JSON, Excel, and Parquet.
    """

    def __init__(self):
        self.handlers = {
            'csv': CSVHandler(),
            'json': JSONHandler(),
            'excel': ExcelHandler(),
            'parquet': ParquetHandler()  # Added ParquetHandler
        }

    def load(self, file_path, format):
        """
        Load a DataFrame from a file using the appropriate handler.

        :param file_path: Path to the file.
        :param format: Format of the file (csv, json, excel, parquet).
        :return: DataFrame loaded from the file.
        """
        if format in self.handlers:
            return self.handlers[format].load(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def save(self, df, file_path, format):
        """
        Save a DataFrame to a file using the appropriate handler.

        :param df: DataFrame to save.
        :param file_path: Path to save the file.
        :param format: Format to save the file (csv, json, excel, parquet).
        """
        if format in self.handlers:
            self.handlers[format].save(df, file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
