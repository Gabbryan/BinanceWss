import zipfile
from io import BytesIO

import pandas as pd

from src.commons.logs.logging_controller import LoggingController


class ZipController:

    def __init__(self):
        self.logger = LoggingController()

    def extract_zip_to_dataframe(self, zip_content, file_type='csv', delimiter=',', encoding='utf-8', concat_files=True):
        """
        Extract the content of a ZIP file and return one or more DataFrames.

        :param zip_content: Binary content of the ZIP file.
        :param file_type: The type of file to extract ('csv', 'excel', 'json').
        :param delimiter: Delimiter used for CSV files.
        :param encoding: Encoding of the files.
        :param concat_files: If True, concatenates files into a single DataFrame.
        :return: A DataFrame or a list of DataFrames based on concat_files.
        """
        try:
            with zipfile.ZipFile(BytesIO(zip_content)) as z:
                file_list = [name for name in z.namelist() if name.endswith(f".{file_type}")]

                if not file_list:
                    raise ValueError(f"No {file_type} files found in the archive.")

                dataframes = []
                for file_name in file_list:
                    with z.open(file_name) as file:
                        if file_type == 'csv':
                            df = pd.read_csv(file, delimiter=delimiter, encoding=encoding)
                        elif file_type == 'excel':
                            df = pd.read_excel(file)
                        elif file_type == 'json':
                            df = pd.read_json(file)
                        else:
                            raise ValueError(f"Unsupported file type: {file_type}")
                        dataframes.append(df)
                        self.logger.log_info(f"File {file_name} successfully extracted.")

                if concat_files:
                    return pd.concat(dataframes, ignore_index=True)
                else:
                    return dataframes

        except zipfile.BadZipFile as e:
            self.logger.log_error(f"Error extracting ZIP file: {e}")
            raise ValueError(f"Error extracting ZIP file: {e}")
