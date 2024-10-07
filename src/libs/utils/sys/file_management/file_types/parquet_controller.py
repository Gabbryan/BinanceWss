import pandas as pd

from src.libs.utils.sys.file_management.file_manager import FileManager


class ParquetFileManager(FileManager):
    def read_file(self, file):
        """ Reads a Parquet file and returns a DataFrame """
        try:
            df = pd.read_parquet(file)
            return df
        except Exception as e:
            self.logger.log_error(f"Error reading parquet file: {e}")
            return None

    def process_data(self, df, params):
        """ Process the DataFrame according to the stream type in params """
        if df is not None:
            if params['stream'] == 'klines':
                new_column_names = ["open_time", "Open", "High", "Low", "Close", "Volume", "close_time", "quote_volume",
                                    "trades_nb", "taker_buy_volume", "taker_buy_quote_volume", "unused"]
            elif params['stream'] == 'aggTrades':
                new_column_names = ["agg_trades_id", "price", "quantity", "first_trade_id", "last_trade_id",
                                    "time", "is_buyer_maker"]
            else:
                self.logger.log_error(f"Unknown stream type: {params['stream']}")
                return None

            df.rename(columns=dict(zip(df.columns, new_column_names)), inplace=True)
            df.sort_values(by="open_time", inplace=True)
            return df
        else:
            self.logger.log_error("No data to process")
            return None
