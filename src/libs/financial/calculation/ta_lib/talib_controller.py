from typing import List, Dict, Any

import numpy as np
import pandas as pd

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
import pandas_ta as ta
# Initialize the logging controller
logger = LoggingController("TAIndicatorController")

class TAIndicatorController:

    def __init__(self, df: pd.DataFrame = None):
        """
        Initialize the controller with a DataFrame containing stock data.
        If no DataFrame is provided, a sample DataFrame will be generated for testing.
        """
        if df is None:
            self.df = self.generate_sample_data(1000)
            logger.log_info("Sample data generated with 1000 rows.", context={'mod': 'TAIndicatorController', 'action': 'GenerateSampleData'})
        else:
            self.df = df
            logger.log_info("DataFrame provided by user.", context={'mod': 'TAIndicatorController', 'action': 'LoadDataFrame'})

        # Ensure the DataFrame index is datetime and sorted
        self.df.index = pd.to_datetime(self.df.index)
        self.df = self.df.sort_index()
        logger.log_info("DataFrame index converted to datetime and sorted.", context={'mod': 'TAIndicatorController', 'action': 'IndexSort'})

    def generate_sample_data(self, rows: int = 1000) -> pd.DataFrame:
        """
        Generate a DataFrame with coherent stock data for testing.
        """
        np.random.seed(42)
        logger.log_info(f"Generating {rows} rows of sample stock data.", context={'mod': 'TAIndicatorController', 'action': 'GenerateSampleData'})

        # Generate synthetic stock prices
        base_price = 100
        prices = base_price + np.cumsum(np.random.normal(0, 1, size=rows))

        # Generate Open, High, Low, Close values
        open_prices = prices + np.random.normal(0, 0.5, size=rows)
        close_prices = prices + np.random.normal(0, 0.5, size=rows)
        high_prices = np.maximum(open_prices, close_prices) + np.random.uniform(0.5, 1, size=rows)
        low_prices = np.minimum(open_prices, close_prices) - np.random.uniform(0.5, 1, size=rows)

        # Generate random volume data
        volumes = np.random.randint(10000, 50000, size=rows)

        # Create a DataFrame with all columns and round to 4 decimal places
        data = {
            'Open': np.round(open_prices, 4),
            'High': np.round(high_prices, 4),
            'Low': np.round(low_prices, 4),
            'Close': np.round(close_prices, 4),
            'Volume': volumes
        }

        df = pd.DataFrame(data)
        logger.log_info("Sample data generation completed.", context={'mod': 'TAIndicatorController', 'action': 'SampleDataGenerated'})
        return df

    def compute_single_indicator(self, indicator_name: str, params: Dict[str, Any] = {}) -> pd.DataFrame:
        """
        Computes a single technical indicator using pandas-ta and returns the result as a DataFrame.
        """
        try:
            logger.log_info(f"Computing indicator: {indicator_name} with parameters: {params}",
                            context={'mod': 'TAIndicatorController', 'action': 'ComputeIndicator'})

            # Compute the indicator using the DataFrame's ta accessor
            result = getattr(self.df.ta, indicator_name)(**params)

            # Append the result to the DataFrame
            if isinstance(result, pd.DataFrame):
                self.df = pd.concat([self.df, result], axis=1)
            elif isinstance(result, pd.Series):
                self.df[result.name] = result
            else:
                raise ValueError(f"Unexpected result type: {type(result)}")

            logger.log_info(f"Indicator '{indicator_name}' computed successfully.", context={'mod': 'TAIndicatorController', 'action': 'IndicatorComputed'})
            return self.df
        except Exception as e:
            logger.log_error(f"Error computing indicator '{indicator_name}': {e}", context={'mod': 'TAIndicatorController', 'action': 'ComputeError'})
            raise ValueError(f"Error computing indicator '{indicator_name}': {e}")

    def compute_custom_indicators(self, custom_indicators: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Computes a custom list of indicators with specified parameters.
        Returns the DataFrame with all indicators computed.
        """
        logger.log_info(f"Computing custom indicators: {custom_indicators}", context={'mod': 'TAIndicatorController', 'action': 'ComputeCustomIndicators'})
        for indicator in custom_indicators:
            indicator_name = indicator['name']
            params = indicator.get('params', {})
            try:
                self.compute_single_indicator(indicator_name, params)
            except Exception as e:
                logger.log_warning(f"Failed to compute indicator '{indicator_name}': {e}",
                                   context={'mod': 'TAIndicatorController', 'action': 'ComputeCustomError'})
        logger.log_info("All custom indicators computed successfully.", context={'mod': 'TAIndicatorController', 'action': 'CustomIndicatorsComputed'})
        return self.df

    def get_dataframe(self) -> pd.DataFrame:
        """
        Returns the DataFrame with computed indicators.

        Returns:
        - pd.DataFrame: The DataFrame containing stock data and computed indicators.
        """
        logger.log_info("Returning the DataFrame with computed indicators.", context={'mod': 'TAIndicatorController', 'action': 'GetDataFrame'})
        return self.df

    def display_full_dataframe(self) -> None:
        """
        Temporarily changes the Pandas settings to display the entire DataFrame without truncation.
        """
        logger.log_info("Displaying the full DataFrame.", context={'mod': 'TAIndicatorController', 'action': 'DisplayDataFrame'})
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(self.df.tail())


# Example Usage
if __name__ == "__main__":
    # Instantiate the controller with the generated sample data
    controller = TAIndicatorController()
    env_controller = EnvController()

    # Compute a single indicator (e.g., Relative Strength Index - RSI)
    rsi_df = controller.compute_single_indicator('rsi', {'length': 14})
    print("RSI Indicator DataFrame:")
    controller.display_full_dataframe()

    # Compute a list of custom indicators
    custom_indicators = env_controller.get_yaml_config('Ta-lib', 'custom_indicators')
    custom_df = controller.compute_custom_indicators(custom_indicators)
    print("\nDataFrame with Custom Indicators:")
    controller.display_full_dataframe()
