import numpy as np
import pandas as pd
from scipy import stats

from src.commons.logs.logging_controller import LoggingController


class DataFrameVerificationController:
    def __init__(self, required_columns=None, data_type_checks=None, schema=None, merge_mapping=None):
        self.required_columns = required_columns if required_columns else []
        self.data_type_checks = data_type_checks if data_type_checks else {}
        self.schema = schema if schema else {}
        self.merge_mapping = merge_mapping if merge_mapping else {}
        self.logger = LoggingController("DataFrame Verification Controller")

    def verify_and_fix_columns(self, df, context=None):
        """
        Add any missing required columns to the DataFrame, without altering existing ones.
        """
        for col in self.required_columns:
            if col not in df.columns:
                self.logger.log_info(f"Adding missing column: {col}", context)
                if col in self.data_type_checks and self.data_type_checks[col] in ['float64', 'int64']:
                    df[col] = np.nan  # Use NaN for numeric columns
                else:
                    df[col] = None  # Use None for non-numeric columns
        return df

    def verify_and_fix_data_types(self, df, context=None):
        for col, expected_type in self.data_type_checks.items():
            if col in df.columns:
                if expected_type == 'int64' and df[col].isnull().any():
                    df[col] = df[col].fillna(0)
                elif pd.api.types.is_numeric_dtype(df[col]) and expected_type == 'float64':
                    df[col] = df[col].fillna(np.nan)
                elif pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].fillna('')
                try:
                    df[col] = df[col].astype(expected_type)
                except ValueError as e:
                    self.logger.log_error(f"Error converting {col} to {expected_type}: {e}", context)
                except TypeError as e:
                    self.logger.log_error(f"Type error while converting {col} to {expected_type}: {e}", context)
        return df

    def convert_dates_to_timestamps(self, df, context=None):
        """
        Convert all detected date columns to Unix timestamps, handle timezones, and rename them to 'timestamp' format.
        """
        # Automatically detect columns that contain 'date' and convert them
        date_columns = [col for col in df.columns if 'date' in col.lower()]

        renamed_columns = {}

        for col in date_columns:
            try:
                # Convert to datetime, handle invalid dates, and normalize to UTC
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

                # Convert to Unix timestamps (ensure no scientific notation)
                df[col] = df[col].apply(lambda x: int(x.timestamp()) if not pd.isnull(x) else np.nan)

                # Rename the column from 'date' to 'timestamp'
                new_col_name = col.replace("date", "timestamp").replace("Date", "Timestamp")
                renamed_columns[col] = new_col_name
            except Exception as e:
                self.logger.log_error(f"Error converting {col} to timestamp: {e}", context)

        # Apply the renaming
        df.rename(columns=renamed_columns, inplace=True)
        return df

    def merge_columns(self, df, context=None):
        """
        Merge columns based on the merge_mapping, ensuring no empty concatenation occurs.
        """
        for required_col, existing_col in self.merge_mapping.items():
            if existing_col in df.columns:
                self.logger.log_info(f"Merging column '{existing_col}' into '{required_col}'", context)
                if required_col in df.columns:
                    # Ensure non-empty DataFrames are merged
                    df[required_col] = df[required_col].where(df[required_col].notna(), df[existing_col])
                else:
                    # Add non-empty existing column
                    df[required_col] = df[existing_col]

                # Optionally drop the existing column after merging
                df.drop(columns=[existing_col], inplace=True)
        return df

    def validate_and_transform_dataframe(self, df, clean_data=False, fill_na_value=None, handle_outliers=False, context=None):
        df = self.verify_and_fix_columns(df, context)
        df = self.merge_columns(df, context)  # Merge columns if needed
        df = self.verify_and_fix_data_types(df, context)
        df = self.convert_dates_to_timestamps(df, context)
        if clean_data:
            df = self.clean_data(df, fill_na_value=fill_na_value, handle_outliers=handle_outliers, context=context)
        return df

    def clean_data(self, df, remove_duplicates=True, fill_na_value=None, handle_outliers=False, context=None):
        if remove_duplicates:
            before = len(df)
            df = df.drop_duplicates()
            after = len(df)
            self.logger.log_info(f"Removed {before - after} duplicate rows.", context)
        if fill_na_value is not None:
            df.fillna(fill_na_value, inplace=True)
            self.logger.log_info(f"Filled missing values with {fill_na_value}.", context)
        if handle_outliers:
            df = df[(np.abs(stats.zscore(df.select_dtypes(include=[np.number]))) < 3).all(axis=1)]
            self.logger.log_info("Handled outliers using Z-score method.", context)
        return df


# Test the DataFrameVerificationController with schema validation and strict column matching
if __name__ == "__main__":
    context = {"user": "test_user", "action": "data_verification"}

    # Define multiple test DataFrames with various issues
    df1 = pd.DataFrame({
        'Open': [1, 2, 3],
        'High': [1, 2, 3],
        'Low': [0.9, 1.9, 2.9],
        'Close': [1.1, 2.1, 3.1],
        'Volume': [100, 200, 300]
    })

    df2 = pd.DataFrame({
        'Open': [1.0, 2.0, 3.0],
        'Low': [0.9, 1.9, 2.9],
        'Extra_Column': [5, 6, 7]
    })

    df3 = pd.DataFrame({
        'Open': [1.0, 2.0, 3.0],
        'High': [1.2, 2.2, 3.2],
        'Low': [0.9, 1.9, 2.9],
        'Close': [1.1, 2.1, 3.1],
        'Volume': [100, 200, 300],
        'Date': ['2023-10-01', '2023-10-02', 'Invalid Date']
    })

    df4 = pd.DataFrame({
        'Something': [1, 2, 3],
        'Else': [10, 20, 30],
        'Another': [5, 6, 7]
    })

    df5 = pd.DataFrame({
        'Open': [1.0, 2.0, 3.0],
        'High': [1.2, 2.2, 3.2],
        'Low': [0.9, 1.9, 2.9],
        'Close': [1.1, 2.1, 3.1],
        'Volume': [100, 200, 300],
        'Date': ['2023-10-01', '2023-10-02', 'Invalid Date'],  # 'Invalid Date' should be handled
        'Created_date': ['2023-10-01', '2023-10-02', '2023-10-03']  # Additional date column to be renamed
    })

    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Date']
    data_type_checks = {'Open': 'float64', 'High': 'float64', 'Close': 'float64', 'Volume': 'int64'}
    merge_mapping = {'Open': 'Something', 'High': 'Else'}

    df_verification_controller = DataFrameVerificationController(
        required_columns=required_columns,
        data_type_checks=data_type_checks,
        merge_mapping=merge_mapping
    )
    # Force pandas to display the full integer format for timestamps
    pd.set_option('display.float_format', '{:.0f}'.format)


# Test 1: DataFrame with correct columns but incorrect data types
    df1 = df_verification_controller.validate_and_transform_dataframe(df1, clean_data=True, context=context)
    print(df1)

    # Test 2: DataFrame with missing and extra columns
    df2 = df_verification_controller.validate_and_transform_dataframe(df2, clean_data=True, context=context)
    print(df2)

    # Test 3: DataFrame with date column to be converted to timestamp
    df3 = df_verification_controller.validate_and_transform_dataframe(df3, clean_data=True, context=context)
    print(df3)

    # Test 4: Completely missing required columns
    df4 = df_verification_controller.validate_and_transform_dataframe(df4, clean_data=True, context=context)
    print(df4)

    df5 = df_verification_controller.validate_and_transform_dataframe(df5, clean_data=True, context=context)
    print(df5)
