import pandas as pd

from src.libs.utils.pandas.analytics.advanced_stats import AdvancedStats
from src.libs.utils.pandas.analytics.missing_data import MissingData
from src.libs.utils.pandas.analytics.summary_operations import SummaryOperations
from src.libs.utils.pandas.analytics.time_series_operations import TimeSeriesOperations

# Sample DataFrame for testing
data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [25, 30, 35], 'Score': [88, None, 92], 'Date': pd.date_range('2021-01-01', periods=3, freq='D')}
df = pd.DataFrame(data)

# Summary Operations
summary_ops = SummaryOperations(df)
summary = summary_ops.get_summary()
print(summary)

# Advanced Stats
adv_stats = AdvancedStats(df)
correlation_matrix = adv_stats.correlation()
print(correlation_matrix)

# Missing Data Operations
missing_data_ops = MissingData(df)
missing_detected = missing_data_ops.detect_missing()
print(missing_detected)
df_filled = missing_data_ops.fill_missing()
print(df_filled)

# Time Series Operations
df['Date'] = pd.to_datetime(df['Date'])
df.set_index('Date', inplace=True)
ts_ops = TimeSeriesOperations(df)
rolling_avg = ts_ops.rolling_average('Score', window=2)
print(rolling_avg)
