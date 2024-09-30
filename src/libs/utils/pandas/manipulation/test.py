import pandas as pd

from src.libs.utils.pandas.manipulation.advanced_transformations import AdvancedTransformations
from src.libs.utils.pandas.manipulation.column_operations import ColumnOperations
from src.libs.utils.pandas.manipulation.dataframe_operations import DataFrameOperations
from src.libs.utils.pandas.manipulation.date_operations import DateOperations
from src.libs.utils.pandas.manipulation.row_operations import RowOperations

# Sample DataFrame for testing
data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [25, 30, 35], 'Date': ['2021-01-01', '2021-02-01', '2021-03-01']}
df = pd.DataFrame(data)

# Initialize DataFrame Operations
df_ops = DataFrameOperations(df)
df_filtered = df_ops.filter_rows('Age', 30)
print(df_filtered)

# Column Operations
col_ops = ColumnOperations(df)
df_renamed = col_ops.rename_column('Age', 'Years')
print(df_renamed)

# Row Operations
row_ops = RowOperations(df)
df_row_filtered = row_ops.filter_rows_by_condition(df['Age'] > 25)
print(df_row_filtered)

# Date Operations
date_ops = DateOperations(df)
df_parsed = date_ops.parse_dates('Date')
print(df_parsed)

# Advanced Transformations
adv_ops = AdvancedTransformations(df)
df_melted = adv_ops.melt(id_vars=['Name'], value_vars=['Age'])
print(df_melted)
