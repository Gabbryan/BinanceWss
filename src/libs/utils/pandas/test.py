# Instantiate PandasManager
from src.libs.utils.pandas.pandas_manager import PandasManager

pandas_manager = PandasManager()

# Generate a random DataFrame via DataFrameGenerator
df = pandas_manager.DataFrameGenerator.generate_random_data(rows=100, columns=5)
print(df)
