import numpy as np
import pandas as pd


class DataFrameGenerator:
    """
    Generates synthetic or test DataFrames for testing or experimentation.
    """

    @staticmethod
    def generate_random_data(rows=100, columns=5):
        """
        Generates a DataFrame with random numerical data.
        :param rows: Number of rows.
        :param columns: Number of columns.
        :return: DataFrame with random numerical data.
        """
        data = np.random.randn(rows, columns)
        column_names = [f'col_{i}' for i in range(columns)]
        return pd.DataFrame(data, columns=column_names)

    @staticmethod
    def generate_categorical_data(rows=100, categories=None):
        """
        Generates a DataFrame with categorical data.
        :param rows: Number of rows.
        :param categories: List of categories to assign randomly to rows.
        :return: DataFrame with random categorical data.
        """
        if categories is None:
            categories = ['Category_A', 'Category_B', 'Category_C']

        data = np.random.choice(categories, size=rows)
        return pd.DataFrame({'Category': data})

    def generate_large_dataframe(self, num_rows=10 ** 6):
        """
        Generates a DataFrame with `num_rows` rows and some random numerical data.
        :param num_rows: Number of rows to generate.
        :return: Pandas DataFrame with random data.
        """
        data = {
            'A': np.random.randn(num_rows),
            'B': np.random.randint(0, 100, size=num_rows),
            'C': np.random.uniform(1.0, 10.0, size=num_rows)
        }
        df = pd.DataFrame(data)
        return df
