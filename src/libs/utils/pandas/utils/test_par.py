import time

import numpy as np

from src.libs.utils.pandas.utils.dataframe_generator import DataFrameGenerator
from src.libs.utils.pandas.utils.parallel import ParallelPandas

df_generator = DataFrameGenerator()


# New long-running custom function
def long_running_custom_function(molecule, factor=2, threshold=0, operation="multiply", iterations=500):
    """
    A more complex custom function that performs multiple operations on the data, with an extended execution time.

    Operations include:
    - Multiply values by a factor
    - Apply a threshold (e.g., set values below a threshold to zero)
    - Perform different operations (e.g., "multiply", "add", "subtract", "divide")
    - Includes multiple iterations of complex mathematical operations

    :param molecule: The pandas Series or DataFrame slice to process.
    :param factor: A factor to multiply, add, or divide the values by.
    :param threshold: A threshold value (e.g., all values below this threshold are set to zero).
    :param operation: The operation to apply to the values ("multiply", "add", "subtract", "divide").
    :param iterations: Number of times to perform the operation, making the function take longer.
    :return: Transformed molecule (Series or DataFrame).
    """
    # Artificially slow down the function by running expensive computations
    for _ in range(iterations):
        # Apply different operations based on the `operation` parameter
        if operation == "multiply":
            molecule = molecule * factor
        elif operation == "add":
            molecule = molecule + factor
        elif operation == "subtract":
            molecule = molecule - factor
        elif operation == "divide":
            molecule = molecule / factor
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        # Apply a non-linear transformation
        molecule = np.log1p(np.abs(molecule))  # Logarithm transformation to slow down the process

    # Apply threshold: set values below the threshold to zero
    molecule[molecule < threshold] = 0

    # Artificially simulate a time delay to slow the process down further
    time.sleep(1)
    return molecule


# Test function for running parallel operations
def run_parallel_test():
    # Initialize the ParallelPandas class
    parallel_pandas = ParallelPandas(numThreads=8, mpBatches=4, linMols=True)

    # 1. Generate a large DataFrame (e.g., 5 million rows)
    df_large = df_generator.generate_large_dataframe(num_rows=5 * 10 ** 6)
    print(f"Generated DataFrame with {len(df_large)} rows.")

    # 2. Test parallel processing on the large DataFrame using the complex function
    result = parallel_pandas.test_parallel_pandas(
        df=df_large,
        column='A',
        func=long_running_custom_function,
        factor=3 ** 3,
        threshold=0.9,  # Apply threshold
        operation="multiply",  # Operation to apply
        iterations=10 ** 3  # Number of iterations to increase processing time
    )

    # 3. Print the result summary
    print(f"Result summary:\n{result.describe()}")


# Main block to protect multiprocessing code
if __name__ == "__main__":
    run_parallel_test()
