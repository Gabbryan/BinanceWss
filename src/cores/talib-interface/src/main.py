from typing import List, Dict, Any

import pandas as pd
import talib as ta
from fastapi import FastAPI
from pydantic import BaseModel

# Define the app
app = FastAPI()


# Sample data class for input
class Category(BaseModel):
    name: str
    indicators: List[Dict[str, Any]]


@app.post("/compute_indicators/")
async def compute_indicators(data: Dict[str, List[float]], categories: List[Category]):
    """
    Endpoint that computes the indicators for given categories and returns the results.

    Parameters:
    - data (dict): Dictionary containing stock data (columns like Open, High, Low, Close).
    - categories (list): List of categories with their indicators.

    Returns:
    - JSON: Computed values for each indicator.
    """

    # Convert the input data into a DataFrame
    df = pd.DataFrame(data)

    # Dictionary to store the new computed columns
    new_columns_dict = {}

    for category in categories:
        indicators = category.indicators
        for indicator in indicators:
            name = indicator['name']
            func = getattr(ta, indicator['func'], None)
            if func is None:
                return {"error": f"Indicator function {indicator['func']} not found."}

            args = indicator['args']
            params = indicator.get('params', {})
            try:
                # Prepare the arguments
                func_args = [df[arg].values for arg in args]

                # Compute the indicator
                result = func(*func_args, **params)

                # Handle multiple outputs (like MACD)
                if isinstance(result, tuple):
                    for i, res in enumerate(result):
                        col_name = f"{name}_{i}"
                        new_columns_dict[col_name] = [
                            None if pd.isna(x) or not pd.isfinite(x) else x for x in res
                        ]
                else:
                    new_columns_dict[name] = [
                        None if pd.isna(x) or not pd.isfinite(x) else x for x in result
                    ]

            except Exception as e:
                return {"error": f"Error calculating {name}: {e}"}

    return {"computed_columns": new_columns_dict}


# Sample test to check the FastAPI app
@app.get("/")
def read_root():
    return {"message": "FastAPI for technical indicator computation is running!"}
