import pandas as pd
from io import BytesIO


def aggregate_data(files_contents):
    # Filter out None values from files_contents
    files_contents = [content for content in files_contents if content is not None]

    # Aggregate data into a single DataFrame
    aggregated_df = pd.concat(
        [
            pd.read_csv(BytesIO(content), header=None, low_memory=False)
            for content in files_contents
        ],
        ignore_index=True,
    )

    # Ensure column names are strings
    aggregated_df.columns = aggregated_df.columns.astype(str)

    return aggregated_df
