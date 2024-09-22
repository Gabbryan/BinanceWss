import pandas as pd

df = pd.read_parquet(
    "../data-providers/Ethereum-chain/sources/ingestion-ethereum-stablecoins-IO/data.parquet"
)

print(df.head())
