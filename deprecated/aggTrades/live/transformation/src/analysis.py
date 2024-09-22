import asyncio
import logging
import os

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from google.cloud import bigtable
from google.cloud.bigtable.row import DirectRow
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

client = bigtable.Client(project=os.getenv("BIGTABLE_PROJECT_ID"), admin=True)
instance = client.instance(os.getenv("BIGTABLE_INSTANCE_ID"))
table = instance.table(os.getenv("BIGTABLE_TABLE_ID"))

app = FastAPI()


class DataRequest(BaseModel):
    symbol: str


def read_from_bigtable(symbol: str):
    start_key = f"{symbol}#".encode()
    end_key = f"{symbol}#{chr(255)}".encode()
    rows = table.read_rows(start_key=start_key, end_key=end_key)

    data = []
    for row in rows:
        row_data = {}
        for column in ["timestamp", "price", "quantity", "is_buyer_market"]:
            try:
                cell = row.cell_value("cf1", column.encode())
                if cell is not None:
                    cell_value = cell.decode("utf-8")
                    if column == "timestamp":
                        row_data[column] = int(cell_value)
                    elif column in ["price", "quantity"]:
                        row_data[column] = float(cell_value)
                    elif column == "is_buyer_market":
                        row_data[column] = cell_value.lower() == "true"
                else:
                    row_data[column] = None
            except KeyError:
                row_data[column] = None
        data.append(row_data)

    logging.info(f"Retrieved {len(data)} rows for {symbol}")
    return pd.DataFrame(data)


def summarize_data(df):
    if df.empty:
        return {}

    summary = {
        "total_trades": int(len(df)),
        "total_volume": float(df["quantity"].sum()),
        "total_value": float((df["price"] * df["quantity"]).sum()),
        "average_price": float(df["price"].mean()),
        "average_quantity": float(df["quantity"].mean()),
        "max_price": float(df["price"].max()),
        "min_price": float(df["price"].min()),
        "buyer_market_trades": int(df[df["is_buyer_market"] == True].shape[0]),
        "seller_market_trades": int(df[df["is_buyer_market"] == False].shape[0]),
    }

    return summary


@app.post("/get_data")
async def get_data(data_request: DataRequest):
    df = read_from_bigtable(data_request.symbol)
    logging.info(f"DataFrame head for {data_request.symbol}: \n{df.head(10)}")

    if df.empty:
        raise HTTPException(
            status_code=400, detail="No data found for the given symbol"
        )

    summary = summarize_data(df)
    logging.info(f"Summary for {data_request.symbol}: {summary}")
    return {"status": "success", "symbol": data_request.symbol, "summary": summary}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/check_data")
async def check_data():
    rows = table.read_rows(limit=1)
    return (
        {"status": "success", "message": "Data found in Bigtable"}
        if next(rows, None)
        else {"status": "error", "message": "No data found in Bigtable"}
    )


async def delete_all_rows():
    if table.exists():
        rows_to_delete = []
        partial_rows = table.read_rows()
        for row in partial_rows:
            row_key = row.row_key
            rows_to_delete.append(row_key)

        for row_key in rows_to_delete:
            row = table.direct_row(row_key)
            row.delete()
            table.mutate_rows([row])

        logging.info(f"All rows in table {table.table_id} deleted.")
    else:
        logging.error("Table not found")


async def main():
    logging.info("Service is starting...")

    # sawait delete_all_rows()

    logging.info("Starting FastAPI server...")
    import uvicorn

    config = uvicorn.Config(app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    finally:
        logging.info("Service has been shut down")
