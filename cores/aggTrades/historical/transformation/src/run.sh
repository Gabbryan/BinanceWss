#!/bin/bash

# Configuration variables
PROJECT_ID="trustia-prod"
BUCKET_NAME="production-trustia-raw-data"
ZIP_NAME="pyspark_module.zip"
ZIP_DIR="pyspark_module"
CLUSTER_NAME="cluster-a4f9"
REGION="europe-west6"
PYSPARK_SCRIPT="run_pipeline.py"
START_YEAR=2024
START_MONTH=6
END_YEAR=2024
END_MONTH=7
TIMEFRAME="4h"
CRYPTO="BTCUSDT"

# Create a zip file of the modules
echo "Creating zip file..."
zip -r $ZIP_NAME $ZIP_DIR

# Upload the zip file to Google Cloud Storage
echo "Uploading zip file to GCS..."
gsutil cp $ZIP_NAME gs://$BUCKET_NAME/

# Submit the PySpark job to Dataproc
echo "Submitting PySpark job to Dataproc..."
gcloud dataproc jobs submit pyspark $PYSPARK_SCRIPT \
    --cluster $CLUSTER_NAME \
    --region $REGION \
    --files gs://$BUCKET_NAME/$ZIP_NAME \
    --py-files gs://$BUCKET_NAME/$ZIP_NAME \
    -- \
    --start_year $START_YEAR \
    --start_month $START_MONTH \
    --end_year $END_YEAR \
    --end_month $END_MONTH \
    --timeframe $TIMEFRAME \
    --crypto $CRYPTO

# Clean up the local zip file
echo "Cleaning up local files..."
rm $ZIP_NAME

echo "Script completed successfully."
