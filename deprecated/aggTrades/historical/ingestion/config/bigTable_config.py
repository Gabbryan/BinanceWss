import os
from google.cloud import bigtable
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BIGTABLE_PROJECT_ID = os.getenv("BIGTABLE_PROJECT_ID")
BIGTABLE_INSTANCE_ID = os.getenv("BIGTABLE_INSTANCE_ID")
BIGTABLE_TABLE_ID = os.getenv("BIGTABLE_TABLE_ID")

bigtable_client = bigtable.Client(project=BIGTABLE_PROJECT_ID, admin=True)
