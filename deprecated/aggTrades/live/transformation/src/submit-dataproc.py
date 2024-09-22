import os
import sys
import time
import logging
from google.cloud import dataproc_v1 as dataproc
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
CLUSTER_NAME = os.getenv("CLUSTER_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")
SCRIPT_PATH = os.getenv("SCRIPT_PATH")  # GCS path for main_dataProc.py


def submit_pyspark_job_to_dataproc(project_id, region, cluster_name, job_file_path):
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    job_details = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": job_file_path},
    }

    try:
        job = job_client.submit_job(
            project_id=project_id, region=region, job=job_details
        )
        logging.info(f"Submitted job {job.reference.job_id}")

        job_id = job.reference.job_id
        job_finished = False
        while not job_finished:
            job = job_client.get_job(project_id, region, job_id)
            if job.status.state == dataproc.types.JobStatus.ERROR:
                logging.error(f"Job {job_id} failed: {job.status.details}")
                job_finished = True
            elif job.status.state == dataproc.types.JobStatus.DONE:
                logging.info(f"Job {job_id} completed successfully.")
                job_finished = True
            else:
                logging.info(f"Job {job_id} is in state {job.status.state.name}")
            time.sleep(10)
    except Exception as e:
        logging.error(f"An error occurred while submitting job to Dataproc: {str(e)}")
        raise


if __name__ == "__main__":
    if (
        not PROJECT_ID
        or not REGION
        or not CLUSTER_NAME
        or not BUCKET_NAME
        or not SCRIPT_PATH
    ):
        logging.error(
            "Please set the environment variables: PROJECT_ID, REGION, CLUSTER_NAME, BUCKET_NAME, SCRIPT_PATH"
        )
        sys.exit(1)

    submit_pyspark_job_to_dataproc(PROJECT_ID, REGION, CLUSTER_NAME, SCRIPT_PATH)
