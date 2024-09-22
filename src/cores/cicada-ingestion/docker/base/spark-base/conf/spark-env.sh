#!/usr/bin/env bash

export SPARK_MASTER_HOST=spark-master
export SPARK_WORKER_MEMORY=20g    # Memory allocated for each worker
export SPARK_WORKER_CORES=4       # Cores allocated for each worker
export SPARK_WORKER_INSTANCES=2   # Number of worker instances per node
