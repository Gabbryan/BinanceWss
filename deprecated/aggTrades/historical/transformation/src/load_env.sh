#!/bin/bash

# Activer l'environnement Conda
source /root/anaconda3/etc/profile.d/conda.sh
conda activate myenv

# Charger les variables d'environnement depuis config.py
export $(python3 -c "
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

print(f'AWS_ACCESS_KEY_ID={Config.ACCESS_KEY}')
print(f'AWS_SECRET_ACCESS_KEY={Config.SECRET_KEY}')
")

# Exécuter spark-submit avec les variables d'environnement chargées
$SPARK_HOME/bin/spark-submit \
  --master spark://84.247.143.179:7077 \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 40g \
  --executor-cores 10 \
  --conf spark.local.dir=/tmp/spark-temp \
  --conf spark.driver.host=84.247.143.179 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
  --jars /root/spark_jars/hadoop-aws-3.2.2.jar,/root/spark_jars/aws-java-sdk-bundle-1.11.375.jar,/root/spark_jars/guava-31.0-jre.jar \
  --py-files dependencies.zip \
  --conf "spark.pyspark.python=/root/anaconda3/envs/myenv/bin/python" \
  backup.py
