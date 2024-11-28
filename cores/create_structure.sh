#!/bin/bash

# Définir la structure des répertoires
directories=(
  "common/src"
  "common/microservices/example_service/src"
  "common/microservices/example_service/logs"
  "aggTrade/historical/ingestion/src"
  "aggTrade/historical/ingestion/logs"
  "aggTrade/historical/transformation/src"
  "aggTrade/historical/transformation/logs"
  "aggTrade/historical/validation/src"
  "aggTrade/historical/validation/logs"
  "aggTrade/historical/stockage/src"
  "aggTrade/historical/stockage/logs"
  "aggTrade/historical/distribution/src"
  "aggTrade/historical/distribution/logs"
  "aggTrade/live/ingestion/src"
  "aggTrade/live/ingestion/logs"
  "aggTrade/live/transformation/src"
  "aggTrade/live/transformation/logs"
  "aggTrade/live/validation/src"
  "aggTrade/live/validation/logs"
  "aggTrade/live/stockage/src"
  "aggTrade/live/stockage/logs"
  "aggTrade/live/distribution/src"
  "aggTrade/live/distribution/logs"
  "docker-compose-files/aggTrade"
  "docker-compose-files/anotherDataType"
)

# Définir la structure des fichiers
files=(
  "common/microservices/example_service/Dockerfile"
  "common/microservices/example_service/requirements.txt"
  "common/Dockerfile.base"
  "common/requirements.txt"
  "aggTrade/historical/ingestion/Dockerfile"
  "aggTrade/historical/ingestion/requirements.txt"
  "aggTrade/historical/transformation/Dockerfile"
  "aggTrade/historical/transformation/requirements.txt"
  "aggTrade/historical/validation/Dockerfile"
  "aggTrade/historical/validation/requirements.txt"
  "aggTrade/historical/stockage/Dockerfile"
  "aggTrade/historical/stockage/requirements.txt"
  "aggTrade/historical/distribution/Dockerfile"
  "aggTrade/historical/distribution/requirements.txt"
  "aggTrade/live/ingestion/Dockerfile"
  "aggTrade/live/ingestion/requirements.txt"
  "aggTrade/live/transformation/Dockerfile"
  "aggTrade/live/transformation/requirements.txt"
  "aggTrade/live/validation/Dockerfile"
  "aggTrade/live/validation/requirements.txt"
  "aggTrade/live/stockage/Dockerfile"
  "aggTrade/live/stockage/requirements.txt"
  "aggTrade/live/distribution/Dockerfile"
  "aggTrade/live/distribution/requirements.txt"
  "docker-compose-files/aggTrade/livePipeline.yml"
  "docker-compose-files/aggTrade/historicalPipeline.yml"
  "docker-compose-files/aggTrade/fullPipeline.yml"
  "docker-compose-files/anotherDataType/livePipeline.yml"
  "docker-compose-files/anotherDataType/historicalPipeline.yml"
  "docker-compose-files/anotherDataType/fullPipeline.yml"
  "docker-compose.yml"
  "requirements.txt"
)

# Créer les répertoires
for dir in "${directories[@]}"; do
  mkdir -p "$dir"
done

# Créer les fichiers
for file in "${files[@]}"; do
  touch "$file"
done
