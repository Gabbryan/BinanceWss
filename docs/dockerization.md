# Documentation Docker

## Introduction

Ce projet utilise des Dockerfiles pour gérer les différentes parties de l'application. La base est constituée d'un fichier `Dockerfile.base` qui contient les
dépendances communes. Chaque service spécifique est ensuite configuré dans le fichier `docker-compose.yml`.

### 1. Dockerfile.base

Le fichier `Dockerfile.base` est utilisé comme base pour tous les autres Dockerfiles. Il installe les dépendances de base pour l'environnement Python et
configure le répertoire de travail.

#### Détails :

- **FROM python:3.12-slim** : Utilise l'image officielle de Python 3.12 en version slim, idéale pour les environnements légers.
- **ENV** : Définit des variables d'environnement pour éviter la génération de fichiers bytecode Python et pour désactiver le buffering dans l'output.
- **RUN** : Met à jour les paquets du système et installe des outils essentiels à la compilation de dépendances.
- **WORKDIR** : Définit le répertoire de travail où les fichiers seront copiés et exécutés.
- **COPY** : Copie les fichiers nécessaires dans le conteneur.
- **pip install** : Installe les dépendances listées dans le fichier `requirements.txt`.
- **CMD** : Commande par défaut exécutée au démarrage du conteneur.

```dockerfile
FROM python:3.12-slim

# Définir les variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Installer les dépendances de base
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers communs
COPY ./src/commons /app/src/commons
COPY ./config /app/config
COPY ./src/requirements.txt /app/requirements.txt

# Installer les dépendances
RUN pip install -r requirements.txt

# Correction pour la librairie pandas_ta
RUN sed -i 's|from numpy import NaN as npNaN|from numpy import nan as npNaN|g' $(python3 -c 'import site; print(site.getsitepackages()[0])')/pandas_ta/momentum/squeeze_pro.py

# Commande à exécuter au démarrage du container
CMD ["python"]
```

### 2. Dockerfile.dsol

Le `Dockerfile.dsol` hérite du Dockerfile.base et y ajoute les dépendances spécifiques au service `DSOL`. Ce fichier contient des instructions supplémentaires
pour copier des bibliothèques spécifiques et installer des dépendances supplémentaires.

Détails :

- **FROM base AS dsol** : Utilise Dockerfile.base comme base pour éviter la duplication de configuration.
- **COPY** : Copie les bibliothèques spécifiques dans le conteneur.
- **pip install** : Installe les bibliothèques nécessaires pour le service DSOL.

```dockerfile
FROM base AS dsol

# Copier les fichiers spécifiques à DSOL
COPY ./src/libs/ /app/src/libs/

# Installer les dépendances spécifiques à DSOL
RUN pip install /app/src/libs
```

### 3. docker-compose.yml

Le fichier `docker-compose.yml` est utilisé pour orchestrer plusieurs services. Chaque service est défini avec son propre contexte de construction (`build`),
ses volumes montés, ses variables d'environnement, et la commande à exécuter.

### Détails :

- **services** : Section où chaque service est défini.
- **build** : Décrit le contexte de construction du conteneur (dossier où se trouve le Dockerfile).
- **volumes** : Permet de monter des fichiers ou dossiers depuis l'hôte dans le conteneur. Cela est utile pour partager du code entre l'hôte et le conteneur
  sans avoir à reconstruire l'image.
- **environment** : Définit des variables d'environnement spécifiques au service.
- **command** : Définit la commande à exécuter lors du démarrage du conteneur.

Exemple d'un service transformation-binance-klines :

```yaml
services:
  transformation-binance-klines:
    build:
      context: ..
      dockerfile: ./docker/dsol.Dockerfile
    volumes:
      - ../src/cores/cicada-transformation/indicators/ta-lib/main.py:/app/main.py
      - ../src/cores/cicada-transformation/indicators/ta-lib/talib_processing_controller.py:/app/talib_processing_controller.py
    environment:
      - PYTHONUNBUFFERED=1
      - ENV=development
    command: [ "python", "main.py" ]

```

### 4. Ajouter un nouveau service

Pour ajouter un nouveau service, suivez ces étapes :

- Si votre nouveau service nécessite des dépendances ou des configurations spécifiques, créez un nouveau Dockerfile en vous basant sur `Dockerfile.base` ou
  `Dockerfile.dsol`. Par exemple, si vous avez un service nommé new-service, vous pourriez créer un fichier `Dockerfile.new-service` :

````dockerfile
FROM base AS new-service

# Copier les fichiers spécifiques
COPY ./src/new-service/ /app/src/new-service/

# Installer les dépendances spécifiques
RUN pip install /app/src/new-service/

````

- Définir le service dans `docker-compose.yml` : Ajoutez un nouveau bloc dans le fichier `docker-compose.yml` pour le service. Voici un exemple :

```yaml
new-service:
  build:
    context: ..
    dockerfile: ./docker/dsol.Dockerfile
  volumes:
    - ../src/your/new/service/file.py:/app/file.py
  environment:
    - PYTHONUNBUFFERED=1
    - ENV=development
  command: [ "python3", "file.py" ]

```

- Lancer les services avec Docker Compose : Utilisez la commande suivante pour construire et lancer les services :

```shell
docker-compose up --build

```
