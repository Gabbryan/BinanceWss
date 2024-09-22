# Utilise une image officielle Python avec pip
FROM python:3.10-slim

RUN pip install --upgrade pip

# Installe les dépendances nécessaires à la compilation de TA-Lib
RUN apt-get update && \
    apt-get install -y build-essential wget gcc make && \
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    # Spécifie explicitement le type de build pour aarch64
    ./configure --build=aarch64-unknown-linux-gnu --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Crée un répertoire pour l'application
WORKDIR /app

# Copie le fichier requirements.txt et installe les dépendances Python
COPY src/requirements.txt /app/requirements.txt


# Installe TA-Lib pour Python
RUN pip install --no-cache-dir -r requirements.txt
#
## Met à jour numpy puis réinstalle TA-Lib
#RUN pip install --upgrade numpy && \
#    pip install --no-cache-dir --force-reinstall TA-Lib
#

# Copie le code de l'application dans le container
COPY src /app

# Expose le port de l'application FastAPI
EXPOSE 8000

# Commande pour démarrer l'application FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
