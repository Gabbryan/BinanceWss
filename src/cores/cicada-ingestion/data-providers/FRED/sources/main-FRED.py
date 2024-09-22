import os
import time

import pandas as pd
import requests

# Remplacez 'votre_api_key' par votre clé API FRED
api_key = "9bc3e9a47cf483e7b4eeef9531d0f4f6"
base_url = "https://api.stlouisfed.org/fred/series/observations"
# Liste mise à jour des séries_id
series_ids = [
    # Produit Intérieur Brut (PIB)
    "GDP",
    "GDPC1",
    "GDPDEF",
    "RGDP",
    "RIP",
    # Prix à la Consommation
    "CPIAUCNS",
    "CPILFESL",
    "PPIACO",
    "CPIPSA",
    # Emploi et Marché du Travail
    "UNRATE",
    "U6RATE",
    "PAYEMS",
    "ICSA",
    "JOLTS",
    "MORATE",
    "OCC",
    # Production Industrielle
    "IP",
    "IPMAN",
    "IPG2211S",
    "IPG3114S",
    "IPG3131S",
    # Masse Monétaire et Réserves
    "M1SL",
    "M2SL",
    "M0SL",
    "RESBALNS",
    "M2",
    "M3",
    # Taux d'Intérêt
    "FEDFUNDS",
    "IR1420T",
    "IR3M",
    "IR1Y",
    "IR5Y",
    "DGS10",
    "DGS5",
    "DGS2",
    "DGS1",
    "DGS30",
    # Balance des Paiements et Commerce
    "BOPGSTB",
    "BOPBOT",
    "EXH2O",
    "USIMPM",
    "EXPGS",
    "IMPGS",
    "NETEXP",
    "CAD",
    # Défense et Dépenses Publiques
    "FYGFD",
    "FYGFDP",
    "GCEC1",
    "CNP",
    # Indices Boursiers et Marchés Financiers
    "SP500",
    "DJIA",
    "NASDAQ",
    "VIXCLS",
    "RUSSELL2000",
    # Consommation et Revenu
    "PCE",
    "PCED",
    "PCI",
    "PCEPI",
    "PCECTPI",
    # Emploi et Salaire
    "MSP",
    "EPO",
    "W430RC1",
    # Conditions de Crédit et Prêts
    "LOANS",
    "DTCOLNVHM",
    "DTB3",
    "DTB10",
    "DTCCL",
    "DTB4",
    "DTB6",
    # Productivité et Coûts
    "OPHPPI",
    "LPPI",
    "LPI",
    # Immobilier et Logement
    "SFRHBH",
    "HOUST",
    "SFRHPH",
    # Rendements et Inflation
    "T10YIE",
    "T5YIE",
    "T1YIE",
    # Réserves et Masse Monétaire
    "WALCL",
    "M2V",
    # Indices de Production
    "INDPRO",
    "IPMAN",
    "IPMFG",
    # Saisonnalité et Ajustements
    "SA",
    "NSA",
    # Autres Indicateurs Financiers
    "H15",
    "LIBOR",
    "OILPRICE",
]


# Fréquences à tester
frequencies = [
    "d",
    "w",
    "m",
    "q",
    "a",
]  # 'd' : daily, 'w' : weekly, 'm' : monthly, 'q' : quarterly, 'a' : annual


def fetch_data(series_id, api_key, frequency):
    params = {
        "api_key": api_key,
        "file_type": "json",
        "series_id": series_id,
        "sort_order": "asc",  # Trie les données de la plus ancienne à la plus récente
        "limit": 1000,  # Nombre maximum d'observations par page
        "frequency": frequency,
    }

    all_observations = []
    offset = 0

    while True:
        params["offset"] = offset
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            observations = data.get("observations", [])
            if not observations:
                break
            all_observations.extend(observations)
            offset += 1000
        else:
            print(
                f"Erreur lors de la récupération des données pour {series_id} avec la fréquence {frequency}: {response.status_code}"
            )
            break

        # Petite pause pour éviter les dépassements de limites d'API
        time.sleep(1)

    return all_observations


def get_data_for_series(series_id, api_key):
    data_list = []
    for frequency in frequencies:
        print(
            f"Tentative de récupération des données pour la série {series_id} avec la fréquence {frequency}"
        )
        observations = fetch_data(series_id, api_key, frequency)
        if observations:
            data_list.extend(
                {
                    "series_id": series_id,
                    "frequency": frequency,
                    "date": obs["date"],
                    "value": obs["value"],
                }
                for obs in observations
            )
            break  # Sort de la boucle si les données ont été trouvées
    return data_list


# Répertoire de sortie principal
base_output_dir = "data/series_names"

# Collecte des données et sauvegarde dans des fichiers Parquet
for series_id in series_ids:
    data_all = get_data_for_series(series_id, api_key)

    if data_all:
        # Convertir en DataFrame
        df = pd.DataFrame(data_all)

        # Déterminer la plage de dates
        min_date = df["date"].min()
        max_date = df["date"].max()
        date_range_str = f"{min_date}_{max_date}"

        # Créer le répertoire pour chaque série s'il n'existe pas
        series_output_dir = os.path.join(base_output_dir, series_id)
        os.makedirs(series_output_dir, exist_ok=True)

        # Exporter en Parquet
        parquet_file_name = f"data_{date_range_str}.parquet"
        parquet_file_path = os.path.join(series_output_dir, parquet_file_name)
        df.to_parquet(parquet_file_path, index=False)

        print(
            f"Les données pour la série {series_id} ont été exportées vers {parquet_file_path}"
        )
    else:
        print(f"Aucune donnée trouvée pour la série {series_id}")
