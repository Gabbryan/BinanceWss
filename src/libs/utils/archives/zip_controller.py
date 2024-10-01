import zipfile
import pandas as pd
from io import BytesIO

class ZipController:
    
    @staticmethod
    def extract_zip_to_dataframe(zip_content):
        """
        Extrait le contenu d'un fichier ZIP et retourne un DataFrame.
        :param zip_content: contenu binaire du fichier ZIP.
        :return: DataFrame
        """
        try:
            with zipfile.ZipFile(BytesIO(zip_content)) as z:
                with z.open(z.namelist()[0]) as file:
                    df = pd.read_csv(file)
            return df
        except zipfile.BadZipFile as e:
            raise ValueError(f"Erreur lors de l'extraction du fichier ZIP : {e}")
