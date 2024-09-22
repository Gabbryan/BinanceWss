def data_exists(existing_data, new_data):
    """
    Vérifie si les nouvelles données existent déjà dans les données existantes.
    Cette fonction peut être étendue pour gérer différents types de structures JSON.
    """
    if isinstance(new_data, dict):
        return any(new_data == existing_item for existing_item in existing_data)
    elif isinstance(new_data, list):
        # Exemple pour des listes simples. Peut nécessiter un ajustement pour des listes de dictionnaires.
        return new_data in existing_data
    else:
        return new_data in existing_data
