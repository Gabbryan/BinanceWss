class TaskProcessor:
    def __init__(self, period_minutes):
        self.period_minutes = period_minutes
        self.tasks = self.initialize_tasks(period_minutes)

    def initialize_tasks(self, period_minutes):
        """
        Initialise et ajuste les tâches basées sur le periodMinutes donné.
        """
        tasks = [
            (
                "post",
                "/bapi/futures/v1/public/future/data/global-long-short-account-ratio",
                {"name": "BTCUSDT"},
                "global_long_short_account_ratio",
            ),
            (
                "post",
                "/bapi/futures/v1/public/future/data/open-interest-stats",
                {"name": "BTCUSDT"},
                "open_interest_stats",
            ),
            # Liste complète des tâches ici ...
        ]

        # Ajuster les tâches qui nécessitent un 'periodMinutes'
        adjusted_tasks = []
        for method, endpoint, params, name in tasks:
            if (
                params is not None and "name" in params
            ):  # Vérifie si le paramètre est présent
                params["periodMinutes"] = period_minutes  # Ajuste periodMinutes
            adjusted_tasks.append((method, endpoint, params, name))

        return adjusted_tasks

    # Exemple de méthode pour exécuter les tâches. Ceci doit être adapté à votre logique d'application spécifique.
    def execute_tasks(self):
        for method, endpoint, params, name in self.tasks:
            # Votre logique d'exécution ici, par exemple en faisant des requêtes HTTP
            print(f"Executing {method} {endpoint} with params {params} named {name}")
            # Ici, vous pourriez utiliser requests.post ou requests.get en fonction de la méthode.


period_minutes = 60
processor = TaskProcessor(period_minutes)
processor.execute_tasks()
