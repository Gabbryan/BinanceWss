from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.commons.notifications.notifications_controller import NotificationsController

# Initialize the logging controller
logger = LoggingController("FarsideDataManager")

class FarsideDataManager:
    def __init__(self):
        """
        Initialize the FarsideDataManager to manage and execute ingestors.
        """
        self.ingestors = []
        self.env_manager = EnvController()
        logger.log_info("FarsideDataManager initialized.", context={'mod': 'FarsideDataManager', 'action': 'Initialize'})

    def register_ingestor(self, ingestor):
        """
        Register an ingestor to be managed by FarsideDataManager.
        """
        self.ingestors.append(ingestor)
        logger.log_info(f"Ingestor registered: {ingestor.__class__.__name__}", context={'mod': 'FarsideDataManager', 'action': 'RegisterIngestor'})

    def run(self):
        """
        Run the fetch and process operations for each registered ingestor.
        """
        for ingestor in self.ingestors:
            try:
                logger.log_info(f"Running fetch_data for {ingestor.__class__.__name__}", context={'mod': 'FarsideDataManager', 'action': 'RunFetchData'})
                ingestor.fetch_data()

                logger.log_info(f"Running process_data for {ingestor.__class__.__name__}", context={'mod': 'FarsideDataManager', 'action': 'RunProcessData'})
                ingestor.process_data()

            except Exception as e:
                logger.log_error(f"Error running ingestor {ingestor.__class__.__name__}: {e}", context={'mod': 'FarsideDataManager', 'action': 'IngestorRunError'})
