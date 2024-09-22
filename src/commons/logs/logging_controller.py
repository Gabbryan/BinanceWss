import logging
from logging.handlers import TimedRotatingFileHandler


class CustomFormatter(logging.Formatter):
    def format(self, record):
        # Add default values for context keys to avoid KeyError
        record.mod = getattr(record, 'mod', 'N/A')
        record.user = getattr(record, 'user', 'N/A')
        record.action = getattr(record, 'action', 'N/A')
        record.system = getattr(record, 'system', 'N/A')
        return super().format(record)


class LoggingController:
    def __init__(self, log_file: str = 'app.log', level: int = logging.INFO, rotation_when: str = 'midnight', interval: int = 1, backup_count: int = 7):
        """
        Initialise le contrôleur de logging avec rotation des logs par défaut quotidienne.
        """
        self.logger = logging.getLogger('appLogger')
        self.logger.setLevel(level)

        # Custom formatter with context fields
        formatter = CustomFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s - [mod: %(mod)s] - [user: %(user)s] - [action: %(action)s] - [system: %(system)s]')

        # Timed Rotating File Handler for log rotation
        rotating_handler = TimedRotatingFileHandler(log_file, when=rotation_when, interval=interval, backupCount=backup_count)
        rotating_handler.setFormatter(formatter)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Adding handlers to logger
        self.logger.addHandler(rotating_handler)
        self.logger.addHandler(console_handler)

    def log_info(self, message: str, context: dict = None):
        if context:
            adapter = logging.LoggerAdapter(self.logger, context)
            adapter.info(message)
        else:
            self.logger.info(message)

    def log_debug(self, message: str, context: dict = None):
        if context:
            adapter = logging.LoggerAdapter(self.logger, context)
            adapter.debug(message)
        else:
            self.logger.debug(message)

    def log_warning(self, message: str, context: dict = None):
        if context:
            adapter = logging.LoggerAdapter(self.logger, context)
            adapter.warning(message)
        else:
            self.logger.warning(message)

    def log_error(self, message: str, context: dict = None):
        if context:
            adapter = logging.LoggerAdapter(self.logger, context)
            adapter.error(message)
        else:
            self.logger.error(message)

    def log_critical(self, message: str, context: dict = None):
        if context:
            adapter = logging.LoggerAdapter(self.logger, context)
            adapter.critical(message)
        else:
            self.logger.critical(message)


if __name__ == "__main__":
    # Daily rotation at midnight with default configuration
    logger = LoggingController(log_file='application.log', level=logging.DEBUG)

    # Logs with different levels and optional context
    logger.log_info("Application started", context={"user": "admin", "action": "login"})
    logger.log_debug("This is a debug message")
    logger.log_warning("This is a warning", context={"mod": "authentication"})
    logger.log_error("This is an error", context={"user": "guest", "action": "access_denied"})
    logger.log_critical("Critical issue occurred!", context={"system": "database", "status": "unavailable"})
