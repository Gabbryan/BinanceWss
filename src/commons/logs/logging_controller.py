import logging
import os
from logging.handlers import TimedRotatingFileHandler

from src.commons.notifications.notifications_slack import NotificationsSlackController


class CustomFormatter(logging.Formatter):
    def format(self, record):
        # Add default values for context keys to avoid KeyError
        record.mod = getattr(record, 'mod', 'N/A')
        record.user = getattr(record, 'user', 'N/A')
        record.action = getattr(record, 'action', 'N/A')
        record.system = getattr(record, 'system', 'N/A')
        return super().format(record)

class LoggingController:
    def __init__(self, log_file: str = 'logs/app.log', level: int = logging.INFO, rotation_when: str = 'midnight', interval: int = 1, backup_count: int = 7):
        """
        Initialize the logging controller with log file rotation, ensuring the log directory exists.
        Avoid adding duplicate handlers.
        """
        # Ensure the log directory exists
        log_dir = os.path.dirname(log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.slack_controller = NotificationsSlackController("Logger")
        # Set up logger
        self.logger = logging.getLogger('appLogger')
        self.logger.setLevel(level)

        # Prevent the logger from propagating messages to the root logger
        self.logger.propagate = False

        # Check if the logger already has handlers to avoid adding duplicates
        if not self.logger.hasHandlers():
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
        self.slack_controller.send_error_notification(message)


    def log_critical(self, message: str, context: dict = None):
        if context:
            adapter = logging.LoggerAdapter(self.logger, context)
            adapter.critical(message)
        else:
            self.logger.critical(message)
            self.slack_controller.send_error_notification(message)
