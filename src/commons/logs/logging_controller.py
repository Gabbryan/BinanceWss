import logging
import os
from logging.handlers import TimedRotatingFileHandler

from src.commons.env_manager.env_controller import EnvController
from src.commons.notifications.notifications_controller import NotificationsController


class CustomFormatter(logging.Formatter):
    def format(self, record):
        # Add default values for context keys to avoid KeyError
        record.mod = getattr(record, 'mod', 'N/A')
        record.user = getattr(record, 'user', 'N/A')
        record.action = getattr(record, 'action', 'N/A')
        record.system = getattr(record, 'system', 'N/A')
        return super().format(record)

class LoggingController:
    def __init__(self, app_name):
        """
        Initialize the logging controller with log file rotation, ensuring the log directory exists.
        Prevent duplicate handlers by checking if they are already added.
        """
        # Ensure the log directory exists
        log_dir = "logs"
        log_file = os.path.join(log_dir, f"{app_name}.log")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        self.slack_controller = NotificationsController("Logging Controller")
        self.EnvController = EnvController()
        level = self.EnvController.get_yaml_config('logging', 'level')
        rotation_when = self.EnvController.get_yaml_config('logging', 'rotation_when')
        interval = self.EnvController.get_yaml_config('logging', 'interval')
        backup_count = self.EnvController.get_yaml_config('logging', 'backup_count')


        self.app_name = app_name

        # Set up the logger
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(level)

        # Prevent the logger from propagating messages to the root logger
        self.logger.propagate = False

        # Check if the logger already has handlers to avoid duplicates
        if not any(isinstance(handler, TimedRotatingFileHandler) for handler in self.logger.handlers):
            # Custom formatter with context fields
            formatter = CustomFormatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s - [mod: %(mod)s] - [user: %(user)s] - [action: %(action)s] - [system: %(system)s]'
            )

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
        self.slack_controller.send_error_notification(message, self.app_name)

    def log_critical(self, message: str, context: dict = None):
        if context:
            adapter = logging.LoggerAdapter(self.logger, context)
            adapter.critical(message)
        else:
            self.logger.critical(message)
            self.slack_controller.send_error_notification(message)
