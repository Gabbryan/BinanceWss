import json
import logging
import os
import re
import uuid
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

from src.commons.env_manager.env_controller import EnvController
from src.commons.notifications.notifications_controller import NotificationsController


class JsonFormatter(logging.Formatter):
    def format(self, record):
        """
        Format log records as JSON.
        """
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": getattr(record, 'mod', 'N/A'),
            "user": getattr(record, 'user', 'N/A'),
            "action": getattr(record, 'action', 'N/A'),
            "system": getattr(record, 'system', 'N/A'),
            "correlation_id": getattr(record, 'correlation_id', 'N/A'),
        }
        return json.dumps(log_record)

class LoggingController:
    def __init__(self, app_name):
        """
        Initialize the LoggingController with JSON formatted logs, dynamic log levels,
        and Slack notifications for errors. Handles sensitive data masking and ensures
        proper log directory setup.
        """
        # Ensure the log directory exists
        log_dir = "logs"
        log_file = os.path.join(log_dir, f"{app_name}.log")
        os.makedirs(log_dir, exist_ok=True)

        self.slack_controller = NotificationsController("Logging Controller")
        self.EnvController = EnvController()
        self.app_name = app_name

        # Fetch configuration for logging
        self.level = self.EnvController.get_yaml_config('logging', 'level')
        rotation_when = self.EnvController.get_yaml_config('logging', 'rotation_when')
        interval = self.EnvController.get_yaml_config('logging', 'interval')
        backup_count = self.EnvController.get_yaml_config('logging', 'backup_count')

        # Set up the logger
        self.logger = logging.getLogger(app_name)
        self.configure_logger()

        # JSON formatter for structured logs
        json_formatter = JsonFormatter()

        # Timed Rotating File Handler for log rotation
        rotating_handler = TimedRotatingFileHandler(log_file, when=rotation_when, interval=interval, backupCount=backup_count)
        rotating_handler.setFormatter(json_formatter)

        # Console Handler for logs
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(json_formatter)

        # Add handlers to logger
        self.logger.addHandler(rotating_handler)
        self.logger.addHandler(console_handler)

    def configure_logger(self):
        """
        Configure logger based on environment (production, development, staging).
        """
        env = self.EnvController.get_env('ENV')  # Retrieve environment setting

        if env == 'production':
            self.logger.setLevel(logging.ERROR)
        elif env == 'staging':
            self.logger.setLevel(logging.WARNING)
        else:
            self.logger.setLevel(logging.DEBUG)

        self.log_info(f"Logger configured for {env} environment")

    def _log_with_context(self, level, message, context=None):
        """
        Helper method to log with or without context using the appropriate log level.
        Adds correlation IDs and masks sensitive data if needed.
        """
        if context is None:
            context = {}

        # Add correlation ID if not present
        if 'correlation_id' not in context:
            context['correlation_id'] = self._get_correlation_id()

        # Mask sensitive data
        message = self._mask_sensitive_data(message)

        # Log with the context
        adapter = logging.LoggerAdapter(self.logger, context)
        getattr(adapter, level)(message)

    def _mask_sensitive_data(self, message: str):
        """
        Redacts sensitive information from logs (e.g., API keys, passwords).
        """
        sensitive_patterns = {
            "api_key": r"(api_key=\w+)",
            "password": r"(password=\w+)"
        }

        for key, pattern in sensitive_patterns.items():
            message = re.sub(pattern, f"{key}=****", message)

        return message

    def _get_correlation_id(self):
        """
        Generate a correlation ID for traceability.
        """
        return str(uuid.uuid4())

    def set_log_level(self, level: str):
        """
        Dynamically set the log level at runtime.
        :param level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        numeric_level = getattr(logging, level.upper(), None)
        if isinstance(numeric_level, int):
            self.logger.setLevel(numeric_level)
            for handler in self.logger.handlers:
                handler.setLevel(numeric_level)
            self.log_info(f"Log level changed to {level.upper()}")
        else:
            self.log_error(f"Invalid log level: {level}")

    def log_info(self, message: str, context: dict = None):
        """
        Logs an info level message.
        """
        self._log_with_context('info', message, context)

    def log_debug(self, message: str, context: dict = None):
        """
        Logs a debug level message.
        """
        self._log_with_context('debug', message, context)

    def log_warning(self, message: str, context: dict = None):
        """
        Logs a warning level message.
        """
        self._log_with_context('warning', message, context)

    def log_error(self, message: str, context: dict = None):
        """
        Logs an error level message and sends a structured Slack notification.
        """
        self._log_with_context('error', message, context)

        # Sending structured error logs to Slack
        slack_message = {
            "service": self.app_name,
            "error_message": message,
            "time": self._get_current_datetime(),
            "context": context if context else {}
        }
        self.slack_controller.send_error_notification(json.dumps(slack_message, indent=2), self.app_name)

    def log_critical(self, message: str, context: dict = None):
        """
        Logs a critical level message and sends a structured Slack notification.
        """
        self._log_with_context('critical', message, context)

        # Sending structured critical logs to Slack
        slack_message = {
            "service": self.app_name,
            "error_message": message,
            "time": self._get_current_datetime(),
            "context": context if context else {}
        }
        self.slack_controller.send_error_notification(json.dumps(slack_message, indent=2), self.app_name)

    def _get_current_datetime(self):
        """
        Get the current datetime in human-readable format.
        """
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
