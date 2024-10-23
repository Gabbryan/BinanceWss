import random
import socket
import threading
from datetime import datetime
from time import time

import psutil

from src.commons.env_manager.env_controller import EnvController
from src.libs.third_services.slack.controller_slack import SlackMessageController


class NotificationsController:
    def __init__(self, service_name):
        """
        Initializes the NotificationsController with relevant context for the data micro-service.
        """
        self.EnvController = EnvController()
        self.slack_controller = SlackMessageController(self.EnvController.get_env("WEBHOOK_URL"))
        self.service_name = service_name
        self.motivational_quotes = [
            "Great things take time! 🌱",
            "Keep up the awesome work! 💪",
            "Every step forward counts! 🚀",
            "Together, we achieve more! 🎯",
            "Success is just around the corner! 🌟"
        ]
        self.process_start_time = None
        self.env = self.EnvController.env

    def _get_motivational_quote(self):
        """
        Returns a random motivational quote.
        """
        return random.choice(self.motivational_quotes)

    def _get_current_datetime(self):
        """
        Returns the current date and time in a human-readable format.
        """
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _get_hostname(self):
        """
        Returns the machine hostname for more context on where the process is running.
        """
        return socket.gethostname()

    def _get_process_id(self):
        """
        Generates a unique process ID based on the service name and the current timestamp.
        """
        return f"{self.service_name}-{int(time())}"

    def _get_system_stats(self):
        """
        Returns the current CPU and memory usage as a dictionary.
        """
        cpu_usage = psutil.cpu_percent()
        memory_info = psutil.virtual_memory()
        memory_usage = memory_info.percent
        return {"cpu": cpu_usage, "memory": memory_usage}

    def _get_thread_count(self):
        """
        Returns the number of active threads in the current process.
        """
        return threading.active_count()

    def _get_disk_usage(self):
        """
        Returns the current disk usage as a percentage.
        """
        disk_usage = psutil.disk_usage('/')
        return disk_usage.percent

    def _get_system_uptime(self):
        """
        Returns the system uptime in a human-readable format (hours and minutes).
        """
        uptime_seconds = time() - psutil.boot_time()
        uptime_hours, remainder = divmod(uptime_seconds, 3600)
        uptime_minutes, _ = divmod(remainder, 60)
        return f"{int(uptime_hours)}h {int(uptime_minutes)}m"

    def _perform_health_check(self):
        """
        Performs a simple health check, returning a boolean to indicate if the system is healthy.
        """
        system_stats = self._get_system_stats()
        return system_stats['cpu'] <= 90 and system_stats['memory'] <= 90

    def send_process_start_message(self):
        """
        Sends a Slack notification when the data process starts, including service name, machine hostname,
        start time, system resource usage, and a motivational quote.
        """
        self.process_start_time = time()
        process_id = self._get_process_id()
        start_time = self._get_current_datetime()
        hostname = self._get_hostname()

        system_stats = self._get_system_stats()
        thread_count = self._get_thread_count()
        disk_usage = self._get_disk_usage()
        uptime = self._get_system_uptime()
        health_status = "Healthy ✅" if self._perform_health_check() else "Warning ⚠️ High CPU or Memory Usage"
        quote = self._get_motivational_quote()

        message = (
            f"🚀 *{self.service_name} ({self.env})* processing has started!\n"
            f"🔧 *Process ID*: `{process_id}`\n"
            f"🕒 *Start Time*: {start_time}\n"
            f"💻 *Host*: `{hostname}`\n"
            f"💽 *CPU Usage*: {system_stats['cpu']}%\n"
            f"🧠 *Memory Usage*: {system_stats['memory']}%\n"
            f"🔄 *Threads*: {thread_count}\n"
            f"💾 *Disk Usage*: {disk_usage}%\n"
            f"🖥️ *System Uptime*: {uptime}\n"
            f"🩺 *System Health*: {health_status}\n"
            f"💬 _\"{quote}\"_"
        )
        self.slack_controller.send_slack_message(f"{self.service_name} ({self.env}) Process Started 🚀", message, "#36a64f")

    def send_process_end_message(self):
        """
        Sends a Slack notification when the data process ends, including service name, machine hostname,
        end time, total running time, and a motivational quote.
        """
        end_time = self._get_current_datetime()
        elapsed_time = time() - self.process_start_time if self.process_start_time else 0
        hostname = self._get_hostname()
        system_stats = self._get_system_stats()
        thread_count = self._get_thread_count()
        disk_usage = self._get_disk_usage()
        uptime = self._get_system_uptime()
        threshold_minutes = 30  # Set the threshold for process duration
        warning = "⚠️ *Warning*: Process duration exceeded expectations." if elapsed_time / 60 > threshold_minutes else ""
        quote = self._get_motivational_quote()

        message = (
            f"🎉 *{self.service_name} ({self.env})* processing is complete!\n"
            f"🕒 *End Time*: {end_time}\n"
            f"⏳ *Duration*: {elapsed_time / 60:.2f} minutes\n"
            f"{warning}\n"
            f"💻 *Host*: `{hostname}`\n"
            f"💽 *CPU Usage*: {system_stats['cpu']}%\n"
            f"🧠 *Memory Usage*: {system_stats['memory']}%\n"
            f"🔄 *Threads*: {thread_count}\n"
            f"💾 *Disk Usage*: {disk_usage}%\n"
            f"🖥️ *System Uptime*: {uptime}\n"
            f"💬 _\"{quote}\"_"
        )
        self.slack_controller.send_slack_message(f"{self.service_name} ({self.env}) Process Complete 🎉", message, "#36a64f")

    def send_error_notification(self, error_message, app_name):
        """
        Sends a Slack notification if the process encounters an error, including the service name and machine hostname.

        :param error_message: The error message to be sent.
        :param app_name: The name of the application.
        """
        hostname = self._get_hostname()
        message = (
            f"⚠️ *{app_name}* encountered an error on `{hostname}`:\n"
            f"```{error_message}```\n"
            "But remember, every setback is a setup for a comeback! 💪"
        )
        self.slack_controller.send_slack_message(f"{app_name} ({self.env}) Error ⚠️", message, "#ff0000")
