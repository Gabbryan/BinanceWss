from threading import Thread
from src.commons.logs.logging_controller import LoggingController

class ThreadController:
    """
    Manages threading for different clients in an MVC architecture.
    Responsible for starting, stopping, and managing multiple threads.
    """

    def __init__(self):
        self.threads = []
        self.logger = LoggingController("ThreadController")
        self.logger.log_info("ThreadController initialized.", context={'mod': 'ThreadController', 'action': 'Init'})

    def add_thread(self, client, method_name="process_task", *args):
        """
        Add a client to be run in its own thread.
        :param client: The client (e.g., BinanceDataVision) whose method will be invoked in the thread.
        :param method_name: The name of the method to be run in the thread (default is 'process_task').
        :param args: Arguments to pass to the method.
        """
        try:
            if hasattr(client, method_name):
                method = getattr(client, method_name)
                thread = Thread(target=method, args=args)
                self.threads.append(thread)
                self.logger.log_info(f"Thread added for {client} method '{method_name}'.", context={'mod': 'ThreadController', 'action': 'AddThread'})
            else:
                raise AttributeError(f"The client does not have a method '{method_name}'")
        except AttributeError as e:
            self.logger.log_error(f"Error adding thread: {e}", context={'mod': 'ThreadController', 'action': 'AddThreadError'})

    def start_all(self):
        """
        Start all threads.
        """
        try:
            for thread in self.threads:
                thread.start()
            self.logger.log_info("All threads started.", context={'mod': 'ThreadController', 'action': 'StartAllThreads'})
        except Exception as e:
            self.logger.log_error(f"Error starting threads: {e}", context={'mod': 'ThreadController', 'action': 'StartAllThreadsError'})

    def stop_all(self):
        """
        Join all threads (optional, for clean shutdown).
        """
        try:
            for thread in self.threads:
                thread.join()
            self.threads.clear()
            self.logger.log_info("All threads stopped and cleared.", context={'mod': 'ThreadController', 'action': 'StopAllThreads'})
        except Exception as e:
            self.logger.log_error(f"Error stopping threads: {e}", context={'mod': 'ThreadController', 'action': 'StopAllThreadsError'})
