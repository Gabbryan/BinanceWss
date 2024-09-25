from threading import Thread


class ThreadController:
    """
    Manages threading for different clients in an MVC architecture.
    Responsible for starting, stopping, and managing multiple threads.
    """

    def __init__(self):
        self.threads = []

    def add_thread(self, client, method_name="connect"):
        """
        Add a client to be run in its own thread.
        :param client: The client (e.g., BinanceWSSClient) whose method will be invoked in the thread.
        :param method_name: The name of the method to be run in the thread (default is 'connect').
        """
        if hasattr(client, method_name):
            method = getattr(client, method_name)
            thread = Thread(target=method)
            self.threads.append(thread)
        else:
            raise AttributeError(f"The client does not have a method '{method_name}'")

    def start_all(self):
        """
        Start all threads.
        """
        for thread in self.threads:
            thread.start()

    def stop_all(self):
        """
        Join all threads (optional, for clean shutdown).
        """
        for thread in self.threads:
            thread.join()
