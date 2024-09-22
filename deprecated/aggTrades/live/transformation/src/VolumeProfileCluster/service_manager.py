import traceback

import requests

TIMEOUT = 30 * 60


class ServicesManager:
    def __init__(self, services, headers):
        """
        Initialize with a dictionary of cores mapping to their base URLs.
        e.g. {"service1": "http://service1.com", "service2": "http://service2.com"}
        """
        self.services = services
        self.headers = headers
        self.current_base_url = self.services.get(
            "cicada", next(iter(services.values()))
        )  # Default to "cindex" or the first one

    def select_service(self, service_name):
        """Select a base URL by service name."""
        if service_name in self.services:
            self.current_base_url = self.services[service_name]
        else:
            pass

    def make_request(self, method, endpoint, data=None, service_name="cindex"):
        """Make a request to the specified endpoint.

        If service_name is not provided, it defaults to "cindex".
        """
        if service_name in self.services:
            base_url = self.services[service_name]
            url = f"{base_url}{endpoint}"
            try:
                if method == "GET":

                    response = requests.get(
                        url, params=data, headers=self.headers, timeout=TIMEOUT
                    )
                elif method == "POST":
                    response = requests.post(
                        url, data=data, headers=self.headers, timeout=(5, 20)
                    )
                elif method == "PUT":
                    response = requests.put(
                        url, data=data, headers=self.headers, timeout=(5, 20)
                    )
                elif method == "DELETE":
                    response = requests.delete(
                        url, headers=self.headers, timeout=(5, 20)
                    )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                print("An exception occurred:", str(e))
                print("Exception type:", type(e).__name__)
                print("Traceback details:")
                traceback.print_exc()
                return None

        else:
            pass

    def add_service(self, service_name, base_url):
        """
        Add a new service and its base URL to the cores' dictionary.
        """
        if service_name not in self.services:
            self.services[service_name] = base_url
        else:
            pass
