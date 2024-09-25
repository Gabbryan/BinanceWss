from .Server import Server


class ServerBuilder(object):
    def __init__(self) -> None:
        self.token = ""
        self.server = None

    def buildServer(self, url="", token=""):
        bReturn = False
        token = token or self.token

        server = Server()
        server.urlServer = f"{url}"
        server.headers = {"Authorization": "Bearer {}".format(token)}
        server.buildRestUrls(restApiUrl="/")

        try:
            serv = server.get(allow_redirects=False)
            if serv.status_code <= 399:
                bReturn = server
            else:
                # server.buildRestUrls(restApiUrl="/ping")
                serv = server.get("ping", allow_redirects=False)

                if serv.status_code <= 399:
                    bReturn = server
        except Exception as e:
            print(f"Error building server: {e}")
            bReturn = False

        return bReturn


if __name__ == "__main__":
    server = ServerBuilder(url='https://api.binance.com/api/v3/ticker/24hr')
