# -*- coding: utf-8 -*-
"""
Created on 09 nov. 2018

@author: Baptiste
"""

# from requests.adapters import HTTPAdapter

import requests
from urllib3.util.retry import Retry


# from requests import Session


class Server(object):
    """
    classdocs
    """

    def __init__(self):
        """
        Constructor
        """
        # All Session parameters
        self.urlServer = None
        self.requestCommand = None
        self.requestResponse = None

        # Http Request parameters only
        self.httpUser = ""
        self.httpPassword = ""
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
        }

        # Applicative Session parameters only
        self.user = ""
        self.password = ""

        # API REST parameters
        self.restApiUrl = ""
        self.restApiFunction = ""

    def buildRestUrls(self, restApiUrl=""):
        self.restApiUrl = f"{self.urlServer}{restApiUrl}"

    """def buildHeaders(self, headers = {}):
        
        if(headers != {}):
            if(self.headers != {}
               and self.headers is not None):
                headers.update(self.headers)
        else:
            headers = self.headers

        return headers"""

    def buildHeaders(self, headers={}):

        headers = {**self.headers, **headers}
        return headers

    def sessionHttp(self, urlServer="", custom=""):
        """IMPLEMENT COMMENTS"""
        bReturn = True
        # All Session parameters
        self.urlServer = self.urlServer or urlServer
        requestUrl = self.urlServer
        self.requestCommand = requests.get

        if custom != "":
            requestUrl = f"{requestUrl}{custom}"

        try:
            # Start a http session
            self.requestResponse = self.requestCommand(requestUrl)
        except (requests.exceptions.ConnectionError):
            # Communication error : connection function failed
            print("Connexion {0} impossible".format(requestUrl))
            bReturn = False
        else:
            if self.requestResponse.ok:
                print("Connexion au Serveur {0}".format(self.urlServer))
            else:
                print(
                    "Connexion {0} refusée avec statut {1}".format(
                        requestUrl, self.requestResponse.status_code
                    )
                )
                bReturn = False
        return bReturn

    def get(
            self, extUrl="", headers={}, params={}, withAuth=False, allow_redirects=True
    ):
        """Perform get HTTP request
        extUrl : extension of self.restApiUrl
        headers : HTTP headers
        params : parameters to add in URL request"""

        headers = self.buildHeaders(headers=headers)

        if withAuth == False:
            r = requests.get(
                self.restApiUrl + extUrl,
                headers=headers,
                json=params,
                allow_redirects=allow_redirects,
            )
        else:
            r = requests.get(
                self.restApiUrl + extUrl,
                headers=headers,
                json=params,
                auth=(self.user, self.password),
                allow_redirects=allow_redirects,
            )

        if r.status_code > 399:
            print(
                "Error on request {0} with status code {1} - with response {2}".format(
                    r.url, r.status_code, r.text
                )
            )

        return r

    def post(self, extUrl="", headers={}, data={}, json={}):
        """perform post HTTP request
        extUrl : extension of self.restApiUrl
        headers : HTTP headers by default contains access to json application
        data : body of post request to pass argument
        json : body of post request to pass argument in JSON"""

        headers = self.buildHeaders(headers=headers)

        r = requests.post(
            self.restApiUrl + extUrl, headers=headers, data=data, json=json
        )

        if r.status_code > 399:
            print(
                "Error on request {0} with status code {1}".format(r.url, r.status_code)
            )

        return r

    def put(self, extUrl="", headers={}, data={}, json={}):
        """perform post HTTP request
        extUrl : extension of self.restApiUrl
        headers : HTTP headers by default contains access to json application
        data : body of post request to pass argument
        json : body of post request to pass argument in JSON"""

        headers = self.buildHeaders(headers=headers)

        r = requests.put(
            self.restApiUrl + extUrl, headers=headers, data=data, json=json
        )

        if r.status_code > 399:
            print(
                "Error on request {0} with status code {1}".format(r.url, r.status_code)
            )

        return r

    def delete(self, extUrl="", headers={}, data={}, json={}):
        """perform post HTTP request
        extUrl : extension of self.restApiUrl
        headers : HTTP headers by default contains access to json application
        data : body of post request to pass argument
        json : body of post request to pass argument in JSON"""

        headers = self.buildHeaders(headers=headers)

        r = requests.delete(
            self.restApiUrl + extUrl, headers=headers, data=data, json=json
        )

        if r.status_code > 399:
            print(
                "Error on request {0} with status code {1}".format(r.url, r.status_code)
            )

        return r

    def retry_request(
            self,
            method,
            extUrl="",
            retries=5,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
            **kwargs,
    ):
        """Perform an HTTP request with retries
        method : HTTP method to use ('get', 'post', 'put', 'delete')
        extUrl : extension of self.restApiUrl
        retries : number of retries if a request fails
        backoff_factor : delay factor between retries
        **kwargs : Extra parameters to pass to the HTTP request
        """

        # Initialize a Session
        s = requests.Session()
        r = None

        # Set Retry parameters
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            method_whitelist=method_whitelist,
        )

        # Initialize a Retry adapter
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        # Mount it for both http and https usage
        s.mount("https://", adapter)
        s.mount("http://", adapter)

        # Update headers
        if "headers" in kwargs:
            self.headers.update(kwargs["headers"])

        # Add headers to kwargs
        kwargs["headers"] = self.headers

        requestMethod = getattr(s, method.lower())

        try:
            r = requestMethod(self.restApiUrl + extUrl, **kwargs)
        except requests.exceptions.InvalidHeader as e:
            if str(e) == "Invalid Retry-After header: -1":
                print("Warning: Invalid Retry-After header, ignoring and retrying")
                # Reprendre l'action souhaitée ici, peut-être en réessayant la requête
            else:
                raise

        if r.status_code > 399:
            print(
                f"Error on request {r.url} with status code {r.status_code} - with response {r.text}"
            )

        return r
