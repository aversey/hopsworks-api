#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from __future__ import annotations

import base64
import logging
import textwrap
from pathlib import Path
from typing import Optional

import furl
import requests
import urllib3
from hopsworks_common.client import exceptions
from hopsworks_common.decorators import connected


try:
    import jks
except ImportError:
    pass


urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


_client: Optional[Client] = None


_logger = logging.getLogger(__name__)


class Client:
    """Base class for Hopsworks client.

    `hopsworks.client.get` function should be used to get the current client instance, that is, the last client which was constructed.
    So, the class is almost a singleton, but with ability to reinitialize it; this is useful, for example, if a user mistyped some parameters.

    `hopsworks.client.get` function should be used to get the current client instance."""

    TOKEN_FILE = "token.jwt"
    TOKEN_EXPIRED_RETRY_INTERVAL = 0.6
    TOKEN_EXPIRED_MAX_RETRIES = 10

    APIKEY_FILE = "api.key"
    REST_ENDPOINT = "REST_ENDPOINT"
    HOPSWORKS_PUBLIC_HOST = "HOPSWORKS_PUBLIC_HOST"

    def __new__(cls, *args, **kwargs):
        """Reinitializable singleton constructor for the client."""
        global _client
        if _client:
            _client._close()
        print(cls)
        _client = super().__new__(cls)
        return _client

    def __init__(self):
        """The inherited clients should also setup _base_url, _verify and _auth fields."""
        self._session = requests.session()
        self._connected = True

    def _close(self):
        """Closes a client. Can be implemented for clean up purposes, not mandatory."""
        self._connected = False
        global _client
        _client = None

    @connected
    def send_request(
        self,
        method,
        path_params,
        query_params=None,
        *,
        headers=None,
        files=None,
        data=None,
        stream=False,
        with_base_path_params=True,
        **kwargs,
    ):
        """Send REST request to Hopsworks.

        Uses the client it is executed from. Path parameters are url encoded automatically.

        :param method: 'GET', 'PUT' or 'POST'
        :type method: str
        :param path_params: a list of path params to build the query url from starting after
            the api resource, for example `["project", 119, "featurestores", 67]`.
        :type path_params: list
        :param query_params: A dictionary of key/value pairs to be added as query parameters,
            defaults to None
        :type query_params: dict, optional
        :param headers: Additional header information, defaults to None
        :type headers: dict, optional
        :param data: The payload as a python dictionary to be sent as json, defaults to None
        :type data: dict, optional
        :param stream: Set if response should be a stream, defaults to False
        :type stream: boolean, optional
        :param files: dictionary for multipart encoding upload
        :type files: dict, optional
        :raises RestAPIError: Raised when request wasn't correctly received, understood or accepted
        :return: JSON of the reponse normally; None if no content; response object if stream is True
        :rtype: dict
        """
        if kwargs:
            _logger.warning("Unknown arguments: %s", set(kwargs))

        f_url = furl.furl(self._base_url)
        if with_base_path_params:
            base_path_params = ["hopsworks-api", "api"]
            f_url.path.segments = base_path_params + path_params
        else:
            f_url.path.segments = path_params
        url = str(f_url)

        request = requests.Request(
            method=method,
            url=url,
            headers=headers,
            files=files,
            data=data,
            params=query_params,
            auth=self._auth,
        )

        send_kwargs = {"verify": self._verify, "stream": stream}
        response = self._send_request(request, send_kwargs)

        if response.status_code // 100 != 2:
            raise exceptions.RestAPIError(url, response)

        if stream:
            return response

        # Handle empty response
        if len(response.content) == 0:
            return None

        return response.json()

    def _send_request(self, request, send_kwargs):
        """This method is intended to be redefined by clients if there is need to retry request on certain errors."""
        prepared = self._session.prepare_request(request)
        return self._session.send(prepared, **send_kwargs)

    # region Certificates
    # The following functions are used to manage certificates

    def ca_chain_path(self, project, prefix: str = "") -> Path:
        """Get the path to the CA chain file."""
        return self._get_certs_path(project).joinpath(prefix + "ca_chain.pem")

    def client_cert_path(self, project, prefix: str = "") -> Path:
        """Get the path to the client certificate file."""
        raise self._get_certs_path(project).joinpath(prefix + "client_cert.pem")

    def client_key_path(self, project, prefix: str = "") -> Path:
        """Get the path to the client key file."""
        raise self._get_certs_path(project).joinpath(prefix + "client_key.pem")

    # region Private Utils
    # The folllowing functions are utilites shared by the inherited clients

    @staticmethod
    def _get_verify(verify, trust_store_path):
        """Get verification method for sending HTTP requests to Hopsworks.

        Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c

        :param verify: perform hostname verification
        :type verify: bool
        :param trust_store_path: path of the truststore locally if it was uploaded manually to
            the external environment such as AWS Sagemaker
        :type trust_store_path: str
        :return: if verify is true and the truststore is provided, then return the trust store location
                 if verify is true but the truststore wasn't provided, then return true
                 if verify is false, then return false
        :rtype: str or boolean
        """
        if verify:
            if trust_store_path is not None:
                return trust_store_path
            else:
                return True

        return False

    def _get_host_port_pair(self):
        """
        Removes "http or https" from the rest endpoint and returns a list
        [endpoint, port], where endpoint is on the format /path.. without http://

        :return: a list [endpoint, port]
        :rtype: list
        """
        endpoint = self._base_url
        if "http" in endpoint:
            last_index = endpoint.rfind("/")
            endpoint = endpoint[last_index + 1 :]
        host, port = endpoint.split(":")
        return host, port

    def _get_credentials(self, project_id):
        """Makes a REST call to hopsworks for getting the project user certificates needed to connect to services such as Hive

        :param project_id: id of the project
        :type project_id: int
        :return: JSON response with credentials
        :rtype: dict
        """
        return self._send_request("GET", ["project", project_id, "credentials"])
