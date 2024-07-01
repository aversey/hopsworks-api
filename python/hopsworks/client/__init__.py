#
#   Copyright 2022 Logical Clocks AB
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

from typing import Literal, Optional, Union

from hopsworks.client import external, hopsworks
from hopsworks.client.istio import base as istio_base
from hopsworks.client.istio import external as istio_external
from hopsworks.client.istio import internal as istio_internal
from hopsworks.constants import HOSTS


_client = None
_python_version = None

_client_type = None
_saas_connection = None

_istio_client = None

_kserve_installed = None
_serving_resource_limits = None
_serving_num_instances_limits = None
_knative_domain = None


def init(
    client_type: Union[Literal["hopsworks"], Literal["external"]],
    host: Optional[str] = None,
    port: Optional[int] = None,
    project: Optional[str] = None,
    engine: Optional[str] = None,
    region_name: Optional[str] = None,
    secrets_store=None,
    hostname_verification: Optional[bool] = None,
    trust_store_path: Optional[str] = None,
    cert_folder: Optional[str] = None,
    api_key_file: Optional[str] = None,
    api_key_value: Optional[str] = None,
) -> None:
    global _client_type
    _client_type = client_type

    global _saas_connection
    _saas_connection = host == HOSTS.APP_HOST

    global _client
    if not _client:
        if client_type == "hopsworks":
            _client = hopsworks.Client()
        elif client_type == "external":
            _client = external.Client(
                host,
                port,
                project,
                engine,
                region_name,
                secrets_store,
                hostname_verification,
                trust_store_path,
                cert_folder,
                api_key_file,
                api_key_value,
            )


def get_instance() -> Union[hopsworks.Client, external.Client]:
    global _client
    if _client:
        return _client
    raise Exception("Couldn't find client. Try reconnecting to Hopsworks.")


def set_istio_client(host, port, project=None, api_key_value=None):
    global _client_type, _istio_client

    if not _istio_client:
        if _client_type == "internal":
            _istio_client = istio_internal.Client(host, port)
        elif _client_type == "external":
            _istio_client = istio_external.Client(host, port, project, api_key_value)


def get_istio_instance() -> istio_base.Client:
    global _istio_client
    return _istio_client


def get_client_type() -> str:
    global _client_type
    return _client_type


def is_saas_connection() -> bool:
    global _saas_connection
    return _saas_connection


def set_kserve_installed(kserve_installed):
    global _kserve_installed
    _kserve_installed = kserve_installed


def is_kserve_installed() -> bool:
    global _kserve_installed
    return _kserve_installed


def set_serving_resource_limits(max_resources):
    global _serving_resource_limits
    _serving_resource_limits = max_resources


def get_serving_resource_limits():
    global _serving_resource_limits
    return _serving_resource_limits


def set_serving_num_instances_limits(num_instances_range):
    global _serving_num_instances_limits
    _serving_num_instances_limits = num_instances_range


def get_serving_num_instances_limits():
    global _serving_num_instances_limits
    return _serving_num_instances_limits


def is_scale_to_zero_required():
    # scale-to-zero is required for KServe deployments if the Hopsworks variable `kube_serving_min_num_instances`
    # is set to 0. Other possible values are -1 (unlimited num instances) or >1 num instances.
    return get_serving_num_instances_limits()[0] == 0


def get_knative_domain():
    global _knative_domain
    return _knative_domain


def set_knative_domain(knative_domain):
    global _knative_domain
    _knative_domain = knative_domain


def get_python_version():
    global _python_version
    return _python_version


def set_python_version(python_version):
    global _python_version
    _python_version = python_version


def stop():
    global _client
    if _client:
        _client._close()
    _client = None

    global _istio_client
    if _istio_client:
        _istio_client._close()
    _istio_client = None
