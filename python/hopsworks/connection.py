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

import importlib
import os
import re
import sys
import warnings
from typing import Optional

from hopsworks import client, version
from hopsworks.core import project_api, secret_api, variable_api
from hopsworks.decorators import connected, not_connected
from hsfs import engine, feature_store, usage, util
from hsfs.core import (
    feature_store_api,
    hosts_api,
    services_api,
)
from hsfs.core.opensearch import OpenSearchClientSingleton
from hsml.core import model_api, model_registry_api, model_serving_api
from requests.exceptions import ConnectionError


HOPSWORKS_PORT_DEFAULT = 443
HOSTNAME_VERIFICATION_DEFAULT = True
CERT_FOLDER_DEFAULT = "/tmp"
SECRETS_STORE_DEFAULT = "parameterstore"
AWS_DEFAULT_REGION = "default"


class Connection:
    """A hopsworks connection object.

    This class provides convenience classmethods accessible from the `hopsworks`-module:

    !!! example "Connection factory"
        For convenience, `hopsworks` provides a factory method, accessible from the top level
        module, so you don't have to import the `Connection` class manually:

        ```python
        import hopsworks
        conn = hopsworks.connection()
        ```

    !!! hint "Save API Key as File"
        To get started quickly, you can simply create a file with the previously
         created Hopsworks API Key and place it on the environment from which you
         wish to connect to Hopsworks.

        You can then connect by simply passing the path to the key file when
        instantiating a connection:

        ```python hl_lines="6"
            import hopsworks
            conn = hopsworks.connection(
                'my_instance',                      # Hostname of your Hopsworks instance
                443,                                # Port to reach your Hopsworks instance, defaults to 443
                'my_project',                       # Name of your Hopsworks Feature Store project
                api_key_file='hopsworks.key',       # The file containing the API key generated above
                hostname_verification=True,         # Disable for self-signed certificates
            )
        ```

    Clients in external clusters need to connect to the Hopsworks using an
    API key. The API key is generated inside the Hopsworks platform, and requires at
    least the "project" scope to be able to access a project at all; plus the "featurestore" scope
    to be able to access a feature store; or "modelregistry", "dataset.create", "dataset.view",
    "dataset.delete", "serving" and "kafka" scopes to be able to access a model registry and its model serving.
    For more information, see the [integration guides](../setup.md).

    # Arguments
        host: The hostname of the Hopsworks instance in the form of `[UUID].cloud.hopsworks.ai`,
            defaults to `None`. Do **not** use the url including `https://` when connecting
            programatically.
        port: The port on which the Hopsworks instance can be reached,
            defaults to `443`.
        project: The name of the project to connect to. When running on Hopsworks, this
            defaults to the project from where the client is run from.
            Defaults to `None`.
        engine: Which engine to use, `"spark"`, `"python"` or `"training"`. Defaults to `None`,
            which initializes the engine to Spark if the environment provides Spark, for
            example on Hopsworks and Databricks, or falls back on Hive in Python if Spark is not
            available, e.g. on local Python environments or AWS SageMaker. This option
            allows you to override this behaviour. `"training"` engine is useful when only
            feature store metadata is needed, for example training dataset location and label
            information when Hopsworks training experiment is conducted.
        region_name: The name of the AWS region in which the required secrets are
            stored, defaults to `"default"`.
        secrets_store: The secrets storage to be used, either `"secretsmanager"`,
            `"parameterstore"` or `"local"`, defaults to `"parameterstore"`.
        hostname_verification: Whether or not to verify Hopsworks’ certificate, defaults
            to `True`.
        trust_store_path: Path on the file system containing the Hopsworks certificates,
            defaults to `None`.
        cert_folder: The directory to store retrieved HopsFS certificates, defaults to
            `"/tmp"`. Only required to produce messages to Kafka broker from external environment.
        api_key_file: Path to a file containing the API Key, if provided,
            `secrets_store` will be ignored, defaults to `None`.
        api_key_value: API Key as string, if provided, `secrets_store` will be ignored`,
            however, this should be used with care, especially if the used notebook or
            job script is accessible by multiple parties. Defaults to `None`.

    # Returns
        `Connection`. Connection handle to perform operations on a
            Hopsworks project.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: Optional[str] = None,
        engine: Optional[str] = None,
        region_name: str = AWS_DEFAULT_REGION,
        secrets_store: str = SECRETS_STORE_DEFAULT,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        self._host = host
        self._port = port
        self._project = project
        self._engine = engine
        self._region_name = region_name
        self._secrets_store = secrets_store
        self._hostname_verification = hostname_verification
        self._trust_store_path = trust_store_path
        self._cert_folder = cert_folder
        self._api_key_file = api_key_file
        self._api_key_value = api_key_value
        self._connected = False
        self._model_api = model_api.ModelApi()
        self._model_registry_api = model_registry_api.ModelRegistryApi()
        self._model_serving_api = model_serving_api.ModelServingApi()

        self.connect()

    @connected
    def get_secrets_api(self):
        """Get the secrets api.

        # Returns
            `SecretsApi`: The Secrets Api handle
        """
        return self._secret_api

    @connected
    def create_project(
        self, name: str, description: str = None, feature_store_topic: str = None
    ):
        """Create a new project.

        Example for creating a new project

        ```python

        import hopsworks

        connection = hopsworks.connection()

        connection.create_project("my_hopsworks_project", description="An example Hopsworks project")

        ```
        # Arguments
            name: The name of the project.
            description: optional description of the project
            feature_store_topic: optional feature store topic name

        # Returns
            `Project`. A project handle object to perform operations on.
        """
        return self._project_api._create_project(name, description, feature_store_topic)

    @connected
    def get_project(self, name: str = None):
        """Get an existing project.

        # Arguments
            name: The name of the project.

        # Returns
            `Project`. A project handle object to perform operations on.
        """

        if not name:
            name = client.get_instance()._project_name

        return self._project_api._get_project(name)

    @connected
    def get_projects(self):
        """Get all projects.

        # Returns
            `List[Project]`: List of Project objects
        """

        return self._project_api._get_projects()

    @connected
    def project_exists(self, name: str):
        """Check if a project exists.

        # Arguments
            name: The name of the project.

        # Returns
            `bool`. True if project exists, otherwise False
        """
        return self._project_api._exists(name)

    @connected
    def _check_compatibility(self):
        """Check the compatibility between the client and backend.
        Assumes versioning (major.minor.patch).
        A client is considered compatible if the major and minor version matches.

        """

        versionPattern = r"\d+\.\d+"
        regexMatcher = re.compile(versionPattern)

        client_version = version.__version__
        backend_version = self._variable_api.get_version("hopsworks")

        major_minor_client = regexMatcher.search(client_version).group(0)
        major_minor_backend = regexMatcher.search(backend_version).group(0)

        if major_minor_backend != major_minor_client:
            print("\n", file=sys.stderr)
            warnings.warn(
                "The installed hopsworks client version {0} may not be compatible with the connected Hopsworks backend version {1}. \nTo ensure compatibility please install the latest bug fix release matching the minor version of your backend ({2}) by running 'pip install hopsworks=={2}.*'".format(
                    client_version, backend_version, major_minor_backend
                ),
                stacklevel=1,
            )
            sys.stderr.flush()

    def _set_client_variables(self):
        python_version = self._variable_api.get_variable(
            "docker_base_image_python_version"
        )
        client.set_python_version(python_version)

    @connected
    def get_model_registry(self, project: str = None):
        """Get a reference to a model registry to perform operations on, defaulting to the project's default model registry.
        Shared model registries can be retrieved by passing the `project` argument.

        # Arguments
            project: The name of the project that owns the shared model registry,
            the model registry must be shared with the project the connection was established for, defaults to `None`.
        # Returns
            `ModelRegistry`. A model registry handle object to perform operations on.
        """
        return self._model_registry_api.get(project)

    @connected
    def get_model_serving(self):
        """Get a reference to model serving to perform operations on. Model serving operates on top of a model registry, defaulting to the project's default model registry.

        !!! example
            ```python

            import hopsworks

            project = hopsworks.login()

            ms = project.get_model_serving()
            ```

        # Returns
            `ModelServing`. A model serving handle object to perform operations on.
        """
        return self._model_serving_api.get()

    @usage.method_logger
    @connected
    def get_feature_store(
        self, name: Optional[str] = None
    ) -> feature_store.FeatureStore:
        """Get a reference to a feature store to perform operations on.

        Defaulting to the project name of default feature store. To get a
        Shared feature stores, the project name of the feature store is required.

        !!! example "How to get feature store instance"

            ```python
            import hsfs
            conn = hsfs.connection()
            fs = conn.get_feature_store()

            # or

            import hopsworks
            project = hopsworks.login()
            fs = project.get_feature_store()
            ```

        # Arguments
            name: The name of the feature store, defaults to `None`.

        # Returns
            `FeatureStore`. A feature store handle object to perform operations on.
        """
        if not name:
            name = client.get_instance()._project_name
        return self._feature_store_api.get(util.append_feature_store_suffix(name))

    @not_connected
    def connect(self):
        """Instantiate the connection.

        Creating a `Connection` object implicitly calls this method for you to
        instantiate the connection. However, it is possible to close the connection
        gracefully with the `close()` method, in order to clean up materialized
        certificates. This might be desired when working on external environments such
        as AWS SageMaker. Subsequently you can call `connect()` again to reopen the
        connection.

        !!! example
            ```python
            import hopsworks
            conn = hopsworks.connection()
            conn.close()
            conn.connect()
            ```
        """
        client.stop()
        self._connected = True
        try:
            # determine engine, needed to init client
            if (self._engine is not None and self._engine.lower() == "spark") or (
                self._engine is None and importlib.util.find_spec("pyspark")
            ):
                self._engine = "spark"
            elif (
                self._engine is not None and self._engine.lower() in ["hive", "python"]
            ) or (self._engine is None and not importlib.util.find_spec("pyspark")):
                self._engine = "python"
            elif self._engine is not None and self._engine.lower() == "training":
                self._engine = "training"
            elif (
                self._engine is not None
                and self._engine.lower() == "spark-no-metastore"
            ):
                self._engine = "spark-no-metastore"
            else:
                raise ConnectionError(
                    "Engine you are trying to initialize is unknown. "
                    "Supported engines are `'spark'`, `'python'` and `'training'`."
                )

            # init client
            if client.base.Client.REST_ENDPOINT not in os.environ:
                client.init(
                    "external",
                    self._host,
                    self._port,
                    self._project,
                    self._engine,
                    self._region_name,
                    self._secrets_store,
                    self._hostname_verification,
                    self._trust_store_path,
                    self._cert_folder,
                    self._api_key_file,
                    self._api_key_value,
                )
            else:
                client.init("hopsworks")

            # init engine
            engine.init(self._engine)

            self._project_api = project_api.ProjectApi()
            self._secret_api = secret_api.SecretsApi()
            self._variable_api = variable_api.VariableApi()
            self._feature_store_api = feature_store_api.FeatureStoreApi()
            self._hosts_api = hosts_api.HostsApi()
            self._services_api = services_api.ServicesApi()
            usage.init_usage(
                self._host, variable_api.VariableApi().get_version("hopsworks")
            )
            self._model_api = model_api.ModelApi()
            if self._project:
                self._model_serving_api.load_default_configuration()  # istio client, default resources,...
        except (TypeError, ConnectionError):
            self._connected = False
            raise
        print(
            "Connected. Call `.close()` to terminate connection gracefully.",
            flush=True,
        )

        self._check_compatibility()
        self._set_client_variables()

    def close(self):
        """Close a connection gracefully.

        This will clean up any materialized certificates on the local file system of
        external environments such as AWS SageMaker.

        Usage is recommended but optional.

        !!! example
            ```python
            import hopsworks
            conn = hopsworks.connection()
            conn.close()
            ```
        """
        from hsfs import client as hsfs_client
        from hsfs import engine as hsfs_engine
        from hsml import client as hsml_client

        if self._project:
            OpenSearchClientSingleton().close()

        try:
            hsfs_client.stop()
        except:  # noqa: E722
            pass

        self._feature_store_api = None

        try:
            hsfs_engine.stop()
        except:  # noqa: E722
            pass

        try:
            hsml_client.stop()
        except:  # noqa: E722
            pass

        self._model_api = None

        client.stop()
        self._connected = False

        print("Connection closed.")

    @classmethod
    def connection(
        cls,
        host: Optional[str] = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: Optional[str] = None,
        engine: Optional[str] = None,
        region_name: str = AWS_DEFAULT_REGION,
        secrets_store: str = SECRETS_STORE_DEFAULT,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str = None,
        api_key_value: str = None,
    ):
        """Connection factory method, accessible through `hopsworks.connection()`."""
        return cls(
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

    @property
    def host(self):
        return self._host

    @host.setter
    @not_connected
    def host(self, host):
        self._host = host

    @property
    def port(self):
        return self._port

    @port.setter
    @not_connected
    def port(self, port):
        self._port = port

    @property
    def project(self):
        return self._project

    @project.setter
    @not_connected
    def project(self, project):
        self._project = project

    @property
    def region_name(self) -> str:
        return self._region_name

    @region_name.setter
    @not_connected
    def region_name(self, region_name: str) -> None:
        self._region_name = region_name

    @property
    def secrets_store(self) -> str:
        return self._secrets_store

    @property
    def hostname_verification(self):
        return self._hostname_verification

    @hostname_verification.setter
    @not_connected
    def hostname_verification(self, hostname_verification):
        self._hostname_verification = hostname_verification

    @property
    def api_key_file(self):
        return self._api_key_file

    @property
    def api_key_value(self):
        return self._api_key_value

    @api_key_file.setter
    @not_connected
    def api_key_file(self, api_key_file):
        self._api_key_file = api_key_file

    @api_key_value.setter
    @not_connected
    def api_key_value(self, api_key_value):
        self._api_key_value = api_key_value

    @property
    def trust_store_path(self):
        return self._trust_store_path

    @trust_store_path.setter
    @not_connected
    def trust_store_path(self, trust_store_path):
        self._trust_store_path = trust_store_path

    @property
    def cert_folder(self):
        return self._cert_folder

    @cert_folder.setter
    @not_connected
    def cert_folder(self, cert_folder):
        self._cert_folder = cert_folder

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()
