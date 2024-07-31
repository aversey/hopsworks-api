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

from hopsworks_common.client import base


def get() -> base.Client:
    if not base._client:
        raise Exception("Couldn't find client. Try reconnecting to Hopsworks.")
    return base._client


# The rest is legacy from hsml client, it probably should be refactored


_kserve_installed = None


def set_kserve_installed(kserve_installed):
    global _kserve_installed
    _kserve_installed = kserve_installed


def is_kserve_installed() -> bool:
    global _kserve_installed
    return _kserve_installed


_serving_resource_limits = None


def set_serving_resource_limits(max_resources):
    global _serving_resource_limits
    _serving_resource_limits = max_resources


def get_serving_resource_limits():
    global _serving_resource_limits
    return _serving_resource_limits


_serving_num_instances_limits = None


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


_knative_domain = None


def get_knative_domain():
    global _knative_domain
    return _knative_domain


def set_knative_domain(knative_domain):
    global _knative_domain
    _knative_domain = knative_domain
