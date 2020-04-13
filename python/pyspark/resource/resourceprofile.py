
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.resource.executorresourcerequest import ExecutorResourceRequest
from pyspark.resource.taskresourcerequest import TaskResourceRequest


class ResourceProfile(object):

    """
    .. note:: Evolving

    Resource profile to associate with an RDD. A ResourceProfile allows the user to
    specify executor and task requirements for an RDD that will get applied during a
    stage. This allows the user to change the resource requirements between stages.
    This is meant to be immutable so user doesn't change it after building.

    .. versionadded:: 3.1.0
    """

    def __init__(self, _java_resource_profile):
        self._java_resource_profile = _java_resource_profile

    @property
    def taskResources(self):
        taskRes = self._java_resource_profile.taskResourcesJMap()
        result = {}
        # convert back to python TaskResourceRequest
        for k, v in taskRes.items():
            result[k] = TaskResourceRequest(v.resourceName(), v.amount())
        return result

    @property
    def executorResources(self):
        execRes = self._java_resource_profile.executorResourcesJMap()
        result = {}
        # convert back to python ExecutorResourceRequest
        for k, v in execRes.items():
            result[k] = ExecutorResourceRequest(v.resourceName(), v.amount(),
                                                v.discoveryScript(), v.vendor())
        return result
