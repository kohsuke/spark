#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Stubs for pyspark.streaming.kinesis (Python 3.5)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.

from typing import Any, Optional

def utf8_decoder(s): ...

class KinesisUtils:
    @staticmethod
    def createStream(
        ssc,
        kinesisAppName,
        streamName,
        endpointUrl,
        regionName,
        initialPositionInStream,
        checkpointInterval,
        storageLevel: Any = ...,
        awsAccessKeyId: Optional[Any] = ...,
        awsSecretKey: Optional[Any] = ...,
        decoder: Any = ...,
        stsAssumeRoleArn: Optional[Any] = ...,
        stsSessionName: Optional[Any] = ...,
        stsExternalId: Optional[Any] = ...,
    ): ...

class InitialPositionInStream:
    LATEST: Any
    TRIM_HORIZON: Any
