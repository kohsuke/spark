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

# Stubs for pyspark (Python 3)
#

from typing import Callable, Optional, TypeVar

from pyspark.accumulators import (
    Accumulator as Accumulator,
    AccumulatorParam as AccumulatorParam,
)
from pyspark.broadcast import Broadcast as Broadcast
from pyspark.conf import SparkConf as SparkConf
from pyspark.context import SparkContext as SparkContext
from pyspark.files import SparkFiles as SparkFiles
from pyspark.profiler import BasicProfiler as BasicProfiler, Profiler as Profiler
from pyspark.rdd import RDD as RDD, RDDBarrier as RDDBarrier
from pyspark.serializers import (
    MarshalSerializer as MarshalSerializer,
    PickleSerializer as PickleSerializer,
)
from pyspark.status import (
    SparkJobInfo as SparkJobInfo,
    SparkStageInfo as SparkStageInfo,
    StatusTracker as StatusTracker,
)
from pyspark.storagelevel import StorageLevel as StorageLevel
from pyspark.taskcontext import (
    BarrierTaskContext as BarrierTaskContext,
    BarrierTaskInfo as BarrierTaskInfo,
    TaskContext as TaskContext,
)
from pyspark.util import InheritableThread as InheritableThread

# Compatiblity imports
from pyspark.sql import SQLContext as SQLContext, HiveContext as HiveContext, Row as Row

T = TypeVar("T")
F = TypeVar("F", bound=Callable)

def since(version: str) -> Callable[[T], T]: ...
def copy_func(
    f: F,
    name: Optional[str] = ...,
    sinceversion: Optional[str] = ...,
    doc: Optional[str] = ...,
) -> F: ...
def keyword_only(func: F) -> F: ...

# Names in __all__ with no definition:
#   SparkJobInfo
#   SparkStageInfo
#   StatusTracker
