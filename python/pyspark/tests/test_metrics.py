#
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
from operator import add

from pyspark.testing.utils import ReusedPySparkTestCase


class MetricsTests(ReusedPySparkTestCase):

    def test_metrics(self):
        count = self.sc.parallelize(range(1, 10000), 1).reduce(add)
        read_time = self.sc._jvm.org.apache.spark.api.python.PythonMetrics.getFromWorkerReadTime()
        self.assertGreater(read_time_ns, 0)


if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_metrics import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
