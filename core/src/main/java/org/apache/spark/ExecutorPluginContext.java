/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.annotation.Private;

/**
 * This class encapsulates the input parameters for the init
 * method of the ExecutorPlugin interface.
 */
@DeveloperApi
public class ExecutorPluginContext {

  MetricRegistry metricRegistry;
  SparkConf sparkConf;
  String executorId;

  @Private
  public ExecutorPluginContext(MetricRegistry registry, SparkConf conf, String id) {
      metricRegistry = registry;
      sparkConf = conf;
      executorId = id;
  }

  public MetricRegistry getMetricRegistry() {
      return metricRegistry;
  }

  public SparkConf getSparkConf() {
      return sparkConf;
  }

  public String getExecutorId() {
      return executorId;
  }

}
