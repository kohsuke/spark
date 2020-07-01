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

package org.apache.spark.deploy.k8s.features

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{K8sVolumeTestUtils, KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants.{SPARK_CONF_DIR_INTERNAL, SPARK_CONF_VOLUME}

class MountConfDirExecutorFeatureStepSuite extends SparkFunSuite {

  test("Mount spark driver's conf dir as configmap on executor pod's container.") {
    val baseDriverPod = SparkPod.initialPod()
    val kubernetesConf = KubernetesTestConf.createExecutorConf()
    val step = new MountConfDirExecutorFeatureStep(kubernetesConf)
    val podConfigured = step.configurePod(baseDriverPod)
    K8sVolumeTestUtils.containerHasVolume(podConfigured.container,
      SPARK_CONF_VOLUME, SPARK_CONF_DIR_INTERNAL)
    K8sVolumeTestUtils.podHasVolume(podConfigured.pod, SPARK_CONF_VOLUME)
  }

  // TODO support these features may be in separate JIRAs.
  // ("Skip creating config map and mounting step if, no files selected from the conf dir.")
  // ("Skip all files larger than set size, as configmaps can hold only upto 1Mb")
  // ("Skip binary files as well, configmap(in fabric8.io api) tries to convert data to string.")

}
