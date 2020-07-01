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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants.{SPARK_CONF_DIR_INTERNAL, SPARK_CONF_VOL_EXEC}
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils

/**
 * Populate the contents of SPARK_CONF_DIR, except spark-(conf|properties) and templates to
 * configMap to be mounted on executor pods.
 */
private[spark] class MountConfDirExecutorFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  override def configurePod(pod: SparkPod): SparkPod = {
    val configMapName = KubernetesClientUtils.configMapNameExecutor
    val confFilesMap = KubernetesClientUtils
      .buildSparkConfDirFilesMap(configMapName, conf.sparkConf, Map.empty)
    val keyToPaths = KubernetesClientUtils.buildKeyToPathObjects(confFilesMap)
    val mutatedContainer = new ContainerBuilder(pod.container)
      // Since SPARK_CONF_DIR ENV is already set up in BasicExecutorFeatureStep, we can skip here.
      .addNewVolumeMount()
        .withName(SPARK_CONF_VOL_EXEC)
        .withMountPath(SPARK_CONF_DIR_INTERNAL)
        .endVolumeMount()
      .build()
    val mutatedPod = new PodBuilder(pod.pod)
      .editSpec()
        .addNewVolume()
          .withName(SPARK_CONF_VOL_EXEC)
          .withNewConfigMap()
            .withItems(keyToPaths.asJava)
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()
    SparkPod(mutatedPod, mutatedContainer)
  }
}
